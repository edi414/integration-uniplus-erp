import os
import logging
from typing import List
import pandas as pd
from openai import OpenAI
from tqdm import tqdm

class EmbeddingService:
    def __init__(self, model: str = "text-embedding-3-small"):
        self.logger = logging.getLogger("embedding_service")
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = model
        
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY not found in environment variables.")

    def _fetch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Gera embeddings via OpenAI em lotes de 2000."""
        if not texts: return []
        
        embeddings = []
        for i in tqdm(range(0, len(texts), 2000), desc="  [API] Gerando Embeddings"):
            response = self.client.embeddings.create(input=texts[i:i + 2000], model=self.model)
            embeddings.extend([d.embedding for d in response.data])
        return embeddings

    def process_dataframe(self, df: pd.DataFrame, db_connection, text_col='nome', code_col='codigo', out_col='embedding'):
        """Otimiza o DF gerando embeddings apenas para registros sem cache no banco."""
        if text_col not in df.columns or code_col not in df.columns:
            return df

        # 1. Busca apenas os codigos que já têm embedding (sem carregar o vetor)
        self.logger.info("Verificando cache de embeddings no banco...")
        query = f"SELECT {code_col} FROM catalogo.produtos WHERE {code_col} = ANY(%s) AND {out_col} IS NOT NULL"
        df_cache = db_connection.get_data(query, (df[code_col].tolist(),))

        cached_codes = set(df_cache[code_col].tolist()) if not df_cache.empty else set()

        # 2. Gera embeddings apenas para registros sem cache
        missing_mask = ~df[code_col].isin(cached_codes)
        df_missing = df[missing_mask]

        new_embs_map = {}
        if not df_missing.empty:
            self.logger.info(f"Gerando {len(df_missing)} novos embeddings...")
            new_embs = self._fetch_embeddings(df_missing[text_col].fillna('').tolist())
            new_embs_map = {
                code: str(emb).replace(' ', '')
                for code, emb in zip(df_missing[code_col], new_embs)
            }

        # 3. Preenche apenas os novos; produtos com cache mantêm embedding existente via UPSERT
        df[out_col] = df[code_col].map(new_embs_map)
        
        return df
