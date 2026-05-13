import numpy as np
import faiss
from psycopg2.extras import execute_values
from handlers.db_connection import DatabaseConnection
from handlers.log_handler import setup_logger
from typing import Dict

THRESHOLD = 0.70
TOP_K = 10


class ProdutosSimilaresService:
    def __init__(self, target_config: Dict):
        self.db = DatabaseConnection(target_config)
        self.logger = setup_logger("produtos_similares", log_file="logs/produtos_similares.log")

    def run(self) -> int:
        self.logger.info("Iniciando compute de produtos similares via FAISS")

        # Load embeddings as text — slow over network but only runs once per ETL cycle
        df = self.db.get_data("""
            SELECT codigo, embedding::text AS emb_text
            FROM catalogo.produtos
            WHERE embedding IS NOT NULL AND nome IS NOT NULL AND LENGTH(nome) >= 3
            ORDER BY codigo
        """)

        if df.empty:
            self.logger.warning("Nenhum embedding encontrado, abortando")
            return 0

        n = len(df)
        self.logger.info(f"{n} produtos carregados")

        embeddings = np.array(
            [list(map(float, row.strip("[]").split(","))) for row in df["emb_text"]],
            dtype="float32",
        )

        faiss.normalize_L2(embeddings)

        index = faiss.IndexFlatIP(embeddings.shape[1])
        index.add(embeddings)

        self.logger.info("Índice FAISS construído, buscando vizinhos")
        sims, idxs = index.search(embeddings, TOP_K + 1)

        codigos = df["codigo"].tolist()
        pairs = []
        for i, (sim_row, idx_row) in enumerate(zip(sims, idxs)):
            ca = codigos[i]
            for sim, j in zip(sim_row, idx_row):
                if j < 0 or j == i:
                    continue
                if float(sim) < THRESHOLD:
                    break
                cb = codigos[j]
                if ca < cb:
                    pairs.append((ca, cb, round(float(sim), 3)))

        self.logger.info(f"{len(pairs)} pares acima do threshold {THRESHOLD}")

        self.db.connect()
        conn = self.db.connection
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("TRUNCATE catalogo.produtos_similares")
        if pairs:
            execute_values(
                cur,
                "INSERT INTO catalogo.produtos_similares (codigo_a, codigo_b, similaridade) VALUES %s",
                pairs,
            )
        cur.close()
        conn.autocommit = False

        self.logger.info(f"Concluído: {len(pairs)} pares armazenados")
        return len(pairs)
