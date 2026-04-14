import pandas as pd
import logging
import time
from typing import Dict, List
from handlers.db_connection import DatabaseConnection
from handlers.log_handler import setup_logger
from handlers.nfe_parser import NFeParser

class NFeProcessorETL:
    def __init__(self, target_config: Dict):
        self.target_connection = DatabaseConnection(target_config)
        self.logger = setup_logger("nfe_processor_etl", log_file="logs/nfe_processor_etl.log")
        self.parser = NFeParser()

    def extract_unprocessed_xmls(self) -> pd.DataFrame:
        """
        Busca XMLs pendentes de processamento na tabela notas_fiscais.
        Diferente do anterior, agora verifica se a nota já existe no destino (nfes_informations)
        para garantir que falhas anteriores ou exclusões sejam corrigidas.
        """
        query = """
            SELECT nf.chave, nf.arquivo_xml 
            FROM notas_fiscais nf 
            WHERE nf.arquivo_xml IS NOT NULL 
              AND (
                nf.processed = 'false' 
                OR nf.processed IS NULL 
                OR NOT EXISTS (SELECT 1 FROM nfes_informations ni WHERE ni.chave_nfe = nf.chave)
              )
        """
        try:
            return self.target_connection.get_data(query)
        except Exception as e:
            self.logger.error(f"Erro ao extrair XMLs pendentes: {str(e)}")
            return pd.DataFrame()

    def process_and_load(self, df_xmls: pd.DataFrame) -> None:
        """Processa cada XML e carrega nas tabelas de informações e precificação."""
        if df_xmls.empty:
            self.logger.info("Nenhum XML pendente de processamento.")
            return

        self.logger.info(f"Iniciando refinamento de {len(df_xmls)} XMLs encontrados/pendentes.")
        
        self.target_connection.connect()
        conn = self.target_connection.connection
        
        try:
            for _, row in df_xmls.iterrows():
                chave = row['chave']
                
                try:
                    # O arquivo_xml vem do Postgres como memoryview/bytes
                    xml_binary = row['arquivo_xml']
                    if not xml_binary:
                        continue
                        
                    xml_content = bytes(xml_binary).decode('utf-8')
                    
                    self.logger.info(f"Refinando XML: {chave}")
                    
                    # 1. Parse Sumário (nfes_informations)
                    summary_data = self.parser.parse_summary(xml_content)
                    df_summary = pd.DataFrame([summary_data])
                    
                    # 2. Parse Itens (precificacao)
                    items_data = self.parser.parse_items(xml_content)
                    df_items = pd.DataFrame(items_data)
                    
                    # 3. Carregar Sumário (UPSERT por chave_nfe para garantir integridade)
                    self.target_connection.upsert(
                        table_name="nfes_informations",
                        data=df_summary,
                        unique_columns=["chave_nfe"],
                        schema="public"
                    )
                    
                    # 4. Carregar Itens (Limpa duplicatas antigas e insere lote atualizado)
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM precificacao WHERE chave_nfe = %s", (chave,))
                    
                    self.target_connection.insert_batch(
                        table_name="precificacao",
                        data=df_items,
                        schema="public"
                    )
                    
                    # 5. Atualiza flag de processado na origem
                    with conn.cursor() as cur:
                        cur.execute("UPDATE notas_fiscais SET processed = 'true' WHERE chave = %s", (chave,))
                    
                    conn.commit()
                    self.logger.info(f"Concluído com sucesso para chave {chave}")
                    
                except Exception as ex:
                    self.logger.error(f"Erro técnico ao processar {chave}: {str(ex)}")
                    conn.rollback()
                    continue

        finally:
            self.target_connection.disconnect()

    def run_etl(self) -> None:
        """Ponto de entrada do serviço."""
        try:
            self.logger.info("Iniciando rotina de refinamento NFe Processor...")
            df_xmls = self.extract_unprocessed_xmls()
            self.process_and_load(df_xmls)
            self.logger.info("Rotina NFe Processor finalizada.")
        except Exception as e:
            self.logger.error(f"Falha na rotina NFe Processor: {str(e)}")
            raise
