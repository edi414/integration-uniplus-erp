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

    def extract_unprocessed_xmls(self, limit: int = 100) -> pd.DataFrame:
        """
        Busca XMLs pendentes de processamento na tabela notas_fiscais.
        Usa LIMIT para evitar sobrecarga de memória ao processar grandes volumes de XMLs.
        """
        query = f"""
            SELECT nf.chave, nf.arquivo_xml 
            FROM notas_fiscais nf 
            WHERE nf.arquivo_xml IS NOT NULL 
              AND (
                nf.processed = 'false' 
                OR nf.processed IS NULL 
                OR NOT EXISTS (SELECT 1 FROM nfes_informations ni WHERE ni.chave_nfe = nf.chave)
              )
            ORDER BY nf.data_emissao DESC NULLS LAST
            LIMIT {limit}
        """
        try:
            return self.target_connection.get_data(query)
        except Exception as e:
            self.logger.error(f"Erro ao extrair XMLs pendentes: {str(e)}")
            return pd.DataFrame()

    def process_and_load(self, df_xmls: pd.DataFrame) -> int:
        """
        Processa cada XML e carrega nas tabelas de informações e precificação.
        Retorna a quantidade de notas processadas com sucesso.
        """
        if df_xmls.empty:
            return 0

        success_count = 0
        for _, row in df_xmls.iterrows():
            chave = row['chave']
            
            try:
                xml_binary = row['arquivo_xml']
                if not xml_binary:
                    continue
                    
                xml_content = bytes(xml_binary).decode('utf-8')
                
                # 1. Parse Sumário e Itens
                summary_data = self.parser.parse_summary(xml_content)
                df_summary = pd.DataFrame([summary_data])
                
                items_data = self.parser.parse_items(xml_content)
                df_items = pd.DataFrame(items_data)
                
                # 2. Persistência Atômica por Nota
                # Sumário
                self.target_connection.upsert(
                    table_name="nfes_informations",
                    data=df_summary,
                    unique_columns=["chave_nfe"],
                    schema="public"
                )
                
                # Ações DML manuais para limpeza e flag
                try:
                    self.target_connection.connect()
                    conn = self.target_connection.connection
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM precificacao WHERE chave_nfe = %s", (chave,))
                        cur.execute("UPDATE notas_fiscais SET processed = 'true' WHERE chave = %s", (chave,))
                    conn.commit()
                finally:
                    self.target_connection.disconnect()
                
                # Inserção dos itens
                self.target_connection.insert_batch(
                    table_name="precificacao",
                    data=df_items,
                    schema="public"
                )
                
                success_count += 1
                self.logger.info(f"Sucesso: {chave}")
                
            except Exception as ex:
                self.logger.error(f"Erro ao processar {chave}: {str(ex)}")
                continue
                
        return success_count

    def run_etl(self) -> None:
        """
        Ponto de entrada do serviço.
        Processa em lotes até que não restem mais notas pendentes.
        """
        try:
            self.logger.info("Iniciando rotina de refinamento NFe Processor...")
            total_processed = 0
            batch_limit = 100
            
            while True:
                df_xmls = self.extract_unprocessed_xmls(limit=batch_limit)
                if df_xmls.empty:
                    break
                
                self.logger.info(f"Processando lote de {len(df_xmls)} XMLs...")
                processed_in_batch = self.process_and_load(df_xmls)
                total_processed += processed_in_batch
                
                self.logger.info(f"Lote finalizado. Total acumulado: {total_processed}")
                
                # Pequena pausa para aliviar o banco
                time.sleep(1)
                
                # Se o lote não veio cheio, significa que chegamos ao fim
                if len(df_xmls) < batch_limit:
                    break

            self.logger.info(f"Rotina NFe Processor finalizada. Total refinado: {total_processed}")
            
        except Exception as e:
            self.logger.error(f"Falha na rotina NFe Processor: {str(e)}")
            raise
