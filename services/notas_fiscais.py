import pandas as pd
import os
import psycopg2
import logging
from handlers.db_connection import DatabaseConnection
from handlers.query_loader import get_etl_query, get_etl_config
from handlers.log_handler import setup_logger
from handlers.nfe_handler import NFeHandler
from typing import Dict, Optional, List

class NotasFiscaisETL:
    def __init__(self, source_config: Dict, target_config: Dict):
        self.source_connection = DatabaseConnection(source_config)
        self.target_connection = DatabaseConnection(target_config)
        self.logger = setup_logger("notas_fiscais_etl", log_file="logs/notas_fiscais_etl.log")
        
        # Inicializa o Handler modularizado da SEFAZ
        self.nfe_handler = NFeHandler(
            pfx_path=os.getenv("CERTIFICADO_PATH"),
            pfx_senha=os.getenv("CERTIFICADO_PASSWORD"),
            cnpj_interessado=os.getenv("CNPJ")
        )

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info(f"Iniciando transformação de {len(df)} registros")
            
            column_mapping = {
                'codigo': 'codigo',
                'chave_nfe': 'chave',
                'data_emissao': 'data_emissao',
                'fornecedor': 'fornecedor',
                'cpnj_cpf': 'cpnj_cpf',
                'valor': 'valor',
                'natureza_operacao': 'natureza_operacao',
                'processed': 'processed',
                'data_hora_entrada': 'data_inclusao',
                'status_processamento': 'status_processamento',
                'manifestacao': 'manifestacao',
                'status_xml': 'status_xml'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Conversão Numérica
            if 'valor' in df.columns:
                df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
            
            # Conversão de Timestamp
            timestamp_cols = ['data_emissao', 'data_inclusao']
            for col in timestamp_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    df[col] = df[col].replace({pd.NaT: None})

            # Cleanup de Booleanos e Strings
            if 'processed' in df.columns:
                df['processed'] = df['processed'].astype(str).str.lower()
            
            # Lista final de colunas para o banco
            columns = [
                'codigo', 'chave', 'data_emissao', 'fornecedor', 'cpnj_cpf', 'valor',
                'natureza_operacao', 'processed', 'data_inclusao', 
                'status_processamento', 'manifestacao', 'status_xml'
            ]
            
            return df[columns]
            
        except Exception as e:
            self.logger.error(f"Erro na transformação de dados: {str(e)}")
            raise

    def extract_data(self) -> pd.DataFrame:
        try:
            query = get_etl_query('notas_fiscais')
            result = self.source_connection.get_data(query)
            return result
        except Exception as e:
            self.logger.error(f"Erro na extração de dados: {str(e)}")
            raise

    def download_missing_xmls(self) -> None:
        """
        Busca chaves no Postgres que não possuem XML, mas cujo status_xml indica disponibilidade.
        """
        try:
            config = get_etl_config('notas_fiscais')
            table_name = config.get('table', 'report_uniplus_notas_fiscais')
            
            self.target_connection.connect()
            # Regra: arquivo_xml é nulo E status_xml é 'XML Disponível'
            query = f"""
                SELECT chave FROM {table_name} 
                WHERE arquivo_xml IS NULL 
                  AND status_xml = 'XML Disponível' 
                LIMIT 30
            """
            
            with self.target_connection.connection.cursor() as cursor:
                cursor.execute(query)
                chaves = [row[0] for row in cursor.fetchall()]
                
                if not chaves:
                    self.logger.info("Nenhuma nota pendente de XML com status 'Disponível' encontrada.")
                    return

                self.logger.info(f"Identificadas {len(chaves)} notas pendentes de download condicional.")
                
                for chave in chaves:
                    self.logger.info(f"Iniciando download SEFAZ para chave: {chave}")
                    xml_content = self.nfe_handler.download_xml(chave)
                    
                    if xml_content:
                        update_query = f"UPDATE {table_name} SET arquivo_xml = %s, status_xml = 'XML Disponível' WHERE chave = %s"
                        cursor.execute(update_query, (psycopg2.Binary(xml_content.encode('utf-8')), chave))
                        self.target_connection.connection.commit()
                        self.logger.info(f"Download e salvamento concluídos para {chave}")
                    else:
                        self.logger.warning(f"Download falhou ou XML não retornado para {chave}")
            
            self.target_connection.disconnect()
        except Exception as e:
            self.logger.error(f"Erro na rotina de download condicional: {str(e)}")

    def load_data(self, df: pd.DataFrame) -> None:
        try:
            config = get_etl_config('notas_fiscais')
            table_name = config.get('table', 'report_uniplus_notas_fiscais')
            schema = config.get('schema', 'public')
            
            self.logger.info(f"Executando UPSERT de {len(df)} registros para manter XMLs existentes.")
            self.target_connection.upsert(
                table_name=table_name,
                data=df,
                unique_columns=['chave'],
                schema=schema
            )
        except Exception as e:
            self.logger.error(f"Erro na carga de dados (Upsert): {str(e)}")
            raise

    def run_etl(self) -> None:
        try:
            self.logger.info("Iniciando processo ETL de Notas Fiscais...")
            
            raw_data = self.extract_data()
            if raw_data.empty:
                self.logger.warning("Nenhum dado capturado no monitor G3 para o mês atual.")
                return
            
            transformed_data = self.transform_data(raw_data)
            self.load_data(transformed_data)
            
            # Aciona o download apenas para o que é condicionalmente necessário
            self.download_missing_xmls()
            
            self.logger.info("Processo ETL de Notas Fiscais finalizado.")
            
        except Exception as e:
            self.logger.error(f"Falha crítica no ETL de Notas Fiscais: {str(e)}")
            raise
