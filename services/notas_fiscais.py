import pandas as pd
from handlers.db_connection import DatabaseConnection
from handlers.query_loader import get_etl_query, get_etl_config
from handlers.log_handler import setup_logger
from typing import Dict, Optional

class NotasFiscaisETL:
    def __init__(self, source_config: Dict, target_config: Dict):
        self.source_connection = DatabaseConnection(source_config)
        self.target_connection = DatabaseConnection(target_config)
        self.logger = setup_logger("notas_fiscais_etl", log_file="logs/notas_fiscais_etl.log")
        
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info(f"Starting transformation of {len(df)} records")
            
            column_mapping = {
                'id_uniplus': 'id_uniplus',
                'chave': 'chave', 
                'data_emissao': 'data_emissao',
                'fornecedor': 'fornecedor',
                'cpnj_cpf': 'cpnj_cpf',
                'valor': 'valor',
                'data_inclusao': 'data_inclusao',
                'vencimento': 'vencimento',
                'status_nfe': 'situacao',
                'situacaomanifestacao': 'manifestacao',
                'status_documento_fiscal': 'status',
                'processed': 'processed',
                'arquivoxml': 'arquivo_xml'
            }
            
            df = df.rename(columns=column_mapping)
            
            df['arquivo_xml_text'] = None
            
            df['processed'] = df['processed'].astype(str).str.lower()
            
            if 'data_inclusao' in df.columns:
                df['data_inclusao'] = df['data_inclusao'].astype(str)
                df.loc[df['data_inclusao'].str.contains('NaT|nat|NaN|nan', case=False, na=False), 'data_inclusao'] = None
                df.loc[df['data_inclusao'] == 'None', 'data_inclusao'] = None
            
            columns = [
                'id_uniplus', 'data_emissao', 'fornecedor', 'cpnj_cpf', 'valor',
                'vencimento', 'situacao', 'manifestacao', 'status', 'chave',
                'data_inclusao', 'processed', 'arquivo_xml', 'arquivo_xml_text'
            ]
            
            result = df[columns]
            self.logger.debug(f"Transformation completed, returning {len(result)} records")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error during data transformation: {str(e)}")
            raise

    def extract_data(self, date_filter: Optional[str] = None) -> pd.DataFrame:
        try:
            query = get_etl_query('notas_fiscais')
            
            params = {}
            if date_filter:
                params['data_emissao'] = date_filter
                self.logger.debug(f"Using date filter: {date_filter}")
            
            result = self.source_connection.get_data(query, params)
            self.logger.debug(f"Query executed successfully, returned {len(result)} rows")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error during data extraction: {str(e)}")
            raise

    def load_data(self, df: pd.DataFrame) -> None:
        try:
            config = get_etl_config('notas_fiscais')
            table_name = config.get('table', 'report_uniplus_notas_fiscais')
            schema = config.get('schema', 'public')
            
            self.logger.debug(f"Target table: {schema}.{table_name}")
            
            self.logger.info(f"Clearing table {schema}.{table_name}...")
            self.target_connection.connect()
            with self.target_connection.connection.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
                self.target_connection.connection.commit()
            self.target_connection.disconnect()
            self.logger.info("Table cleared successfully")
            
            self.logger.info(f"Inserting {len(df)} records...")
            self.target_connection.insert_batch(
                table_name=table_name,
                data=df,
                schema=schema
            )
            
            self.logger.info("Data load completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during data load: {str(e)}")
            raise

    def run_etl(self, date_filter: Optional[str] = None) -> None:
        try:
            self.logger.info(f"Starting ETL process for notas fiscais")
            
            self.logger.info("Starting data extraction...")
            raw_data = self.extract_data(date_filter)
            
            if raw_data.empty:
                self.logger.warning(f"No data found for date filter: {date_filter}")
                return
            
            self.logger.info(f"Extracted {len(raw_data)} records from source database")
            
            self.logger.info("Starting data transformation...")
            transformed_data = self.transform_data(raw_data)
            self.logger.info(f"Transformed {len(transformed_data)} records")
            
            self.logger.info("Starting data load...")
            self.load_data(transformed_data)
            
            self.logger.info(f"ETL completed successfully. Processed {len(transformed_data)} records.")
            
        except Exception as e:
            self.logger.error(f"ETL process failed with error: {str(e)}")
            raise
