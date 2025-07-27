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
        """
        Transform the raw data to match the target table structure
        """
        try:
            self.logger.debug(f"Starting transformation of {len(df)} records")
            
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
            
            # Rename columns to match target table
            self.logger.debug("Renaming columns...")
            df = df.rename(columns=column_mapping)
            
            # Add arquivo_xml_text column (initially null)
            df['arquivo_xml_text'] = None
            
            # Ensure processed is text type ('false' instead of boolean)
            self.logger.debug("Converting processed column to text...")
            df['processed'] = df['processed'].astype(str).str.lower()
            
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
        """
        Extract data from source database
        Args:
            date_filter: Optional date filter for data_emissao (format: 'YYYY-MM-DD')
        """
        try:
            self.logger.debug("Loading SQL query from file...")
            query = get_etl_query('notas_fiscais')
            
            params = {}
            if date_filter:
                params['data_emissao'] = date_filter
                self.logger.debug(f"Using date filter: {date_filter}")
            
            self.logger.debug("Executing query against source database...")
            result = self.source_connection.get_data(query, params)
            self.logger.debug(f"Query executed successfully, returned {len(result)} rows")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error during data extraction: {str(e)}")
            raise

    def load_data(self, df: pd.DataFrame) -> None:
        """
        Load transformed data into target table using upsert
        """
        try:
            self.logger.debug("Loading ETL configuration...")
            config = get_etl_config('notas_fiscais')
            table_name = config.get('table', 'report_uniplus_notas_fiscais')
            schema = config.get('schema', 'public')
            
            self.logger.debug(f"Target table: {schema}.{table_name}")
            self.logger.debug(f"Loading {len(df)} records using upsert...")
            
            # Use upsert to handle duplicates based on chave (unique constraint)
            self.target_connection.upsert(
                table_name=table_name,
                data=df,
                unique_columns=['chave'],
                schema=schema
            )
            
            self.logger.debug("Data load completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during data load: {str(e)}")
            raise

    def run_etl(self, date_filter: Optional[str] = None) -> None:
        """
        Execute the full ETL process
        Args:
            date_filter: Optional date filter for data_emissao (format: 'YYYY-MM-DD')
        """
        try:
            self.logger.info(f"Starting ETL process for notas fiscais with date filter: {date_filter}")
            
            # Extract
            self.logger.info("Starting data extraction...")
            raw_data = self.extract_data(date_filter)
            
            if raw_data.empty:
                self.logger.warning(f"No data found for date filter: {date_filter}")
                return
            
            self.logger.info(f"Extracted {len(raw_data)} records from source database")
            
            # Transform  
            self.logger.info("Starting data transformation...")
            transformed_data = self.transform_data(raw_data)
            self.logger.info(f"Transformed {len(transformed_data)} records")
            
            # Load
            self.logger.info("Starting data load...")
            self.load_data(transformed_data)
            
            self.logger.info(f"ETL completed successfully. Processed {len(transformed_data)} records.")
            
        except Exception as e:
            self.logger.error(f"ETL process failed with error: {str(e)}")
            raise
