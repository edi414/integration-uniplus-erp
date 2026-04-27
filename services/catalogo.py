import pandas as pd
from handlers.db_connection import DatabaseConnection
from handlers.query_loader import get_etl_query, get_etl_config
from utils.data_transformers import clean_dataframe_nans
from handlers.log_handler import setup_logger
from typing import Dict

class CatalogoETL:
    def __init__(self, source_config: Dict, target_config: Dict):
        self.source_connection = DatabaseConnection(source_config)
        self.target_connection = DatabaseConnection(target_config)
        self.logger = setup_logger("catalogo_etl", log_file="logs/catalogo_etl.log")
        
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info(f"Transforming {len(df)} records")
            
            # Text columns to ensure they are strings
            text_columns = [
                'sku', 'ean', 'nome', 'nome_pdv', 'cean_no_fornecedor',
                'unidade_venda', 'imagem', 'cest', 'ncm', 'ippt', 'iat', 'paf_p_st'
            ]
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
            # Numeric columns
            numeric_columns = [
                'preco_venda', 'preco_ultima_compra', 'stock', 
                'preco_custo', 'fator_multiplicativo', 'qtd_por_caixa'
            ]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
            # Integer columns that might have nulls
            int_columns = [
                'balanca_integrada', 'id_grupo', 'id_regra_icms', 
                'id_grupo_ipi', 'id_grupo_pis', 'id_grupo_cofins', 'ecf_icms_st'
            ]
            for col in int_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

            # Datetime columns
            date_columns = ['cadastro_at', 'ultima_compra_at', 'ultima_venda_at', 'edited_at']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Return only columns that exist in target table to avoid insertion errors
            target_columns = [
                'sku', 'ean', 'nome', 'nome_pdv', 
                'preco_ultima_compra', 'preco_venda', 'stock'
            ]
            df = df[[c for c in target_columns if c in df.columns]]
            
            return clean_dataframe_nans(df)
            
        except Exception as e:
            self.logger.error(f"Error during transformation: {str(e)}")
            raise

    def extract_data(self) -> pd.DataFrame:
        try:
            self.logger.info("Starting data extraction")
            query = get_etl_query('catalogo')
            result = self.source_connection.get_data(query)
            self.logger.info(f"Extracted {len(result)} records")
            return result
            
        except Exception as e:
            self.logger.error(f"Error during extraction: {str(e)}")
            raise

    def load_data(self, df: pd.DataFrame) -> None:
        try:
            self.logger.info(f"Loading {len(df)} records")
            
            config = get_etl_config('catalogo')
            table_name = config.get('table', 'catalogo')
            schema = config.get('schema', 'public')
            
            self.target_connection.connect()
            with self.target_connection.connection.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
                self.target_connection.connection.commit()
            self.target_connection.disconnect()
            
            self.target_connection.insert_batch(
                table_name=table_name,
                data=df,
                schema=schema
            )
            
            self.logger.info("Data load completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during data load: {str(e)}")
            raise

    def run_etl(self) -> None:
        try:
            self.logger.info("Starting catalog ETL process")
            
            raw_data = self.extract_data()
            
            if raw_data.empty:
                self.logger.warning("No data found in source")
                return
            
            transformed_data = self.transform_data(raw_data)
            self.load_data(transformed_data)
            
            self.logger.info("ETL process completed successfully")
            
        except Exception as e:
            self.logger.error(f"ETL process failed: {str(e)}")
            raise