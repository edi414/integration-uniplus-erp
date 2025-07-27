import pandas as pd
from handlers.db_connection import DatabaseConnection
from handlers.query_loader import get_etl_query, get_etl_config
from handlers.log_handler import setup_logger
from typing import Dict, Optional

class VendasDailyETL:
    def __init__(self, connection_config: Dict):
        self.connection = DatabaseConnection(connection_config)
        self.logger = setup_logger("vendas_daily_etl", log_file="logs/vendas_daily_etl.log")
        
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        column_mapping = {
            'pdv': 'pdv',
            'filial': 'filial',
            'usuario': 'usuario',
            'valorbruto': 'v_bruto',
            'valorliquido': 'v_liquido',
            'cancelado': 'canc',
            'finalizador': 'finalizador',
            'valortotal': 'valor_finalizador',
            'descontoitem': 'desconto',
            'acrescimoitem': 'acrescimo',
            'data': 'emissao',
            'horainicial': 'hora',
            'troco': 'troco',
            'horafinal': 'hora_final'
        }
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Transform cancelado from 0/1 to Não/Sim
        df['canc'] = df['canc'].map({0: 'Não', 1: 'Sim'})
        
        # Create documento by concatenating serienfce and numeronfce
        df['documento'] = df['serienfce'].astype(str) + '/' + df['numeronfce'].astype(str)
        
        # Add missing columns with default values
        df['vendedor'] = None
        df['ccf'] = None
        df['cliente'] = None
        df['cnpj_cpf'] = None
        df['v_venda'] = df['v_bruto']
        
        # Ensure all columns are present and in correct order
        columns = [
            'pdv', 'filial', 'usuario', 'vendedor', 'emissao', 'hora', 'documento', 'ccf',
            'v_bruto', 'desconto', 'acrescimo', 'v_venda', 'v_liquido', 'canc', 'cliente',
            'cnpj_cpf', 'finalizador', 'valor_finalizador', 'hora_final', 'troco'
        ]
        return df[columns]

    def extract_data(self, date: str) -> pd.DataFrame:
        """
        Extract data from source database
        """
        query = get_etl_query('vendas_daily')
        return self.connection.get_data(query, {'data': date})

    def load_data(self, df: pd.DataFrame) -> None:
        """
        Load transformed data into target table
        """
        config = get_etl_config('vendas_daily')
        table_name = config.get('table', 'fato_vendas_diarias')
        schema = config.get('schema', 'public')
        self.connection.insert_batch(
            table_name=table_name,
            data=df,
            schema=schema
        )

    def run_etl(self, date: str) -> None:
        """
        Execute the full ETL process
        """
        try:
            self.logger.info(f"Starting ETL process for vendas daily with date: {date}")
            
            # Extract
            self.logger.info("Starting data extraction...")
            raw_data = self.extract_data(date)
            
            if raw_data.empty:
                self.logger.warning(f"No data found for date: {date}")
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










