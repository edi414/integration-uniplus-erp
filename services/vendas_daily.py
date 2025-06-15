import pandas as pd
from handlers.db_connection import DatabaseConnection
from settings import queries
from settings.database import Database
from typing import Dict, Optional
import json

class VendasDailyETL:
    def __init__(self, connection_config: Dict):
        self.connection = DatabaseConnection(connection_config)
        
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
        with open('settings/queries.json', 'r') as f:
            queries = json.load(f)
            
        query = queries['vendas_daily']['query_unico']
        return self.connection.get_data(query, {'data': date})

    def load_data(self, df: pd.DataFrame) -> None:
        """
        Load transformed data into target table
        """
        with open('settings/queries.json', 'r') as f:
            queries = json.load(f)
        table_name = queries['vendas_daily'].get('table', 'fato_vendas_diarias')
        schema = queries['vendas_daily'].get('schema', 'public')
        self.connection.insert_batch(
            table_name=table_name,
            data=df,
            schema=schema
        )

    def run_etl(self, date: str) -> None:
        """
        Execute the full ETL process
        """
        # Extract
        raw_data = self.extract_data(date)
        
        # Transform
        transformed_data = self.transform_data(raw_data)
        
        # Load
        self.load_data(transformed_data)










