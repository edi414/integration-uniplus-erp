import pandas as pd
from handlers.db_connection import DatabaseConnection
from handlers.query_loader import get_etl_query, get_etl_config, load_query_from_file
from handlers.log_handler import setup_logger
from typing import Dict, Optional

class VendasDailyETL:
    def __init__(self, source_config: Dict, target_config: Dict):
        self.source_connection = DatabaseConnection(source_config)
        self.target_connection = DatabaseConnection(target_config)
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
        df['devolucao_troca'] = None
        
        # Convert data types to match target table
        # Numeric columns (18,2)
        numeric_columns = ['v_bruto', 'desconto', 'acrescimo', 'v_venda', 'v_liquido', 'valor_finalizador', 'troco']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Date column
        if 'emissao' in df.columns:
            df['emissao'] = pd.to_datetime(df['emissao'], errors='coerce').dt.date
        
        # Timestamp columns  
        timestamp_columns = ['hora', 'hora_final']
        for col in timestamp_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Ensure all columns are present and in correct order
        columns = [
            'pdv', 'filial', 'usuario', 'vendedor', 'emissao', 'hora', 'documento', 'ccf',
            'v_bruto', 'desconto', 'acrescimo', 'v_venda', 'devolucao_troca', 'v_liquido', 'canc', 'cliente',
            'cnpj_cpf', 'finalizador', 'valor_finalizador', 'hora_final', 'troco'
        ]
        return df[columns]

    def get_missing_dates(self) -> list:
        """
        Get list of dates that need to be processed (missing from target table)
        """
        try:
            config = get_etl_config('vendas_daily')
            missing_dates_query = config.get('missing_dates_query')
            
            if not missing_dates_query:
                self.logger.warning("No missing_dates_query configured")
                return []
            
            # Load the missing dates query
            query = load_query_from_file(missing_dates_query)
            
            result = self.target_connection.get_data(query)
            if result.empty:
                return []
            
            # Convert to datetime if needed and format as string
            date_column = result.iloc[:, 0]
            if date_column.dtype == 'object':
                # Try to convert to datetime first
                date_column = pd.to_datetime(date_column, errors='coerce')
            
            # Convert to string format YYYY-MM-DD
            if hasattr(date_column, 'dt'):
                dates = date_column.dt.strftime('%Y-%m-%d').tolist()
            else:
                # Already strings, just convert to list
                dates = date_column.astype(str).tolist()
            
            # Remove any NaT or None values
            dates = [date for date in dates if date and date != 'NaT']
            
            self.logger.info(f"Found {len(dates)} missing dates to process")
            return dates
            
        except Exception as e:
            self.logger.error(f"Error getting missing dates: {str(e)}")
            return []

    def extract_data(self, date: str) -> pd.DataFrame:
        """
        Extract data from source database
        """
        query = get_etl_query('vendas_daily')
        return self.source_connection.get_data(query, {'data': date})

    def load_data(self, df: pd.DataFrame, date: str) -> None:
        """
        Load transformed data into target table using UPSERT
        """
        config = get_etl_config('vendas_daily')
        table_name = config.get('table', 'uniplus_vendas_pdvs')
        schema = config.get('schema', 'public')
        unique_columns = config.get('unique_columns', ['emissao', 'hora', 'documento', 'v_liquido'])
        
        try:
            self.logger.info(f"Starting UPSERT for {len(df)} records on date {date}")
            
            # Use existing upsert method with the constraint that already exists
            self.target_connection.upsert(
                table_name=table_name,
                data=df,
                unique_columns=unique_columns,
                schema=schema
            )
            
            self.logger.info(f"UPSERT completed: {len(df)} records processed for date {date}")
            
        except Exception as e:
            self.logger.error(f"Error during UPSERT for date {date}: {str(e)}")
            raise

    def _process_single_date(self, date: str) -> None:
        """
        Process ETL for a single date (internal method)
        """
        self.logger.info(f"Processing date: {date}")
        
        # Extract
        raw_data = self.extract_data(date)
        
        if raw_data.empty:
            self.logger.warning(f"No data found for date: {date}")
            return
        
        self.logger.info(f"Extracted {len(raw_data)} records from source database")
        
        # Transform
        transformed_data = self.transform_data(raw_data)
        self.logger.info(f"Transformed {len(transformed_data)} records")
        
        # Load
        self.load_data(transformed_data, date)
        
        self.logger.info(f"Successfully processed {len(transformed_data)} records for date: {date}")

    def run_etl(self) -> dict:
        """
        Run ETL for all missing dates (main entry point)
        Returns summary of processed dates
        """
        try:
            self.logger.info("Starting Vendas Daily ETL process")
            
            missing_dates = self.get_missing_dates()
            
            if not missing_dates:
                self.logger.info("No missing dates found. All data is up to date.")
                return {"processed": 0, "failed": 0, "dates": {"processed": [], "failed": []}}
            
            self.logger.info(f"Found {len(missing_dates)} missing dates to process")
            
            processed = []
            failed = []
            
            for date in missing_dates:
                try:
                    self._process_single_date(date)
                    processed.append(date)
                    
                except Exception as e:
                    failed.append({"date": date, "error": str(e)})
                    self.logger.error(f"Failed to process date {date}: {str(e)}")
                    # Continue with next date instead of stopping
                    continue
            
            summary = {
                "processed": len(processed),
                "failed": len(failed),
                "dates": {
                    "processed": processed,
                    "failed": failed
                }
            }
            
            self.logger.info(f"ETL completed - Processed: {len(processed)}, Failed: {len(failed)}")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"ETL process failed: {str(e)}")
            raise
