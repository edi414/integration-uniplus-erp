import pandas as pd
from handlers.db_connection import DatabaseConnection
from handlers.query_loader import get_etl_query, get_etl_config, load_query_from_file
from handlers.log_handler import setup_logger
from typing import Dict, Optional

class MovimentacaoEstoqueETL:
    def __init__(self, source_config: Dict, target_config: Dict):
        self.source_connection = DatabaseConnection(source_config)
        self.target_connection = DatabaseConnection(target_config)
        self.logger = setup_logger("movimentacao_estoque_etl", log_file="logs/movimentacao_estoque_etl.log")
        
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform data to match target table structure
        """
        try:
            self.logger.info(f"Transformando {len(df)} registros")
            
            # As colunas já vêm com os nomes corretos da query
            # Garantir tipos e normalizações
            
            # Campo inteiro (filial)
            if "filial" in df.columns:
                df["filial"] = pd.to_numeric(df["filial"], errors="coerce").astype("Int64")
            
            # Campo inteiro (tipodocumento)
            if "tipodocumento" in df.columns:
                df["tipodocumento"] = pd.to_numeric(df["tipodocumento"], errors="coerce").astype("Int64")
            
            # Campos numéricos
            numeric_columns = ["qtd", "valortotal"]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            # Timestamp (datahora)
            if "datahora" in df.columns:
                df["datahora"] = pd.to_datetime(df["datahora"], errors="coerce")
            
            # Campos de texto: normalizar strings e None
            text_columns = [
                "local_estoque",
                "documento",
                "codigo",
                "un",
                "tipo_movimentacao",
                "nome",
            ]
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
                    df.loc[df[col] == "None", col] = None
                    df.loc[df[col] == "nan", col] = None
            
            # Campo numérico (currenttimemillis)
            if "currenttimemillis" in df.columns:
                df["currenttimemillis"] = pd.to_numeric(df["currenttimemillis"], errors="coerce")
            
            # Garantir presença e ordem das colunas da tabela de destino
            columns = [
                "local_estoque",
                "filial",
                "documento",
                "codigo",
                "datahora",
                "currenttimemillis",
                "tipodocumento",
                "qtd",
                "valortotal",
                "un",
                "tipo_movimentacao",
                "nome",
            ]
            
            existing_columns = [c for c in columns if c in df.columns]
            result = df[existing_columns]
            
            # Limpeza final de NaT
            for col in result.columns:
                if str(result[col].dtype).startswith("datetime64"):
                    result[col] = result[col].replace({pd.NaT: None})
            
            # Remover duplicatas baseadas nas colunas da constraint única
            # Isso evita erro "ON CONFLICT DO UPDATE command cannot affect row a second time"
            unique_key_cols = ["datahora", "codigo", "documento", "tipodocumento", "tipo_movimentacao", "currenttimemillis"]
            if all(col in result.columns for col in unique_key_cols):
                initial_count = len(result)
                result = result.drop_duplicates(subset=unique_key_cols, keep='first')
                if len(result) < initial_count:
                    self.logger.warning(f"Removidas {initial_count - len(result)} duplicatas baseadas na constraint única")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Erro na transformação: {str(e)}")
            raise

    def get_missing_dates(self) -> list:
        """
        Get list of dates that need to be processed (missing from target table)
        """
        try:
            config = get_etl_config('movimentacao_estoque')
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
        Extract data from source database for a specific date
        """
        try:
            self.logger.info(f"Extraindo dados para a data: {date}")
            query = get_etl_query('movimentacao_estoque')
            result = self.source_connection.get_data(query, {'data': date})
            self.logger.info(f"Extraídos {len(result)} registros")
            return result
        except Exception as e:
            self.logger.error(f"Erro na extração: {str(e)}")
            raise

    def load_data(self, df: pd.DataFrame, date: str) -> None:
        """
        Load transformed data into target table using UPSERT
        """
        try:
            config = get_etl_config('movimentacao_estoque')
            table_name = config.get('table', 'movimentacao_estoque')
            schema = config.get('schema', 'public')
            unique_columns = config.get(
                'unique_columns',
                ['datahora', 'codigo', 'documento', 'tipodocumento', 'tipo_movimentacao', 'currenttimemillis']
            )
            
            self.logger.info(f"Starting UPSERT for {len(df)} records on date {date}")
            
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
            self.logger.info("Starting Movimentacao Estoque ETL process")
            
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

