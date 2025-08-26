import os
import json
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from typing import Dict, List, Union, Optional
from tqdm import tqdm
from .log_handler import setup_logger

class DatabaseConnection:
    def __init__(self, connection_config: Union[str, Dict]):
        self.logger = setup_logger("database", log_file="logs/database.log")
        
        if isinstance(connection_config, str):
            try:
                self.config = json.loads(connection_config)
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON configuration: {e}")
                raise
        else:
            self.config = connection_config
            
        self.connection = None
        self._validate_config()
        
    def _validate_config(self):
        required_fields = ['host', 'port', 'dbname', 'user', 'password']
        missing_fields = [field for field in required_fields if field not in self.config]
        
        if missing_fields:
            error_msg = f"Missing required configuration fields: {', '.join(missing_fields)}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
            
    def _get_connection_string(self) -> str:
        return (
            f"host={self.config['host']} "
            f"port={self.config['port']} "
            f"dbname={self.config['dbname']} "
            f"user={self.config['user']} "
            f"password={self.config['password']}"
        )
    
    def connect(self) -> None:
        try:
            if not self.connection or self.connection.closed:
                self.connection = psycopg2.connect(self._get_connection_string())
                self.logger.info("Database connection established successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
            
    def disconnect(self) -> None:
        if self.connection and not self.connection.closed:
            self.connection.close()
            self.logger.info("Database connection closed")
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        
    def get_data(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a DataFrame.
        
        Args:
            query: SQL query to execute
            params: Optional parameters for the query
            
        Returns:
            DataFrame containing query results
        """
        try:
            self.connect()
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise
        finally:
            self.disconnect()
            
    def insert_batch(self, table_name: str, data: Union[pd.DataFrame, List[Dict]], 
                    schema: str = 'public', batch_size: int = 1000) -> None:
        """
        Insert multiple records in batches.
        
        Args:
            table_name: Name of the target table
            data: DataFrame or list of dictionaries containing data
            schema: Database schema name
            batch_size: Number of records per batch
        """
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
            
        if not data:
            self.logger.warning("No data provided for batch insert")
            return
            
        try:
            self.connect()
            with self.connection.cursor() as cursor:
                # Get column names from first record
                columns = list(data[0].keys())
                values = [tuple(record[col] for col in columns) for record in data]
                
                # Prepare the query
                query = f"""
                    INSERT INTO {schema}.{table_name} 
                    ({','.join(columns)}) 
                    VALUES %s
                """
                
                # Execute in batches with progress bar
                total_batches = (len(values) + batch_size - 1) // batch_size
                with tqdm(total=total_batches, desc="Inserting batches") as pbar:
                    for i in range(0, len(values), batch_size):
                        batch = values[i:i + batch_size]
                        execute_values(cursor, query, batch)
                        pbar.update(1)
                    
                self.connection.commit()
                self.logger.info(f"Successfully inserted {len(data)} records into {schema}.{table_name}")
                
        except Exception as e:
            self.logger.error(f"Error in batch insert: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            self.disconnect()
            
    def upsert(self, table_name: str, data: Union[pd.DataFrame, Dict], 
               unique_columns: List[str], schema: str = 'public', batch_size: int = 1000) -> None:
        """
        Perform an upsert operation (INSERT ... ON CONFLICT DO UPDATE) in batches.
        
        Args:
            table_name: Name of the target table
            data: DataFrame or dictionary containing data
            unique_columns: List of columns that form the unique constraint
            schema: Database schema name
            batch_size: Number of records per batch
        """
        if isinstance(data, pd.DataFrame):
            data = data.to_dict('records')
        elif isinstance(data, dict):
            data = [data]
            
        if not data:
            self.logger.warning("No data provided for upsert")
            return
            
        try:
            self.connect()
            with self.connection.cursor() as cursor:
                # Get column names from first record
                columns = list(data[0].keys())
                values = [tuple(record[col] for col in columns) for record in data]
                
                # Build the ON CONFLICT clause
                conflict_columns = ','.join(unique_columns)
                update_columns = [col for col in columns if col not in unique_columns]
                update_set = ','.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                
                # Prepare the query
                query = f"""
                    INSERT INTO {schema}.{table_name} 
                    ({','.join(columns)}) 
                    VALUES %s
                    ON CONFLICT ({conflict_columns})
                    DO UPDATE SET {update_set}
                """
                
                # Execute in batches with progress bar
                total_batches = (len(values) + batch_size - 1) // batch_size
                with tqdm(total=total_batches, desc="Upserting batches") as pbar:
                    for i in range(0, len(values), batch_size):
                        batch = values[i:i + batch_size]
                        execute_values(cursor, query, batch)
                        pbar.update(1)
                    
                self.connection.commit()
                self.logger.info(f"Successfully upserted {len(data)} records into {schema}.{table_name}")
                
        except Exception as e:
            self.logger.error(f"Error in upsert operation: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            self.disconnect()
