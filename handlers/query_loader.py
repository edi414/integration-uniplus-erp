import os
import json
from typing import Dict, Any

def load_etl_config() -> Dict[str, Any]:
    """
    Load ETL configuration from config_etl.json
    
    Returns:
        Dictionary with ETL configurations
    """
    config_path = os.path.join('settings', 'config_etl.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_query_from_file(query_file: str) -> str:
    """
    Load SQL query from file
    
    Args:
        query_file: Name of the SQL file (e.g., 'vendas_daily.sql')
        
    Returns:
        SQL query as string
    """
    query_path = os.path.join('queries', query_file)
    
    if not os.path.exists(query_path):
        raise FileNotFoundError(f"Query file not found: {query_path}")
    
    with open(query_path, 'r', encoding='utf-8') as f:
        return f.read().strip()

def get_etl_query(etl_name: str) -> str:
    """
    Get SQL query for a specific ETL process
    
    Args:
        etl_name: Name of the ETL process (e.g., 'vendas_daily', 'notas_fiscais')
        
    Returns:
        SQL query as string
    """
    config = load_etl_config()
    
    if etl_name not in config:
        raise ValueError(f"ETL configuration not found: {etl_name}")
    
    etl_config = config[etl_name]
    query_file = etl_config.get('query_file')
    
    if not query_file:
        raise ValueError(f"Query file not specified for ETL: {etl_name}")
    
    return load_query_from_file(query_file)

def get_etl_config(etl_name: str) -> Dict[str, Any]:
    """
    Get configuration for a specific ETL process
    
    Args:
        etl_name: Name of the ETL process
        
    Returns:
        Configuration dictionary for the ETL process
    """
    config = load_etl_config()
    
    if etl_name not in config:
        raise ValueError(f"ETL configuration not found: {etl_name}")
    
    return config[etl_name] 