import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
UNICO_DATABASE = {
    "dbname": os.getenv("UNICO_DB", "unico_db"),
    "user": os.getenv("UNICO_USER", "unico_user"),
    "password": os.getenv("UNICO_PASSWORD", "unico_password"),
    "host": os.getenv("UNICO_HOST", "localhost"),
    "port": int(os.getenv("UNICO_PORT", "5432")),
}

BANCO_MERCADO = {
    "dbname": os.getenv("MERCADO_DB", "mercado_db"),
    "user": os.getenv("MERCADO_USER", "mercado_user"),
    "password": os.getenv("MERCADO_PASSWORD", "mercado_password"),
    "host": os.getenv("MERCADO_HOST", "localhost"),
    "port": int(os.getenv("MERCADO_PORT", "5432")),
}

def get_connection_config(database: str = "unico") -> Dict[str, Any]:
    """
    Get database connection configuration
    
    Args:
        database: Database to connect to ("unico" or "mercado")
        
    Returns:
        Dictionary with connection parameters
    """
    if database.lower() == "unico":
        return UNICO_DATABASE
    elif database.lower() == "mercado":
        return BANCO_MERCADO
    else:
        raise ValueError(f"Unknown database: {database}. Use 'unico' or 'mercado'")

# Default configuration (source database)
def get_source_config() -> Dict[str, Any]:
    """Get source database configuration (UNICO)"""
    return UNICO_DATABASE

# Target configuration (cloud database)  
def get_target_config() -> Dict[str, Any]:
    """Get target database configuration (BANCO_MERCADO)"""
    return BANCO_MERCADO