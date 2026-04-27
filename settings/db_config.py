import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
UNICO_DATABASE = {
    "dbname": os.getenv("UNICO_DB") or os.getenv("DB") or "unico_db",
    "user": os.getenv("UNICO_USER") or os.getenv("USER") or "unico_user",
    "password": os.getenv("UNICO_PASSWORD") or os.getenv("PASSWORD") or "unico_password",
    "host": os.getenv("UNICO_HOST") or os.getenv("HOST") or "localhost",
    "port": int(os.getenv("UNICO_PORT") or os.getenv("PORT") or "5432"),
}

BANCO_MERCADO = {
    "dbname": os.getenv("MERCADO_DB", "mercado_db"),
    "user": os.getenv("MERCADO_USER", "mercado_user"),
    "password": os.getenv("MERCADO_PASSWORD", "mercado_password"),
    "host": os.getenv("MERCADO_HOST", "localhost"),
    "port": int(os.getenv("MERCADO_PORT", "5432")),
}

# PostgreSQL histórico do Unico (dados pré 01/04/2026)
# Alias de UNICO_DATABASE — mantido por clareza semântica no script de auditoria
UNICO_POSTGRES = {
    "dbname": os.getenv("UNICO_DB", ""),
    "user": os.getenv("UNICO_USER", ""),
    "password": os.getenv("UNICO_PASSWORD", ""),
    "host": os.getenv("UNICO_HOST", ""),
    "port": int(os.getenv("UNICO_PORT", "")),
}

# MySQL G3 (sistema atual — dados a partir de 01/04/2026)
G3_DATABASE = {
    "dbname": os.getenv("G3_DB", ""),
    "user": os.getenv("G3_USER", ""),
    "password": os.getenv("G3_PASSWORD", ""),
    "host": os.getenv("G3_HOST", ""),
    "port": int(os.getenv("G3_PORT", "")),
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
    elif database.lower() == "unico_postgres":
        return UNICO_POSTGRES
    elif database.lower() == "g3":
        return G3_DATABASE
    else:
        raise ValueError(f"Unknown database: {database}. Use 'unico', 'mercado', 'unico_postgres' or 'g3'")

# Default configuration (source database)
def get_source_config() -> Dict[str, Any]:
    """Get source database configuration (UNICO)"""
    return UNICO_DATABASE

# Target configuration (cloud database)  
def get_target_config() -> Dict[str, Any]:
    """Get target database configuration (BANCO_MERCADO)"""
    return BANCO_MERCADO