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