import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL Unico legado (dados pré 01/04/2026) — usado apenas em scripts de backfill/auditoria
UNICO_POSTGRES = {
    "dbname": os.getenv("UNICO_DB", ""),
    "user": os.getenv("UNICO_USER", ""),
    "password": os.getenv("UNICO_PASSWORD", ""),
    "host": os.getenv("UNICO_HOST", ""),
    "port": int(os.getenv("UNICO_PORT", "5432")),
}

# MySQL G3 (sistema atual — fonte do pipeline diário, dados a partir de 01/04/2026)
G3_DATABASE = {
    "dbname": os.getenv("G3_DB", ""),
    "user": os.getenv("G3_USER", ""),
    "password": os.getenv("G3_PASSWORD", ""),
    "host": os.getenv("G3_HOST", ""),
    "port": int(os.getenv("G3_PORT", "3306")),
}

# PostgreSQL destino (banco_mercado — DW da loja)
BANCO_MERCADO = {
    "dbname": os.getenv("MERCADO_DB", ""),
    "user": os.getenv("MERCADO_USER", ""),
    "password": os.getenv("MERCADO_PASSWORD", ""),
    "host": os.getenv("MERCADO_HOST", "localhost"),
    "port": int(os.getenv("MERCADO_PORT", "5432")),
}


def get_target_config() -> Dict[str, Any]:
    return BANCO_MERCADO