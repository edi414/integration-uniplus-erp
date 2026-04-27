"""
Backfill de vendas históricas do Unico (pré-01/04/2026) em uniplus_vendas_pdvs.

Usa UPSERT puro — nunca trunca. Idempotente: pode ser re-executado sem risco.
Chave única: (filial, pdv, id_documento) onde id_documento = operacao.id

Uso:
  python scripts/backfill_vendas_unico.py
  python scripts/backfill_vendas_unico.py --dry-run
"""

import sys
import argparse
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from handlers.db_connection import DatabaseConnection
from handlers.query_loader import load_query_from_file
from handlers.log_handler import setup_logger
from settings.db_config import UNICO_POSTGRES, BANCO_MERCADO
from utils.data_transformers import clean_dataframe_nans

CUTOFF_DATE = date(2026, 4, 1)
TABLE = "uniplus_vendas_pdvs"
SCHEMA = "public"
UNIQUE_COLUMNS = ["filial", "pdv", "id_documento"]

logger = setup_logger("backfill_vendas_unico", log_file="logs/backfill_vendas_unico.log")


def _info(msg: str) -> None:
    print(f"[INFO]  {msg}")
    logger.info(msg)


def _warn(msg: str) -> None:
    print(f"[WARN]  {msg}")
    logger.warning(msg)


def _error(msg: str) -> None:
    print(f"[ERROR] {msg}")
    logger.error(msg)


def get_all_dates(unico: DatabaseConnection) -> list[str]:
    df = unico.get_data(
        "SELECT DISTINCT data AS dt FROM operacao WHERE tipo = 1 AND data < %(cutoff)s ORDER BY 1",
        {"cutoff": CUTOFF_DATE},
    )
    return df["dt"].astype(str).tolist()


def transform(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["pdv"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    if "emissao" in df.columns:
        df["emissao"] = pd.to_datetime(df["emissao"], errors="coerce").dt.date
        df["emissao"] = df["emissao"].replace({pd.NaT: None})

    for col in ["hora", "hora_final"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].replace({pd.NaT: None})

    numeric_cols = ["v_bruto", "desconto", "acrescimo", "v_liquido",
                    "valor_finalizador", "troco", "valor_recebido"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "canc" in df.columns:
        df["canc"] = df["canc"].astype(bool).fillna(False)

    columns = [
        "pdv", "filial", "emissao", "hora", "documento",
        "v_bruto", "desconto", "acrescimo", "v_liquido", "canc",
        "cliente", "cnpj_cpf", "finalizador", "valor_finalizador", "hora_final", "troco",
        "status_nfce", "valor_recebido", "id_documento",
    ]

    return clean_dataframe_nans(df[columns])


def process_date(date_str: str, unico: DatabaseConnection, mercado: DatabaseConnection) -> int:
    query = load_query_from_file("vendas_unico.sql")
    raw = unico.get_data(query, {"data": date_str})
    if raw.empty:
        return 0
    transformed = transform(raw)
    mercado.upsert(table_name=TABLE, data=transformed, unique_columns=UNIQUE_COLUMNS, schema=SCHEMA)
    return len(transformed)


def run(dry_run: bool = False) -> None:
    unico = DatabaseConnection(UNICO_POSTGRES)
    mercado = DatabaseConnection(BANCO_MERCADO)

    _info("Buscando datas no Unico...")
    dates = get_all_dates(unico)
    _info(f"{len(dates)} datas ({dates[0]} → {dates[-1]})")

    if dry_run:
        _info("Dry-run: nenhuma alteração será feita.")
        return

    processed, failed, total_records = [], [], 0

    for i, dt in enumerate(dates, 1):
        try:
            n = process_date(dt, unico, mercado)
            total_records += n
            processed.append(dt)
            if i % 50 == 0 or i == len(dates):
                _info(f"  {i}/{len(dates)} datas | {total_records} registros")
        except Exception as e:
            _error(f"  Falha em {dt}: {e}")
            failed.append({"date": dt, "error": str(e)})

    _info("=" * 60)
    _info(f"Concluído: {len(processed)} OK, {len(failed)} falhas, {total_records} registros.")
    if failed:
        _warn(f"Datas com falha: {[f['date'] for f in failed]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill vendas Unico — UPSERT puro, sem truncate")
    parser.add_argument("--dry-run", action="store_true", help="Lista datas sem executar")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
