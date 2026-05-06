"""
Auditoria e sincronização histórica da tabela movimentacao_estoque.

Fontes:
  - Unico (PostgreSQL): dados pré 01/04/2026
  - G3 (MySQL):         dados a partir de 01/04/2026

Uso:
  python scripts/audit_movimentacao_estoque.py                  # auditoria + sync
  python scripts/audit_movimentacao_estoque.py --audit          # só relatório, sem modificar dados
  python scripts/audit_movimentacao_estoque.py --fix-id-documento  # corrige id_documento de todos os registros Unico
  python scripts/audit_movimentacao_estoque.py --fix-chave-nfe-g3  # reprocessa todas as datas G3 para popular chave_nfe
"""

import sys
import argparse
from datetime import date
from pathlib import Path

import pandas as pd

# Permite importar os módulos do projeto a partir da raiz
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from handlers.db_connection import DatabaseConnection
from handlers.query_loader import load_query_from_file
from handlers.log_handler import setup_logger
from settings.db_config import UNICO_POSTGRES, BANCO_MERCADO, G3_DATABASE
from services.movimentacao_estoque import MovimentacaoEstoqueETL
from utils.data_transformers import clean_dataframe_nans

CUTOFF_DATE = date(2026, 4, 1)

UNIQUE_COLUMNS = [
    "datahora", "codigo", "documento",
    "tipodocumento", "tipo_movimentacao", "currenttimemillis",
]

logger = setup_logger("audit_movimentacao_estoque", log_file="logs/audit_movimentacao_estoque.log")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mercado_conn() -> DatabaseConnection:
    return DatabaseConnection(BANCO_MERCADO)


def _unico_pg_conn() -> DatabaseConnection:
    return DatabaseConnection(UNICO_POSTGRES)


def _g3_conn() -> DatabaseConnection:
    return DatabaseConnection(G3_DATABASE)


def _section(title: str) -> None:
    border = "=" * 60
    logger.info(border)
    logger.info(f"  {title}")
    logger.info(border)
    print(f"\n{border}\n  {title}\n{border}")


def _ok(msg: str) -> None:
    logger.info(f"[OK] {msg}")
    print(f"  [OK] {msg}")


def _warn(msg: str) -> None:
    logger.warning(f"[WARN] {msg}")
    print(f"  [WARN] {msg}")


def _info(msg: str) -> None:
    logger.info(msg)
    print(f"  {msg}")


# ---------------------------------------------------------------------------
# Fase 1 — Duplicatas no destino
# ---------------------------------------------------------------------------

def audit_duplicates(mercado: DatabaseConnection, sync: bool) -> int:
    _section("Fase 1 — Duplicatas na tabela destino")

    query = """
        SELECT datahora, codigo, documento, tipodocumento, tipo_movimentacao,
               currenttimemillis, COUNT(*) AS cnt
        FROM movimentacao_estoque
        GROUP BY datahora, codigo, documento, tipodocumento, tipo_movimentacao, currenttimemillis
        HAVING COUNT(*) > 1
        ORDER BY datahora;
    """
    df = mercado.get_data(query)

    if df.empty:
        _ok("Nenhuma duplicata encontrada.")
        return 0

    total_dup_groups = len(df)
    total_dup_rows = int(df["cnt"].sum()) - total_dup_groups
    _warn(f"{total_dup_groups} grupos duplicados encontrados ({total_dup_rows} linhas extras).")
    _info(df.to_string(index=False))

    if sync:
        _info("Removendo duplicatas (mantendo o registro com menor ctid por grupo)...")
        dedup_query = """
            DELETE FROM movimentacao_estoque
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM movimentacao_estoque
                GROUP BY datahora, codigo, documento, tipodocumento, tipo_movimentacao, currenttimemillis
            );
        """
        mercado.connect()
        try:
            cursor = mercado.connection.cursor()
            cursor.execute(dedup_query)
            deleted = cursor.rowcount
            mercado.connection.commit()
            cursor.close()
            _ok(f"{deleted} linhas duplicadas removidas.")
        except Exception as e:
            mercado.connection.rollback()
            logger.error(f"Erro ao remover duplicatas: {e}")
            raise
        finally:
            mercado.disconnect()

    return total_dup_groups


# ---------------------------------------------------------------------------
# Fase 2 — Verificar constraint UNIQUE no banco destino
# ---------------------------------------------------------------------------

def audit_unique_constraint(mercado: DatabaseConnection) -> bool:
    _section("Fase 2 — Constraint UNIQUE no banco destino")

    query = """
        SELECT tc.constraint_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_name = 'movimentacao_estoque'
          AND tc.table_schema = 'public'
          AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
        ORDER BY tc.constraint_name, kcu.ordinal_position;
    """
    df = mercado.get_data(query)

    if df.empty:
        _warn("Nenhuma constraint UNIQUE ou PRIMARY KEY encontrada na tabela movimentacao_estoque!")
        _warn("Recomenda-se criar: UNIQUE (datahora, codigo, documento, tipodocumento, tipo_movimentacao, currenttimemillis)")
        _warn("SQL sugerido:")
        _warn("  CREATE UNIQUE INDEX IF NOT EXISTS uq_movimentacao_estoque")
        _warn("    ON public.movimentacao_estoque")
        _warn("    (datahora, codigo, documento, tipodocumento, tipo_movimentacao, currenttimemillis);")
        return False

    _info("Constraints encontradas:")
    _info(df.to_string(index=False))

    constraint_cols = set(df["column_name"].tolist())
    expected = set(UNIQUE_COLUMNS)
    if expected.issubset(constraint_cols):
        _ok("Constraint UNIQUE cobre todas as colunas esperadas.")
        return True
    else:
        missing = expected - constraint_cols
        _warn(f"Colunas esperadas ausentes da constraint: {missing}")
        return False


# ---------------------------------------------------------------------------
# Fase 3 — Vendas sem movimentação correspondente
# ---------------------------------------------------------------------------

def audit_vendas_sem_mov(mercado: DatabaseConnection) -> pd.DataFrame:
    _section("Fase 3 — Vendas sem movimentação de saída correspondente")

    query = """
        SELECT v.emissao, COUNT(*) AS vendas_sem_mov
        FROM uniplus_vendas_pdvs v
        LEFT JOIN movimentacao_estoque me
            ON me.id_documento = v.id_documento
            AND me.tipo_movimentacao = 'S'
        WHERE me.id_documento IS NULL
          AND v.canc = false
        GROUP BY v.emissao
        ORDER BY v.emissao;
    """
    df = mercado.get_data(query)

    if df.empty:
        _ok("Todas as vendas possuem movimentação de saída correspondente.")
    else:
        total = int(df["vendas_sem_mov"].sum())
        _warn(f"{total} vendas sem movimentação de saída em {len(df)} datas:")
        _info(df.to_string(index=False))

    return df


# ---------------------------------------------------------------------------
# Fase 4 — Datas faltando (fonte da verdade = bancos de origem)
# ---------------------------------------------------------------------------

def get_missing_dates_unico(unico_pg: DatabaseConnection, mercado: DatabaseConnection) -> list:
    _section("Fase 4a — Datas faltando no destino (fonte: Unico pré 01/04/2026)")

    # Datas disponíveis no Unico
    query_unico = """
        SELECT DISTINCT DATE(m.datahora) AS data_disponivel
        FROM public.movimentoestoque m
        WHERE DATE(m.datahora) < '2026-04-01'
          AND m.cancelado = 0
        ORDER BY 1;
    """
    df_unico = unico_pg.get_data(query_unico)

    if df_unico.empty:
        _info("Nenhum dado encontrado no Unico para o período pré 01/04/2026.")
        return []

    # Datas já carregadas no destino para esse período
    query_destino = """
        SELECT DISTINCT DATE(datahora) AS data_carregada
        FROM movimentacao_estoque
        WHERE datahora < '2026-04-01';
    """
    df_destino = mercado.get_data(query_destino)

    datas_unico = set(df_unico["data_disponivel"].astype(str).tolist())
    datas_destino = set(df_destino["data_carregada"].astype(str).tolist()) if not df_destino.empty else set()

    faltando = sorted(datas_unico - datas_destino)

    _info(f"Datas disponíveis no Unico:  {len(datas_unico)}")
    _info(f"Datas no destino (pré corte): {len(datas_destino)}")

    if faltando:
        _warn(f"{len(faltando)} datas faltando no destino (Unico):")
        for d in faltando:
            _info(f"  - {d}")
    else:
        _ok("Todas as datas do Unico já estão no destino.")

    return faltando


def get_missing_dates_g3(g3: DatabaseConnection, mercado: DatabaseConnection) -> list:
    _section("Fase 4b — Datas faltando no destino (fonte: G3 a partir de 01/04/2026)")

    # Datas com movimentação de vendas no G3
    query_vendas = """
        SELECT DISTINCT data_venda AS data_disponivel
        FROM ecf_venda_cabecalho
        WHERE data_venda >= '2026-04-01'
        ORDER BY 1;
    """
    # Datas com notas de entrada no G3
    query_notas = """
        SELECT DISTINCT COALESCE(data_chegada, data_emissao) AS data_disponivel
        FROM notas_entrada
        WHERE COALESCE(data_chegada, data_emissao) >= '2026-04-01'
        ORDER BY 1;
    """
    df_v = g3.get_data(query_vendas)
    df_n = g3.get_data(query_notas)

    datas_g3 = set()
    for df in [df_v, df_n]:
        if not df.empty:
            datas_g3.update(df["data_disponivel"].astype(str).tolist())

    if not datas_g3:
        _info("Nenhum dado encontrado no G3 para o período pós 01/04/2026.")
        return []

    # Datas já carregadas no destino para esse período
    query_destino = """
        SELECT DISTINCT DATE(datahora) AS data_carregada
        FROM movimentacao_estoque
        WHERE datahora >= '2026-04-01';
    """
    df_destino = mercado.get_data(query_destino)
    datas_destino = set(df_destino["data_carregada"].astype(str).tolist()) if not df_destino.empty else set()

    faltando = sorted(datas_g3 - datas_destino)

    _info(f"Datas disponíveis no G3:      {len(datas_g3)}")
    _info(f"Datas no destino (pós corte): {len(datas_destino)}")

    if faltando:
        _warn(f"{len(faltando)} datas faltando no destino (G3):")
        for d in faltando:
            _info(f"  - {d}")
    else:
        _ok("Todas as datas do G3 já estão no destino.")

    return faltando


# ---------------------------------------------------------------------------
# Sync — Unico histórico
# ---------------------------------------------------------------------------

def _transform_unico(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica as mesmas transformações do MovimentacaoEstoqueETL."""
    if "filial" in df.columns:
        df["filial"] = pd.to_numeric(df["filial"], errors="coerce").round().astype("Int64")

    if "tipodocumento" in df.columns:
        df["tipodocumento"] = pd.to_numeric(df["tipodocumento"], errors="coerce").round().astype("Int64")

    numeric_columns = [
        "qtd", "valortotal", "precoultimacompra", "custoaquisicao", "customedio",
        "icms", "icms_st", "ippt", "pis_cofins", "ipi", "outros_impostos", "comissao",
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "datahora" in df.columns:
        df["datahora"] = pd.to_datetime(df["datahora"], errors="coerce")

    text_columns = ["local_estoque", "documento", "codigo", "un", "tipo_movimentacao", "nome", "cfop", "chave_nfe"]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)

    if "currenttimemillis" in df.columns:
        df["currenttimemillis"] = pd.to_numeric(df["currenttimemillis"], errors="coerce")

    if "id_documento" in df.columns:
        df["id_documento"] = pd.to_numeric(df["id_documento"], errors="coerce").round().astype("Int64")

    columns = [
        "local_estoque", "filial", "documento", "codigo", "nome", "datahora",
        "currenttimemillis", "tipodocumento", "qtd", "tipo_movimentacao",
        "valortotal", "precoultimacompra", "custoaquisicao", "customedio",
        "icms", "icms_st", "ippt", "pis_cofins", "ipi", "outros_impostos",
        "comissao", "cfop", "un", "id_documento", "chave_nfe",
    ]
    existing = [c for c in columns if c in df.columns]
    return clean_dataframe_nans(df[existing])


def sync_unico(dates: list, unico_pg: DatabaseConnection, mercado: DatabaseConnection) -> dict:
    _section("Sync — Unico (dados históricos pré 01/04/2026)")

    query = load_query_from_file("movimentacao_estoque_unico.sql")
    processed, failed = [], []

    for d in dates:
        try:
            _info(f"Processando {d} (Unico)...")
            df_raw = unico_pg.get_data(query, {"data": d})

            if df_raw.empty:
                _warn(f"  Nenhum dado no Unico para {d}.")
                continue

            df = _transform_unico(df_raw)
            mercado.upsert(
                table_name="movimentacao_estoque",
                data=df,
                unique_columns=UNIQUE_COLUMNS,
                schema="public",
            )
            _ok(f"  {len(df)} registros upserted para {d}.")
            processed.append(d)

        except Exception as e:
            logger.error(f"Erro ao processar {d} (Unico): {e}")
            _warn(f"  FALHOU: {e}")
            failed.append({"date": d, "error": str(e)})

    return {"processed": processed, "failed": failed}


# ---------------------------------------------------------------------------
# Sync — G3
# ---------------------------------------------------------------------------

def sync_g3(dates: list) -> dict:
    _section("Sync — G3 (dados a partir de 01/04/2026)")

    etl = MovimentacaoEstoqueETL(
        source_config=G3_DATABASE,
        target_config=BANCO_MERCADO,
    )
    processed, failed = [], []

    for d in dates:
        try:
            _info(f"Processando {d} (G3)...")
            etl._process_single_date(d)
            _ok(f"  Concluído para {d}.")
            processed.append(d)
        except Exception as e:
            logger.error(f"Erro ao processar {d} (G3): {e}")
            _warn(f"  FALHOU: {e}")
            failed.append({"date": d, "error": str(e)})

    return {"processed": processed, "failed": failed}


# ---------------------------------------------------------------------------
# Fix id_documento — reprocessa todas as datas Unico para corrigir id_documento
# ---------------------------------------------------------------------------

def fix_id_documento_unico(unico_pg: DatabaseConnection, mercado: DatabaseConnection) -> dict:
    """
    Reprocessa todas as datas Unico via UPSERT para sobrescrever id_documento com o valor
    correto (m.idoriginal = operacao.id) em vez do valor antigo (m.id = linha de movimentação).

    O UPSERT usa a unique key (datahora, codigo, documento, tipodocumento, tipo_movimentacao,
    currenttimemillis) — que não inclui id_documento — então nenhuma duplicata é criada:
    apenas o campo id_documento é atualizado nos registros existentes.
    """
    _section("Fix — Corrigir id_documento de todos os registros Unico")

    # Busca todas as datas distintas com dados Unico no destino
    query_datas = """
        SELECT DISTINCT DATE(datahora) AS data
        FROM movimentacao_estoque
        WHERE tipodocumento IN (1, 2, 3)
        ORDER BY 1;
    """
    df_datas = mercado.get_data(query_datas)

    if df_datas.empty:
        _warn("Nenhum dado Unico encontrado no destino.")
        return {"processed": [], "failed": []}

    dates = df_datas["data"].astype(str).tolist()
    _info(f"{len(dates)} datas Unico a reprocessar.")

    return sync_unico(dates, unico_pg, mercado)


# ---------------------------------------------------------------------------
# Fix chave_nfe — reprocessa todas as datas G3 para popular chave_nfe
# ---------------------------------------------------------------------------

def fix_chave_nfe_g3(mercado: DatabaseConnection) -> dict:
    """
    Reprocessa todas as datas G3 via UPSERT para sobrescrever chave_nfe com o valor
    correto (notas_entrada.chave). Só afeta entradas (tipodocumento=55).
    """
    _section("Fix — Popular chave_nfe em todos os registros G3")

    query_datas = """
        SELECT DISTINCT DATE(datahora) AS data
        FROM movimentacao_estoque
        WHERE tipodocumento IN (55, 65)
        ORDER BY 1;
    """
    df_datas = mercado.get_data(query_datas)

    if df_datas.empty:
        _warn("Nenhum dado G3 encontrado no destino.")
        return {"processed": [], "failed": []}

    dates = df_datas["data"].astype(str).tolist()
    _info(f"{len(dates)} datas G3 a reprocessar.")

    return sync_g3(dates)


# ---------------------------------------------------------------------------
# Relatório final
# ---------------------------------------------------------------------------

def _print_summary(results: dict) -> None:
    _section("Resumo da Execução")
    for key, val in results.items():
        _info(f"{key}: {val}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auditoria e sync histórico da tabela movimentacao_estoque."
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        help="Apenas audita e exibe relatório sem modificar dados.",
    )
    parser.add_argument(
        "--fix-id-documento",
        action="store_true",
        dest="fix_id_documento",
        help="Reprocessa todas as datas Unico para corrigir id_documento (idoriginal).",
    )
    parser.add_argument(
        "--fix-chave-nfe-g3",
        action="store_true",
        dest="fix_chave_nfe_g3",
        help="Reprocessa todas as datas G3 para popular chave_nfe nas entradas.",
    )
    args = parser.parse_args()

    mercado = _mercado_conn()
    unico_pg = _unico_pg_conn()
    g3 = _g3_conn()

    # Modo especial: corrigir id_documento de todos os registros Unico
    if args.fix_id_documento:
        _info("Modo: FIX id_documento (reprocessa todas as datas Unico)")
        results = {}
        r = fix_id_documento_unico(unico_pg, mercado)
        results["datas_reprocessadas"] = len(r["processed"])
        results["datas_falhou"] = len(r["failed"])
        if r["failed"]:
            results["falhas"] = r["failed"]
        _print_summary(results)
        return

    # Modo especial: popular chave_nfe em todos os registros G3
    if args.fix_chave_nfe_g3:
        _info("Modo: FIX chave_nfe G3 (reprocessa todas as datas G3)")
        results = {}
        r = fix_chave_nfe_g3(mercado)
        results["datas_reprocessadas"] = len(r["processed"])
        results["datas_falhou"] = len(r["failed"])
        if r["failed"]:
            results["falhas"] = r["failed"]
        _print_summary(results)
        return

    sync_mode = not args.audit

    if sync_mode:
        _info("Modo: AUDITORIA + SYNC (use --audit para só relatório)")
    else:
        _info("Modo: APENAS AUDITORIA (nenhum dado será modificado)")

    results = {}

    # Fase 1 — Duplicatas
    dup_count = audit_duplicates(mercado, sync=sync_mode)
    results["duplicatas_encontradas"] = dup_count

    # Fase 2 — Constraint UNIQUE
    has_constraint = audit_unique_constraint(mercado)
    results["unique_constraint_ok"] = has_constraint

    # Fase 3 — Vendas sem movimentação
    df_sem_mov = audit_vendas_sem_mov(mercado)
    results["datas_com_vendas_sem_mov"] = len(df_sem_mov)
    results["total_vendas_sem_mov"] = int(df_sem_mov["vendas_sem_mov"].sum()) if not df_sem_mov.empty else 0

    # Fase 4 — Datas faltando
    missing_unico = get_missing_dates_unico(unico_pg, mercado)
    missing_g3 = get_missing_dates_g3(g3, mercado)
    results["datas_faltando_unico"] = len(missing_unico)
    results["datas_faltando_g3"] = len(missing_g3)

    # Sync (se não for só auditoria)
    if sync_mode:
        if missing_unico:
            r = sync_unico(missing_unico, unico_pg, mercado)
            results["sync_unico_processed"] = len(r["processed"])
            results["sync_unico_failed"] = len(r["failed"])
        else:
            results["sync_unico_processed"] = 0
            results["sync_unico_failed"] = 0

        if missing_g3:
            r = sync_g3(missing_g3)
            results["sync_g3_processed"] = len(r["processed"])
            results["sync_g3_failed"] = len(r["failed"])
        else:
            results["sync_g3_processed"] = 0
            results["sync_g3_failed"] = 0

    _print_summary(results)


if __name__ == "__main__":
    main()
