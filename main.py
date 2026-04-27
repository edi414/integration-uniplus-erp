import time
import os
import sentry_sdk
from dotenv import load_dotenv
from services.vendas_daily import VendasDailyETL
from services.notas_fiscais import NotasFiscaisETL
from services.catalogo import CatalogoETL
from services.contas_a_pagar import ContasAPagarETL
from services.movimentacao_estoque import MovimentacaoEstoqueETL
from services.nfe_processor import NFeProcessorETL
from settings.db_config import G3_DATABASE, get_target_config

load_dotenv()
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    send_default_pii=True,
)


def run_vendas_daily_etl():
    etl = VendasDailyETL(G3_DATABASE, get_target_config())
    return etl.run_etl()


def run_notas_fiscais_etl():
    etl = NotasFiscaisETL(G3_DATABASE, get_target_config())
    etl.run_etl()


def run_nfe_processor_etl():
    etl = NFeProcessorETL(get_target_config())
    etl.run_etl()


def run_catalogo_etl():
    etl = CatalogoETL(G3_DATABASE, get_target_config())
    etl.run_etl()


def run_contas_a_pagar_etl():
    etl = ContasAPagarETL(G3_DATABASE, get_target_config())
    return etl.run_etl()


def run_movimentacao_estoque_etl():
    etl = MovimentacaoEstoqueETL(G3_DATABASE, get_target_config())
    return etl.run_etl()

if __name__ == "__main__":
    print("🚀 Iniciando Vendas Daily ETL...")
    v_summary = run_vendas_daily_etl()
    print(f"   Processados: {v_summary['processed']}, Falhas: {v_summary['failed']}")
    
    print("\n🚀 Iniciando Notas Fiscais ETL (Extração G3 + Download SEFAZ)...")
    try:
        run_notas_fiscais_etl()
        print("\n⏳ Aguardando 60 segundos antes de processar os XMLs...")
        time.sleep(60)
        print("\n🚀 Iniciando Refinamento de Dados XML (NFe Processor)...")
        run_nfe_processor_etl()
    except Exception as e:
        print(f"   [WARN] Notas Fiscais/NFe Processor falhou: {e}")
    
    print("\n🚀 Iniciando Catalogo ETL (Sincronização de Produtos)...")
    run_catalogo_etl()

    print("\n🚀 Iniciando Contas a Pagar ETL...")
    cap_summary = run_contas_a_pagar_etl()
    print(f"   Registros processados: {cap_summary['processed']}")

    print("\n🚀 Iniciando Movimentação de Estoque ETL...")
    est_summary = run_movimentacao_estoque_etl()
    print(f"   Processados: {est_summary['processed']}, Falhas: {est_summary['failed']}")

    print("\n✅ Todos os processos ETL foram finalizados com sucesso.")