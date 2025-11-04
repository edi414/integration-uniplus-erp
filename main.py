# Example usage of the ETL services
from services.vendas_daily import VendasDailyETL
from services.notas_fiscais import NotasFiscaisETL
from services.catalogo import CatalogoETL
from services.xml_downloader import XMLDownloaderService
from services.contas_a_pagar import ContasAPagarETL
from services.movimentacao_estoque import MovimentacaoEstoqueETL
from settings.db_config import get_source_config, get_target_config
from datetime import datetime, date

def run_vendas_daily_etl():
    """
    Run the daily sales ETL for all missing dates
    This is the main entry point - always processes missing dates from company_schedule
    """
    source_config = get_source_config()
    target_config = get_target_config()
    etl = VendasDailyETL(source_config, target_config)
    return etl.run_etl()

def run_notas_fiscais_etl(date_filter: str = None):
    """
    Run the notas fiscais ETL
    Args:
        date_filter: Optional date filter (format: 'YYYY-MM-DD'). 
                    If None, will fetch all records.
                    If provided, will fetch records from that date onwards.
    """
    source_config = get_source_config()
    target_config = get_target_config()
    etl = NotasFiscaisETL(source_config, target_config)
    etl.run_etl(date_filter)

def run_catalogo_etl():
    """
    Run the catalogo (product catalog) ETL
    This will sync all active products from the source database to the target catalog table
    """
    source_config = get_source_config()
    target_config = get_target_config()
    etl = CatalogoETL(source_config, target_config)
    etl.run_etl()

def run_contas_a_pagar_etl():
    """
    Executa o ETL de contas a pagar (com UPSERT)
    """
    source_config = get_source_config()
    target_config = get_target_config()
    etl = ContasAPagarETL(source_config, target_config)
    return etl.run_etl()

def run_movimentacao_estoque_etl():
    """
    Executa o ETL de movimentação de estoque para datas faltantes
    Processa automaticamente as datas faltantes baseado na tabela company_schedule usando UPSERT
    """
    source_config = get_source_config()
    target_config = get_target_config()
    etl = MovimentacaoEstoqueETL(source_config, target_config)
    return etl.run_etl()

def run_xml_download(download_folder: str = r"G:\Meu Drive"):
    target_config = get_target_config()
    downloader = XMLDownloaderService(target_config, download_folder)
    return downloader.run_xml_download()

if __name__ == "__main__":

    print("Running vendas daily ETL for missing dates...")
    summary = run_vendas_daily_etl()
    print(f"Processed: {summary['processed']}, Failed: {summary['failed']}")
    
    print(f"Running notas fiscais ETL")
    run_notas_fiscais_etl()
    
    print("Running catalogo ETL to sync product catalog")
    run_catalogo_etl()

    print("Running contas a pagar ETL")
    cap_summary = run_contas_a_pagar_etl()
    print(f"Registros processados (contas_a_pagar): {cap_summary['processed']}")

    print("Running movimentacao estoque ETL for missing dates...")
    estoque_summary = run_movimentacao_estoque_etl()
    print(f"Processed: {estoque_summary['processed']}, Failed: {estoque_summary['failed']}")

    print("Running XML download")
    stats = run_xml_download()
    print(f"Downloaded: {stats['downloaded']}, Failed: {stats['failed']}")