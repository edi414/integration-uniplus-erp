# Example usage of the ETL services
from services.vendas_daily import VendasDailyETL
from services.notas_fiscais import NotasFiscaisETL
from services.catalogo import CatalogoETL
from services.xml_downloader import XMLDownloaderService
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

def run_xml_download(download_folder: str = r"G:\Meu Drive"):
    target_config = get_target_config()
    downloader = XMLDownloaderService(target_config, download_folder)
    return downloader.run_xml_download()

def download_specific_xmls(nfe_keys: list, download_folder: str = r"G:\Meu Drive"):
    """
    Download XMLs for specific NFe keys
    Args:
        nfe_keys: List of NFe keys to download
        download_folder: Local folder path to save XML files
    """
    target_config = get_target_config()
    downloader = XMLDownloaderService(target_config, download_folder)
    return downloader.download_specific_keys(nfe_keys)

if __name__ == "__main__":
    # # Example: Run vendas daily ETL (processes all missing dates)
    # print("Running vendas daily ETL for missing dates...")
    # summary = run_vendas_daily_etl()
    # print(f"Processed: {summary['processed']}, Failed: {summary['failed']}")
    
    # # Example: Run notas fiscais ETL for current month
    # current_month = date.today().replace(day=1).strftime('%Y-%m-%d')
    # print(f"Running notas fiscais ETL from {current_month} onwards")
    # run_notas_fiscais_etl()
    
    # # Example: Run catalogo ETL to sync all active products
    # print("Running catalogo ETL to sync product catalog")
    # run_catalogo_etl()

    # stats = run_xml_download()
    
    pass
