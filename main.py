# Example usage of the ETL services
from services.vendas_daily import VendasDailyETL
from services.notas_fiscais import NotasFiscaisETL
from services.xml_downloader import XMLDownloaderService
from settings.db_config import get_source_config, get_target_config
from datetime import datetime, date

def run_vendas_daily_etl(target_date: str):
    """
    Run the daily sales ETL for a specific date
    """
    # Note: VendasDailyETL might need similar update to use source/target configs
    connection_config = get_source_config()  # For now, using source config
    etl = VendasDailyETL(connection_config)
    etl.run_etl(target_date)

def run_notas_fiscais_etl(date_filter: str = None):
    """
    Run the notas fiscais ETL
    Args:
        date_filter: Optional date filter (format: 'YYYY-MM-DD'). 
                    If None, will fetch all records.
                    If provided, will fetch records from that date onwards.
    """
    source_config = get_source_config()  # UNICO database (local)
    target_config = get_target_config()  # BANCO_MERCADO (cloud)
    etl = NotasFiscaisETL(source_config, target_config)
    etl.run_etl(date_filter)

def run_xml_download(download_folder: str = r"G:\Meu Drive"):
    target_config = get_target_config()  # BANCO_MERCADO (cloud)
    downloader = XMLDownloaderService(target_config, download_folder)
    return downloader.run_xml_download()

def download_specific_xmls(nfe_keys: list, download_folder: str = r"G:\Meu Drive"):
    """
    Download XMLs for specific NFe keys
    Args:
        nfe_keys: List of NFe keys to download
        download_folder: Local folder path to save XML files
    """
    target_config = get_target_config()  # BANCO_MERCADO (cloud)
    downloader = XMLDownloaderService(target_config, download_folder)
    return downloader.download_specific_keys(nfe_keys)

if __name__ == "__main__":
    # # Example: Run vendas daily ETL for today
    # today = date.today().strftime('%Y-%m-%d')
    # print(f"Running vendas daily ETL for {today}")
    # run_vendas_daily_etl(today)
    
    # # Example: Run notas fiscais ETL for current month
    # current_month = date.today().replace(day=1).strftime('%Y-%m-%d')
    # print(f"Running notas fiscais ETL from {current_month} onwards")
    # run_notas_fiscais_etl()
    

    stats = run_xml_download()
    # print(f"Download completed: {stats}")


