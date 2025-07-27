import os
import pandas as pd
from typing import Dict, List
from handlers.db_connection import DatabaseConnection
from handlers.query_loader import load_query_from_file
from handlers.log_handler import setup_logger

class XMLDownloaderService:
    def __init__(self, target_connection_config: Dict, download_folder: str = r"G:\Meu Drive"):
        self.target_connection = DatabaseConnection(target_connection_config)
        self.download_folder = download_folder
        self.logger = setup_logger("xml_downloader", log_file="logs/xml_downloader.log")
        self._ensure_download_folder_exists()
    
    def _ensure_download_folder_exists(self) -> None:
        try:
            if not os.path.exists(self.download_folder):
                os.makedirs(self.download_folder)
                self.logger.info(f"Created download folder: {self.download_folder}")
            else:
                self.logger.debug(f"Download folder already exists: {self.download_folder}")
        except Exception as e:
            self.logger.error(f"Error creating download folder: {str(e)}")
            raise
    
    def get_pending_nfe_keys(self) -> List[str]:
        try:
            self.logger.info("Searching for NFe keys that need XML download...")
            query = load_query_from_file('xml_download_filter.sql')
            df = self.target_connection.get_data(query)
            keys = df['chave'].tolist() if not df.empty else []
            self.logger.info(f"Found {len(keys)} NFe keys that need XML download")
            self.logger.debug(f"Keys found: {keys[:5]}..." if len(keys) > 5 else f"Keys found: {keys}")
            return keys
        except Exception as e:
            self.logger.error(f"Error getting pending NFe keys: {str(e)}")
            raise
    
    def download_xml_by_key(self, chave: str) -> bool:
        try:
            self.logger.debug(f"Downloading XML for key: {chave}")
            query = load_query_from_file('xml_binary_fetch.sql')
            df = self.target_connection.get_data(query, {'chave': chave})
            if df.empty or df['arquivo_xml'].isna().iloc[0]:
                self.logger.warning(f"No XML binary data found for key: {chave}")
                return False
            xml_binary = df['arquivo_xml'].iloc[0]
            if xml_binary is None:
                self.logger.warning(f"XML binary data is null for key: {chave}")
                return False
            filename = f"{chave}.xml"
            filepath = os.path.join(self.download_folder, filename)
            with open(filepath, "wb") as f:
                f.write(xml_binary)
            self.logger.debug(f"XML saved successfully: {filepath}")
            return True
        except Exception as e:
            self.logger.error(f"Error downloading XML for key {chave}: {str(e)}")
            return False
    
    def run_xml_download(self) -> Dict[str, int]:
        try:
            self.logger.info("Starting XML download process...")
            pending_keys = self.get_pending_nfe_keys()
            if not pending_keys:
                self.logger.info("No NFe keys found for download")
                return {'total': 0, 'success': 0, 'failed': 0}
            success_count = 0
            failed_count = 0
            for i, chave in enumerate(pending_keys, 1):
                self.logger.info(f"Processing {i}/{len(pending_keys)}: {chave}")
                if self.download_xml_by_key(chave):
                    success_count += 1
                else:
                    failed_count += 1
            stats = {
                'total': len(pending_keys),
                'success': success_count,
                'failed': failed_count
            }
            self.logger.info(f"XML download process completed!")
            self.logger.info(f"Statistics: {stats}")
            self.logger.info(f"Files saved to: {self.download_folder}")
            return stats
        except Exception as e:
            self.logger.error(f"XML download process failed: {str(e)}")
            raise
    
    def download_specific_keys(self, keys: List[str]) -> Dict[str, int]:
        try:
            self.logger.info(f"Starting XML download for {len(keys)} specific keys...")
            success_count = 0
            failed_count = 0
            for i, chave in enumerate(keys, 1):
                self.logger.info(f"Processing {i}/{len(keys)}: {chave}")
                if self.download_xml_by_key(chave):
                    success_count += 1
                else:
                    failed_count += 1
            stats = {
                'total': len(keys),
                'success': success_count,
                'failed': failed_count
            }
            self.logger.info(f"Specific XML download process completed!")
            self.logger.info(f"Statistics: {stats}")
            return stats
        except Exception as e:
            self.logger.error(f"Specific XML download process failed: {str(e)}")
            raise 