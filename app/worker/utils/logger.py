import logging
import sys
from datetime import datetime
import os

_logger_initialized = False

def get_logger():
    global _logger_initialized
    logger = logging.getLogger("worker_logger")
    
    if not _logger_initialized:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Console handler (sempre ativo para stdout)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File Handler não é configurado globalmente para evitar logs gigantes
        # Apenas chamamos um FileHandler na hora que tivermos uma mensagem pra processar.
        
        _logger_initialized = True

    return logger

def log_event_to_file(message: str, level: int = logging.INFO):
    """
    Registra um log em arquivo apenas quando necessário (ex: quando chegou um evento),
    evitando arquivos gigantescos apenas com logs de polling ou inicialização.
    """
    now_date = datetime.now().strftime('%d_%m_%Y')
    file_name = f'worker_events_{now_date}.log'
    
    file_handler = logging.FileHandler(file_name)
    formatter = logging.Formatter('%(asctime)s - EVENT - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    temp_logger = logging.getLogger("event_logger")
    temp_logger.setLevel(level)
    
    # Remove handlers antigos se existirem na mesma instância para evitar duplicidade
    if temp_logger.hasHandlers():
        temp_logger.handlers.clear()
        
    temp_logger.addHandler(file_handler)
    
    if level == logging.INFO:
        temp_logger.info(message)
    elif level == logging.ERROR:
        temp_logger.error(message)
    elif level == logging.WARNING:
        temp_logger.warning(message)
        
    # Importante fechar e remover o handler para não segurar o arquivo aberto
    file_handler.close()
    temp_logger.removeHandler(file_handler)
