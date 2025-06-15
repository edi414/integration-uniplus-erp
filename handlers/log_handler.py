import logging
import os
import sys
from typing import Optional

def setup_logger(
    name: str,
    level: int = logging.DEBUG,
    format_str: Optional[str] = None,
    log_file: Optional[str] = None,
) -> logging.Logger:
    if format_str is None:
        format_str = "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)

    logger.handlers = []

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(format_str))
    logger.addHandler(console_handler)

    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(format_str))
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger

app_logger = setup_logger("app", log_file="logs/app.log")