import io
import logging
import os
import sys
from typing import Optional

# Build a single UTF-8 stdout wrapper at module level so it is shared across
# all loggers.  Creating a new TextIOWrapper each call would close the
# underlying buffer on the second invocation, causing "I/O on closed file".
_utf8_stdout = io.TextIOWrapper(
    sys.stdout.buffer,
    encoding="utf-8",
    errors="replace",
    line_buffering=True,
)


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

    console_handler = logging.StreamHandler(_utf8_stdout)
    console_handler.setFormatter(logging.Formatter(format_str))
    logger.addHandler(console_handler)

    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(format_str))
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


app_logger = setup_logger("app", log_file="logs/app.log")
