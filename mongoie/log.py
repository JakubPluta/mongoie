import logging
import sys
from typing import Optional


LOGGING_FORMATTER = (
    "[%(levelname)s] %(name)s %(asctime)s %(funcName)s:%(lineno)d - %(message)s"
)


def get_logger(name: Optional[str] = None, level: str = "DEBUG") -> logging.Logger:
    """Returns Logger Instance with predefined formatting"""
    logger = logging.getLogger(name=name)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(LOGGING_FORMATTER)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if not level or level.upper() not in [
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    ]:
        logger.warning(
            "invalid logging level: {level}, setting logging level to `DEBUG`"
        )
    logger.setLevel(level=level)
    return logger


__all__ = ["get_logger"]
