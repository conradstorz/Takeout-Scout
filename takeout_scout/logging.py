"""
Logging configuration for Takeout Scout.

Provides a consistent logger across all modules with optional
loguru support and fallback to standard library logging.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any


# Default log directory
LOG_DIR = Path('./logs')


def setup_logging(log_dir: Path | None = None) -> Any:
    """Set up logging with loguru if available, else standard logging.
    
    Args:
        log_dir: Directory for log files. Defaults to ./logs
        
    Returns:
        Logger instance (loguru.Logger or logging.Logger shim)
    """
    if log_dir is None:
        log_dir = LOG_DIR
    
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / 'takeout_scout.log'
    
    try:
        from loguru import logger
        
        # Remove default handler and add file handler
        logger.remove()
        logger.add(
            log_path,
            rotation='5 MB',
            retention='30 days',
            compression='gz',
            format='{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{function}:{line} | {message}',
            level='DEBUG',
        )
        # Also log to stderr for immediate feedback
        logger.add(
            lambda msg: None,  # Suppress stderr in library mode
            level='WARNING',
            format='{level}: {message}',
        )
        return logger
    
    except ImportError:
        # Fallback to standard library logging
        return _create_fallback_logger(log_path)


class _FallbackLogger:
    """Minimal logger shim matching loguru's interface."""
    
    def __init__(self, log_path: Path) -> None:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s',
            handlers=[
                logging.FileHandler(log_path, encoding='utf-8'),
            ]
        )
        self._log = logging.getLogger('takeout_scout')
    
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log.info(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log.error(msg, *args, **kwargs)
    
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log.exception(msg, *args, **kwargs)
    
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log.debug(msg, *args, **kwargs)


def _create_fallback_logger(log_path: Path) -> _FallbackLogger:
    """Create a fallback logger when loguru is not available."""
    return _FallbackLogger(log_path)


# Module-level logger instance
logger = setup_logging()
