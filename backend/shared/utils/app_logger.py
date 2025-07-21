"""
🔥 THINK ULTRA! Logging utilities for SPICE HARVESTER
Centralized logging configuration for all services
"""

import logging
import sys
from typing import Optional


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name (typically __name__)
        level: Optional log level override

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Don't add handlers if already configured
    if logger.handlers:
        return logger

    # Set log level
    log_level = level or logging.INFO
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper(), logging.INFO)

    logger.setLevel(log_level)

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(handler)

    # Prevent duplicate logs
    logger.propagate = False

    return logger


def configure_logging(level: str = "INFO") -> None:
    """
    Configure global logging settings.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Set root logger level (works even when handlers exist)
    logging.root.setLevel(log_level)
    
    # Only add handler if no handlers exist (avoid duplicate handlers)
    if not logging.root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logging.root.addHandler(handler)


# Service-specific loggers
def get_funnel_logger(name: str = "funnel") -> logging.Logger:
    """Get Funnel service logger."""
    return get_logger(f"funnel.{name}")


def get_bff_logger(name: str = "bff") -> logging.Logger:
    """Get BFF service logger."""
    return get_logger(f"bff.{name}")


def get_oms_logger(name: str = "oms") -> logging.Logger:
    """Get OMS service logger."""
    return get_logger(f"oms.{name}")


if __name__ == "__main__":
    # Test logging configuration
    configure_logging("DEBUG")

    test_logger = get_logger("test")
    test_logger.debug("Debug message")
    test_logger.info("Info message")
    test_logger.warning("Warning message")
    test_logger.error("Error message")
    test_logger.critical("Critical message")

    # Test service loggers
    funnel_logger = get_funnel_logger("test")
    funnel_logger.info("Funnel service test message")

    bff_logger = get_bff_logger("test")
    bff_logger.info("BFF service test message")

    oms_logger = get_oms_logger("test")
    oms_logger.info("OMS service test message")
