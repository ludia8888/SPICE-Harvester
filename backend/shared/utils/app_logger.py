"""
🔥 THINK ULTRA! Logging utilities for SPICE HARVESTER
Centralized logging configuration for all services
"""

import logging
import sys
from typing import Optional, Union

from shared.observability.logging import TraceContextFilter, install_trace_context_filter

DEFAULT_LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - trace_id=%(trace_id)s span_id=%(span_id)s "
    "req_id=%(request_id)s corr_id=%(correlation_id)s db=%(db_name)s - %(message)s"
)


def _normalize_log_level(level: Union[str, int, None]) -> int:
    if level is None:
        return logging.INFO
    if isinstance(level, int):
        return level
    return getattr(logging, str(level).strip().upper(), logging.INFO)


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

    # Ensure trace fields exist for any formatter contract (global record factory).
    install_trace_context_filter()

    # Set log level
    log_level = _normalize_log_level(level)

    logger.setLevel(log_level)

    # Don't attach per-logger handlers if the root logger is already configured and
    # this logger propagates to it.
    if logging.root.handlers and logger.propagate:
        return logger

    # Don't add handlers if already configured
    if logger.handlers:
        return logger

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    # Create formatter
    install_trace_context_filter()
    trace_filter = TraceContextFilter()
    logger.addFilter(trace_filter)
    handler.addFilter(trace_filter)
    formatter = logging.Formatter(
        DEFAULT_LOG_FORMAT
    )
    handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(handler)

    # Prevent duplicate logs
    logger.propagate = False

    return logger


def configure_logging(level: Union[str, int] = "INFO") -> None:
    """
    Configure global logging settings.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    log_level = _normalize_log_level(level)

    # Always install trace context support, even if handlers are already present.
    install_trace_context_filter()

    # Set root logger level (works even when handlers exist)
    logging.root.setLevel(log_level)
    
    # Only add handler if no handlers exist (avoid duplicate handlers)
    if not logging.root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        handler.addFilter(TraceContextFilter())
        formatter = logging.Formatter(
            DEFAULT_LOG_FORMAT
        )
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
