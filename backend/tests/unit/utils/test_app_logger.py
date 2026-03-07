from __future__ import annotations

import logging

import pytest

from shared.utils.app_logger import configure_logging


@pytest.mark.unit
def test_configure_logging_suppresses_py4j_info_noise() -> None:
    clientserver_logger = logging.getLogger("py4j.clientserver")
    java_gateway_logger = logging.getLogger("py4j.java_gateway")
    original_levels = (clientserver_logger.level, java_gateway_logger.level)

    try:
        clientserver_logger.setLevel(logging.NOTSET)
        java_gateway_logger.setLevel(logging.NOTSET)

        configure_logging("INFO")

        assert clientserver_logger.level == logging.WARNING
        assert java_gateway_logger.level == logging.WARNING
    finally:
        clientserver_logger.setLevel(original_levels[0])
        java_gateway_logger.setLevel(original_levels[1])
