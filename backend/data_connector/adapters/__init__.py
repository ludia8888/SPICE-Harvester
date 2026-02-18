"""Connector adapter abstractions and factory."""

from data_connector.adapters.base import ConnectorAdapter, ConnectorConnectionTestResult, ConnectorExtractResult
from data_connector.adapters.factory import ConnectorAdapterFactory

__all__ = [
    "ConnectorAdapter",
    "ConnectorConnectionTestResult",
    "ConnectorExtractResult",
    "ConnectorAdapterFactory",
]
