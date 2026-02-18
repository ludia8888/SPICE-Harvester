from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ConnectorConnectionTestResult:
    ok: bool
    message: str
    details: Dict[str, Any]


@dataclass(frozen=True)
class ConnectorExtractResult:
    columns: List[str]
    rows: List[Dict[str, Any]]
    next_state: Dict[str, Any]


class ConnectorAdapter(ABC):
    """Common adapter contract for connectivity connectors."""

    connector_kind: str

    @abstractmethod
    async def test_connection(self, *, config: Dict[str, Any], secrets: Dict[str, Any]) -> ConnectorConnectionTestResult:
        raise NotImplementedError

    @abstractmethod
    async def get_public_configuration(self, *, config: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def snapshot_extract(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
    ) -> ConnectorExtractResult:
        raise NotImplementedError

    @abstractmethod
    async def incremental_extract(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
        sync_state: Dict[str, Any],
    ) -> ConnectorExtractResult:
        raise NotImplementedError

    @abstractmethod
    async def cdc_extract(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Dict[str, Any],
        sync_state: Dict[str, Any],
    ) -> ConnectorExtractResult:
        raise NotImplementedError

    @abstractmethod
    async def peek_change_token(
        self,
        *,
        config: Dict[str, Any],
        secrets: Dict[str, Any],
        import_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        raise NotImplementedError
