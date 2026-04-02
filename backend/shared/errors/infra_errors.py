from __future__ import annotations

import httpx


class RegistryUnavailableError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        registry: str | None = None,
        operation: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.registry = registry
        self.operation = operation
        self.cause = cause


class StorageUnavailableError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        operation: str | None = None,
        bucket: str | None = None,
        path: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.operation = operation
        self.bucket = bucket
        self.path = path
        self.cause = cause


class UpstreamUnavailableError(httpx.RequestError):
    def __init__(
        self,
        message: str,
        *,
        service: str,
        operation: str,
        path: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message, request=None)
        self.service = service
        self.operation = operation
        self.path = path
        self.cause = cause
