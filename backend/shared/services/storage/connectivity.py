from __future__ import annotations

from typing import Any, Tuple, Type


class AsyncClientPingMixin:
    _ping_exception_types: Tuple[Type[BaseException], ...] = (Exception,)

    @property
    def client(self) -> Any:
        raise NotImplementedError

    async def ping(self) -> bool:
        try:
            return bool(await self.client.ping())
        except self._ping_exception_types:
            return False
