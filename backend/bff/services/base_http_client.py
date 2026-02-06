from __future__ import annotations

from typing import Any

from shared.utils.async_utils import aclose_if_present


class ManagedAsyncClient:
    client: Any

    async def close(self) -> None:
        await aclose_if_present(getattr(self, "client", None))
