from __future__ import annotations

from typing import Mapping, Optional

import httpx


def build_agent_internal_headers(
    *,
    agent_token: str,
    delegated_user_bearer: Optional[str] = None,
    extra_headers: Optional[Mapping[str, str]] = None,
) -> dict[str, str]:
    token = str(agent_token or "").strip()
    if not token:
        raise ValueError("agent_token is required for internal Agent calls")

    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
    }
    delegated = str(delegated_user_bearer or "").strip()
    if delegated:
        if delegated.lower().startswith("bearer "):
            headers["X-Delegated-Authorization"] = delegated
        else:
            headers["X-Delegated-Authorization"] = f"Bearer {delegated}"
    if extra_headers:
        headers.update({str(k): str(v) for k, v in extra_headers.items()})
    return headers


class AgentInternalClient:
    def __init__(self, *, base_url: str, agent_token: str, timeout_seconds: float = 60.0) -> None:
        cleaned_base = str(base_url or "").strip().rstrip("/")
        if not cleaned_base:
            raise ValueError("base_url is required for AgentInternalClient")
        self._base_url = cleaned_base
        self._agent_token = str(agent_token or "").strip()
        if not self._agent_token:
            raise ValueError("agent_token is required for AgentInternalClient")
        self._timeout_seconds = float(timeout_seconds)

    async def request(
        self,
        *,
        method: str,
        path: str,
        delegated_user_bearer: Optional[str] = None,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[str, str]] = None,
        json: Optional[object] = None,
    ) -> httpx.Response:
        normalized_path = "/" + str(path or "").lstrip("/")
        request_headers = build_agent_internal_headers(
            agent_token=self._agent_token,
            delegated_user_bearer=delegated_user_bearer,
            extra_headers=headers,
        )
        async with httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout_seconds) as client:
            return await client.request(
                method=str(method or "GET").upper(),
                url=normalized_path,
                headers=request_headers,
                params=params,
                json=json,
            )
