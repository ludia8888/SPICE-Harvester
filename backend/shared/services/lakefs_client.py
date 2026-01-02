"""
lakeFS client (REST) for repository/branch/commit/merge operations.

This module intentionally keeps the surface area small so the rest of the
codebase can replace custom versioning/merge logic with lakeFS primitives.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from shared.config.service_config import ServiceConfig


class LakeFSError(RuntimeError):
    pass


class LakeFSAuthError(LakeFSError):
    pass


class LakeFSNotFoundError(LakeFSError):
    pass


class LakeFSConflictError(LakeFSError):
    pass


@dataclass(frozen=True)
class LakeFSConfig:
    api_url: str
    access_key_id: str
    secret_access_key: str

    @staticmethod
    def from_env() -> "LakeFSConfig":
        api_url = (os.getenv("LAKEFS_API_URL") or ServiceConfig.get_lakefs_api_url()).rstrip("/")
        access_key_id = str(os.getenv("LAKEFS_ACCESS_KEY_ID") or "").strip()
        secret_access_key = str(os.getenv("LAKEFS_SECRET_ACCESS_KEY") or "").strip()
        if not access_key_id or not secret_access_key:
            raise LakeFSAuthError(
                "LAKEFS_ACCESS_KEY_ID and LAKEFS_SECRET_ACCESS_KEY are required to use lakeFS"
            )
        return LakeFSConfig(
            api_url=api_url,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        )


def _extract_commit_id(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("id", "commit_id", "commitId", "hash", "reference"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    raise LakeFSError(f"lakeFS response missing commit id: {payload!r}")


def _normalize_metadata(metadata: Dict[str, Any]) -> Dict[str, str]:
    """
    lakeFS commit/merge metadata is stored as string key-value pairs.
    Normalize any scalar types into strings and JSON-encode complex values.
    """

    normalized: Dict[str, str] = {}
    for raw_key, raw_value in (metadata or {}).items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if raw_value is None:
            continue
        if isinstance(raw_value, bool):
            normalized[key] = "true" if raw_value else "false"
            continue
        if isinstance(raw_value, (int, float, str)):
            normalized[key] = str(raw_value)
            continue
        try:
            normalized[key] = json.dumps(raw_value, ensure_ascii=False, sort_keys=True)
        except Exception:
            normalized[key] = str(raw_value)
    return normalized


class LakeFSClient:
    """
    Minimal async lakeFS REST client.

    Notes:
    - Authentication uses basic auth (access_key_id / secret_access_key).
    - This client is intentionally "dumb": it does not auto-create repositories.
    """

    def __init__(self, *, config: Optional[LakeFSConfig] = None, timeout_seconds: int = 30) -> None:
        self._config = config or LakeFSConfig.from_env()
        self._timeout = float(timeout_seconds)

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=f"{self._config.api_url}/api/v1",
            auth=(self._config.access_key_id, self._config.secret_access_key),
            timeout=self._timeout,
        )

    async def create_branch(self, *, repository: str, name: str, source: str = "main") -> None:
        repository = str(repository).strip()
        name = str(name).strip()
        source = str(source).strip() or "main"
        if not repository:
            raise ValueError("repository is required")
        if not name:
            raise ValueError("name is required")
        async with self._client() as client:
            resp = await client.post(
                f"/repositories/{repository}/branches",
                json={"name": name, "source": source},
            )
        if resp.status_code in {401, 403}:
            raise LakeFSAuthError(resp.text)
        if resp.status_code == 404:
            raise LakeFSNotFoundError(resp.text)
        if resp.status_code in {409, 412}:
            raise LakeFSConflictError(resp.text)
        if not resp.is_success:
            raise LakeFSError(f"lakeFS create_branch failed ({resp.status_code}): {resp.text}")

    async def delete_branch(self, *, repository: str, name: str) -> None:
        repository = str(repository).strip()
        name = str(name).strip()
        if not repository:
            raise ValueError("repository is required")
        if not name:
            raise ValueError("name is required")
        async with self._client() as client:
            resp = await client.delete(f"/repositories/{repository}/branches/{name}")
        if resp.status_code in {401, 403}:
            raise LakeFSAuthError(resp.text)
        if resp.status_code == 404:
            raise LakeFSNotFoundError(resp.text)
        if resp.status_code in {409, 412}:
            raise LakeFSConflictError(resp.text)
        if not resp.is_success:
            raise LakeFSError(f"lakeFS delete_branch failed ({resp.status_code}): {resp.text}")

    async def commit(
        self,
        *,
        repository: str,
        branch: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        repository = str(repository).strip()
        branch = str(branch).strip()
        message = str(message).strip() or "commit"
        if not repository:
            raise ValueError("repository is required")
        if not branch:
            raise ValueError("branch is required")
        payload: Dict[str, Any] = {"message": message}
        if metadata:
            payload["metadata"] = _normalize_metadata(metadata)
        async with self._client() as client:
            resp = await client.post(
                f"/repositories/{repository}/branches/{branch}/commits",
                json=payload,
            )
        if resp.status_code in {401, 403}:
            raise LakeFSAuthError(resp.text)
        if resp.status_code == 404:
            raise LakeFSNotFoundError(resp.text)
        if resp.status_code in {409, 412}:
            raise LakeFSConflictError(resp.text)
        if not resp.is_success:
            raise LakeFSError(f"lakeFS commit failed ({resp.status_code}): {resp.text}")
        return _extract_commit_id(resp.json())

    async def get_branch_head_commit_id(self, *, repository: str, branch: str) -> str:
        repository = str(repository).strip()
        branch = str(branch).strip()
        if not repository:
            raise ValueError("repository is required")
        if not branch:
            raise ValueError("branch is required")
        async with self._client() as client:
            resp = await client.get(f"/repositories/{repository}/branches/{branch}")
        if resp.status_code in {401, 403}:
            raise LakeFSAuthError(resp.text)
        if resp.status_code == 404:
            raise LakeFSNotFoundError(resp.text)
        if not resp.is_success:
            raise LakeFSError(f"lakeFS get_branch failed ({resp.status_code}): {resp.text}")
        payload = resp.json()
        if isinstance(payload, dict):
            commit_id = payload.get("commit_id") or payload.get("commitId") or payload.get("head_commit_id")
            if isinstance(commit_id, str) and commit_id.strip():
                return commit_id.strip()
        raise LakeFSError(f"lakeFS branch response missing commit_id: {payload!r}")

    async def list_diff_objects(
        self,
        *,
        repository: str,
        ref: str,
        since: str,
        prefix: Optional[str] = None,
        amount: int = 1000,
    ) -> List[Dict[str, Any]]:
        repository = str(repository).strip()
        ref = str(ref).strip()
        since = str(since).strip()
        if not repository:
            raise ValueError("repository is required")
        if not ref:
            raise ValueError("ref is required")
        if not since:
            raise ValueError("since is required")

        base_params: Dict[str, Any] = {"amount": max(1, int(amount))}
        if prefix:
            base_params["prefix"] = str(prefix).strip()

        results: List[Dict[str, Any]] = []
        after: Optional[str] = None
        endpoint_template = f"/repositories/{repository}/refs/{ref}/diff"
        alt_template = f"/repositories/{repository}/refs/{since}/diff/{ref}"
        mode = "ref_since"
        async with self._client() as client:
            while True:
                params = dict(base_params)
                if after:
                    params["after"] = after
                if mode == "ref_since":
                    params["since"] = since
                    resp = await client.get(endpoint_template, params=params)
                    if resp.status_code in {404, 500} and "invalid api endpoint" in resp.text.lower():
                        mode = "ref_ref"
                        params.pop("since", None)
                        resp = await client.get(alt_template, params=params)
                else:
                    resp = await client.get(alt_template, params=params)
                if resp.status_code in {401, 403}:
                    raise LakeFSAuthError(resp.text)
                if resp.status_code in {404, 500} and "invalid api endpoint" in resp.text.lower():
                    raise LakeFSError(f"lakeFS diff endpoint unsupported: {resp.text}")
                if resp.status_code == 404:
                    raise LakeFSNotFoundError(resp.text)
                if not resp.is_success:
                    raise LakeFSError(f"lakeFS diff failed ({resp.status_code}): {resp.text}")
                payload = resp.json()
                items = []
                if isinstance(payload, dict):
                    items = payload.get("results") or payload.get("diffs") or []
                elif isinstance(payload, list):
                    items = payload
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            results.append(item)
                pagination = payload.get("pagination") if isinstance(payload, dict) else {}
                next_offset = None
                if isinstance(pagination, dict):
                    next_offset = (
                        pagination.get("next_offset")
                        or pagination.get("nextOffset")
                        or pagination.get("next")
                    )
                if not next_offset:
                    break
                after = str(next_offset).strip() or None
                if not after:
                    break
        return results

    async def merge(
        self,
        *,
        repository: str,
        source_ref: str,
        destination_branch: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
        allow_empty: bool = False,
    ) -> str:
        repository = str(repository).strip()
        source_ref = str(source_ref).strip()
        destination_branch = str(destination_branch).strip()
        message = str(message).strip() or "merge"
        if not repository:
            raise ValueError("repository is required")
        if not source_ref:
            raise ValueError("source_ref is required")
        if not destination_branch:
            raise ValueError("destination_branch is required")
        payload: Dict[str, Any] = {"message": message, "allow_empty": bool(allow_empty)}
        if metadata:
            payload["metadata"] = _normalize_metadata(metadata)
        async with self._client() as client:
            resp = await client.post(
                f"/repositories/{repository}/refs/{source_ref}/merge/{destination_branch}",
                json=payload,
            )
        if resp.status_code in {401, 403}:
            raise LakeFSAuthError(resp.text)
        if resp.status_code == 404:
            raise LakeFSNotFoundError(resp.text)
        if resp.status_code in {409, 412}:
            raise LakeFSConflictError(resp.text)
        if not resp.is_success:
            raise LakeFSError(f"lakeFS merge failed ({resp.status_code}): {resp.text}")
        return _extract_commit_id(resp.json())
