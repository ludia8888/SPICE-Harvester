"""QA helpers for Foundry E2E testing.

Provides:
- BugTracker: collects bugs without failing tests, dumps JSON report
- QAClient: async httpx wrapper for BFF/OMS/ES calls
- Real-time API CSV fetchers (Open-Meteo, Frankfurter, USGS)
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from tests.utils.auth import bff_auth_headers, oms_auth_headers


# ─── Bug Tracker ─────────────────────────────────────────────────────────────


@dataclass
class BugRecord:
    phase: str
    step: str
    endpoint: str
    expected: str
    actual: str
    severity: str = "P1"  # P0=blocker, P1=critical, P2=major, P3=minor
    persona: Optional[str] = None
    status_code: Optional[int] = None
    traceback_str: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class BugTracker:
    """Collect bugs in real-time without failing the test."""

    def __init__(self) -> None:
        self.bugs: List[BugRecord] = []
        self.pass_count: int = 0
        self.fail_count: int = 0

    def record(
        self,
        phase: str,
        step: str,
        endpoint: str,
        expected: str,
        actual: str,
        severity: str = "P1",
        persona: Optional[str] = None,
        status_code: Optional[int] = None,
    ) -> None:
        self.bugs.append(
            BugRecord(
                phase=phase,
                step=step,
                endpoint=endpoint,
                expected=expected,
                actual=actual,
                severity=severity,
                persona=persona,
                status_code=status_code,
                traceback_str=traceback.format_exc(),
            )
        )
        self.fail_count += 1

    def record_pass(self) -> None:
        self.pass_count += 1

    def dump_json(self, path: str = "qa_bugs.json") -> None:
        data = [
            {
                "phase": b.phase,
                "step": b.step,
                "endpoint": b.endpoint,
                "expected": b.expected,
                "actual": b.actual,
                "severity": b.severity,
                "persona": b.persona,
                "status_code": b.status_code,
                "traceback": b.traceback_str,
                "timestamp": b.timestamp,
            }
            for b in self.bugs
        ]
        Path(path).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def summary(self) -> str:
        lines = [
            f"\n{'=' * 60}",
            f"  QA SUMMARY: PASS={self.pass_count}  FAIL={self.fail_count}  TOTAL={self.pass_count + self.fail_count}",
            f"{'=' * 60}",
        ]
        by_phase: Dict[str, List[BugRecord]] = {}
        for b in self.bugs:
            by_phase.setdefault(b.phase, []).append(b)
        for phase, bugs in sorted(by_phase.items()):
            sev_counts = {}
            for b in bugs:
                sev_counts[b.severity] = sev_counts.get(b.severity, 0) + 1
            sev_str = ", ".join(f"{k}:{v}" for k, v in sorted(sev_counts.items()))
            lines.append(f"  {phase}: {len(bugs)} bug(s) [{sev_str}]")
            for b in bugs:
                who = f" persona={b.persona}" if b.persona else ""
                code = f" status={b.status_code}" if b.status_code is not None else ""
                lines.append(f"    [{b.severity}]{who}{code} {b.step} {b.endpoint}: {b.actual[:100]}")
        if not self.bugs:
            lines.append("  No bugs found!")
        lines.append(f"{'=' * 60}\n")
        return "\n".join(lines)


# ─── QA Client ───────────────────────────────────────────────────────────────


class QAClient:
    """Async HTTP client for BFF/OMS/ES calls during QA."""

    def __init__(
        self,
        *,
        bff_url: Optional[str] = None,
        oms_url: Optional[str] = None,
        es_url: Optional[str] = None,
    ) -> None:
        self.bff_url = (
            bff_url
            or os.getenv("BFF_BASE_URL")
            or os.getenv("BFF_URL")
            or "http://localhost:8002"
        ).rstrip("/")
        self.oms_url = (
            oms_url
            or os.getenv("OMS_BASE_URL")
            or os.getenv("OMS_URL")
            or "http://localhost:8000"
        ).rstrip("/")
        _es_port = os.getenv("ELASTICSEARCH_PORT") or os.getenv("ELASTICSEARCH_PORT_HOST") or "9200"
        self.es_url = (
            es_url
            or os.getenv("ELASTICSEARCH_URL")
            or f"http://{os.getenv('ELASTICSEARCH_HOST', 'localhost')}:{_es_port}"
        ).rstrip("/")

        self.suffix = uuid.uuid4().hex[:10]
        self.db_name = f"qa_{self.suffix}"
        self.user_id = f"qa-user-{self.suffix}"

        _base = bff_auth_headers()
        self.headers: Dict[str, str] = {
            **_base,
            "X-DB-Name": self.db_name,
            "X-User-ID": self.user_id,
            "X-User-Type": "user",
        }
        self.oms_headers: Dict[str, str] = oms_auth_headers()
        self.timeout = float(os.getenv("QA_HTTP_TIMEOUT", "180") or 180)
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "QAClient":
        self._client = httpx.AsyncClient(headers=self.headers, timeout=self.timeout)
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        assert self._client is not None, "QAClient not entered as context manager"
        return self._client

    # ── Database ──────────────────────────────────────────────────────

    async def create_db(self) -> None:
        resp = await self.client.post(
            f"{self.bff_url}/api/v1/databases",
            json={"name": self.db_name, "description": "Foundry E2E QA"},
        )
        if resp.status_code == 409:
            return
        resp.raise_for_status()
        command_id = str(((resp.json().get("data") or {}) or {}).get("command_id") or "")
        if command_id:
            await self.wait_for_command(command_id, timeout=180)

    async def grant_db_role(self) -> None:
        import asyncpg  # type: ignore

        dsn_candidates = [
            (os.getenv("POSTGRES_URL") or "").strip(),
            "postgresql://spiceadmin:spicepass123@localhost:5433/spicedb",
            "postgresql://spiceadmin:spicepass123@localhost:5432/spicedb",
        ]
        from shared.security.database_access import ensure_database_access_table

        for dsn in dsn_candidates:
            if not dsn:
                continue
            try:
                conn = await asyncpg.connect(dsn)
            except Exception:
                continue
            try:
                await ensure_database_access_table(conn)
                await conn.execute(
                    """
                    INSERT INTO database_access (
                        db_name, principal_type, principal_id, principal_name, role, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                    ON CONFLICT (db_name, principal_type, principal_id)
                    DO UPDATE SET role = EXCLUDED.role, updated_at = NOW()
                    """,
                    self.db_name, "user", self.user_id, self.user_id, "Owner",
                )
                return
            finally:
                await conn.close()

    # ── Command polling ───────────────────────────────────────────────

    async def wait_for_command(self, command_id: str, *, timeout: int = 120) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            resp = await self.client.get(f"{self.bff_url}/api/v1/commands/{command_id}/status")
            if resp.status_code == 200:
                payload = resp.json()
                status = str(
                    payload.get("status") or (payload.get("data") or {}).get("status") or ""
                ).upper()
                if status in {"COMPLETED", "SUCCESS", "SUCCEEDED", "DONE"}:
                    return
                if status in {"FAILED", "ERROR"}:
                    raise AssertionError(f"Command failed: {payload}")
            await asyncio.sleep(0.5)
        raise AssertionError(f"Timed out waiting for command {command_id}")

    # ── CSV upload ────────────────────────────────────────────────────

    async def upload_csv(
        self,
        name: str,
        csv_bytes: bytes,
        description: str = "",
    ) -> Dict[str, Any]:
        idem_key = f"idem-{name}-{self.suffix}"
        resp = await self.client.post(
            f"{self.bff_url}/api/v1/pipelines/datasets/csv-upload",
            params={"db_name": self.db_name, "branch": "main"},
            headers={**self.headers, "Idempotency-Key": idem_key},
            data={
                "dataset_name": name,
                "description": description or f"QA dataset: {name}",
                "delimiter": ",",
                "has_header": "true",
            },
            files={"file": (f"{name}.csv", csv_bytes, "text/csv")},
        )
        resp.raise_for_status()
        return resp.json().get("data") or {}

    # ── Ontology ──────────────────────────────────────────────────────

    async def wait_for_ontology(self, class_id: str, *, timeout: int = 90) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            resp = await self.client.get(
                f"{self.oms_url}/api/v1/database/{self.db_name}/ontology/{class_id}",
                params={"branch": "main"},
                headers=self.oms_headers,
            )
            if resp.status_code == 200:
                return
            await asyncio.sleep(1.0)
        raise AssertionError(f"Timed out waiting for ontology class {class_id}")

    # ── ES polling ────────────────────────────────────────────────────

    async def wait_for_es_doc(
        self,
        index_name: str,
        doc_id: str,
        *,
        timeout: int = 240,
    ) -> Dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                resp = await self.client.get(f"{self.es_url}/{index_name}/_doc/{doc_id}")
            except httpx.HTTPError:
                await asyncio.sleep(1.0)
                continue
            if resp.status_code == 200:
                payload = resp.json()
                if payload.get("found") is True or payload.get("_source"):
                    return payload
            await asyncio.sleep(1.0)
        raise AssertionError(f"Timed out waiting for ES doc {doc_id} in {index_name}")

    async def wait_for_es_overlay(
        self,
        index_name: str,
        doc_id: str,
        *,
        timeout: int = 120,
    ) -> Dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                resp = await self.client.get(f"{self.es_url}/{index_name}/_doc/{doc_id}")
            except httpx.HTTPError:
                await asyncio.sleep(1.0)
                continue
            if resp.status_code == 200:
                payload = resp.json()
                source = payload.get("_source")
                if isinstance(source, dict):
                    return source
            await asyncio.sleep(1.0)
        raise AssertionError(f"Timed out waiting for ES overlay doc {doc_id}")


# ─── Real-time API → CSV converters ─────────────────────────────────────────


async def fetch_open_meteo_csv() -> bytes:
    """Fetch São Paulo hourly weather (past 7 days) → CSV bytes."""
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=-23.5505&longitude=-46.6333"
        "&hourly=temperature_2m,windspeed_10m,precipitation"
        "&past_days=7&forecast_days=0"
    )
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    hourly = data.get("hourly") or {}
    times = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    winds = hourly.get("windspeed_10m") or []
    precips = hourly.get("precipitation") or []

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["time", "temperature_2m", "windspeed_10m", "precipitation"])
    for i in range(len(times)):
        writer.writerow([
            times[i] if i < len(times) else "",
            temps[i] if i < len(temps) else "",
            winds[i] if i < len(winds) else "",
            precips[i] if i < len(precips) else "",
        ])
    return buf.getvalue().encode("utf-8")


async def fetch_frankfurter_csv() -> bytes:
    """Fetch BRL exchange rates (2024 full year) → CSV bytes."""
    url = "https://api.frankfurter.dev/v1/2024-01-01..2024-12-31?base=BRL&symbols=USD,EUR,KRW"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    rates = data.get("rates") or {}
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["date", "base", "usd_rate", "eur_rate", "krw_rate"])
    for date_str in sorted(rates.keys()):
        day_rates = rates[date_str]
        writer.writerow([
            date_str,
            data.get("base", "BRL"),
            day_rates.get("USD", ""),
            day_rates.get("EUR", ""),
            day_rates.get("KRW", ""),
        ])
    return buf.getvalue().encode("utf-8")


async def fetch_usgs_earthquake_csv(*, min_magnitude: float = 2.5, max_rows: int = 500) -> bytes:
    """Fetch USGS earthquakes (past month, mag >= 2.5) → CSV bytes."""
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    features = data.get("features") or []
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["eq_id", "magnitude", "place", "latitude", "longitude", "depth_km", "time"])
    count = 0
    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or [0, 0, 0]
        mag = props.get("mag")
        if mag is None or mag < min_magnitude:
            continue
        writer.writerow([
            feat.get("id", ""),
            mag,
            (props.get("place") or "").replace(",", ";"),  # avoid CSV comma issues
            coords[1] if len(coords) > 1 else "",
            coords[0] if len(coords) > 0 else "",
            coords[2] if len(coords) > 2 else "",
            props.get("time", ""),
        ])
        count += 1
        if count >= max_rows:
            break
    return buf.getvalue().encode("utf-8")
