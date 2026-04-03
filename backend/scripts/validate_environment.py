#!/usr/bin/env python3
"""
SPICE Harvester Environment Validator (SSoT)

Validates effective configuration and connectivity using the centralized
Pydantic settings SSoT (`shared.config.settings.get_settings`).

Security posture:
- Does NOT auto-load `.env` (set `SPICE_LOAD_DOTENV=true` explicitly for local dev).
- Does NOT print raw secrets (tokens/passwords are redacted).
"""

from __future__ import annotations

import asyncio
import os
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import aiohttp
import asyncpg
from confluent_kafka.admin import AdminClient
from elasticsearch import AsyncElasticsearch
import redis.asyncio as redis

from shared.config.settings import get_settings


def _redact_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return raw
    try:
        parts = urlsplit(raw)
        if not (parts.username or parts.password):
            return raw
        hostname = parts.hostname or ""
        netloc = hostname
        if parts.port:
            netloc = f"{hostname}:{parts.port}"
        if parts.username:
            netloc = f"{parts.username}:***@{netloc}"
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        return "<redacted-url>"


class EnvironmentValidator:
    def __init__(self) -> None:
        self.results: dict[str, bool] = {}
        self.errors: list[str] = []
        self.settings = get_settings()

    def print_header(self, title: str) -> None:
        print("\n" + "=" * 70)
        print(f"🔍 {title}")
        print("=" * 70)

    def check_result(self, name: str, success: bool, message: str = "") -> None:
        if success:
            print(f"  ✅ {name}: {message if message else 'OK'}")
            self.results[name] = True
            return
        print(f"  ❌ {name}: {message if message else 'FAILED'}")
        self.results[name] = False
        self.errors.append(f"{name}: {message}")

    def validate_effective_settings(self) -> None:
        self.print_header("EFFECTIVE SETTINGS (SSoT)")

        cfg = self.settings
        db = cfg.database
        services = cfg.services
        storage = cfg.storage

        print("  📋 Runtime mode:")
        self.check_result("Environment", True, cfg.environment.value)
        self.check_result("HTTPS Enabled", True, str(bool(services.use_https)).lower())
        self.check_result(
            "Runtime DDL Bootstrap",
            not (cfg.environment.value in {"staging", "production"} and cfg.allow_runtime_ddl_bootstrap),
            (
                "disabled"
                if not cfg.allow_runtime_ddl_bootstrap
                else "enabled (dev/test compatibility fallback)"
            ),
        )

        print("\n  📋 Endpoints (redacted):")
        self.check_result("OMS Base URL", True, services.oms_base_url)
        self.check_result("BFF Base URL", True, services.bff_base_url)
        self.check_result("Funnel Runtime", True, "internal (in-process)")
        self.check_result("Agent Base URL", True, services.agent_base_url)

        self.check_result("Postgres URL", True, _redact_url(db.postgres_url))
        self.check_result("Redis URL", True, _redact_url(db.redis_url))
        self.check_result("Kafka Bootstrap", True, db.kafka_servers)
        self.check_result("Elasticsearch URL", True, _redact_url(db.elasticsearch_url))

        self.check_result("MinIO Endpoint", True, storage.minio_endpoint_url)
        self.check_result("Event Store Bucket", True, storage.event_store_bucket)

    async def check_postgresql(self) -> None:
        self.print_header("POSTGRESQL CONNECTION CHECK")

        dsn = self.settings.database.postgres_url
        try:
            conn = await asyncpg.connect(dsn=dsn)
            self.check_result("PostgreSQL Connection", True, f"Connected ({_redact_url(dsn)})")

            schema_exists = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata
                    WHERE schema_name = 'spice_event_registry'
                )
                """
            )
            self.check_result("spice_event_registry Schema", bool(schema_exists))

            if schema_exists:
                schema_migrations_exists = await conn.fetchval(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name = 'schema_migrations'
                    )
                    """
                )
                self.check_result("schema_migrations Table", bool(schema_migrations_exists))

                processed_events_exists = await conn.fetchval(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'spice_event_registry'
                          AND table_name = 'processed_events'
                    )
                    """
                )
                self.check_result("processed_events Table", bool(processed_events_exists))

                aggregate_versions_exists = await conn.fetchval(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'spice_event_registry'
                          AND table_name = 'aggregate_versions'
                    )
                    """
                )
                self.check_result("aggregate_versions Table", bool(aggregate_versions_exists))

            await conn.close()
        except Exception as exc:
            self.check_result("PostgreSQL Connection", False, str(exc))

    async def check_redis(self) -> None:
        self.print_header("REDIS CONNECTION CHECK")

        url = self.settings.database.redis_url
        try:
            client = redis.from_url(url)
            await client.ping()
            self.check_result("Redis Connection", True, f"Connected ({_redact_url(url)})")

            keys = await client.keys("*")
            self.check_result("Redis Keys", True, f"{len(keys)} keys found")

            await client.aclose()
        except Exception as exc:
            self.check_result("Redis Connection", False, str(exc))

    async def check_elasticsearch(self) -> None:
        self.print_header("ELASTICSEARCH CONNECTION CHECK")

        db = self.settings.database
        base = f"http://{db.elasticsearch_host}:{db.elasticsearch_port}"
        try:
            kwargs: dict[str, Any] = {}
            if db.elasticsearch_username and db.elasticsearch_password:
                kwargs["basic_auth"] = (db.elasticsearch_username, db.elasticsearch_password)

            es = AsyncElasticsearch([base], request_timeout=db.elasticsearch_request_timeout, **kwargs)
            info = await es.info()
            version = ((info or {}).get("version") or {}).get("number", "unknown")
            self.check_result("Elasticsearch Connection", True, f"Version {version}")

            indices = await es.cat.indices(format="json")
            spice_indices = [idx for idx in (indices or []) if "spice" in str(idx.get("index", "")).lower()]
            self.check_result("Elasticsearch Indices", True, f"{len(spice_indices)} SPICE indices")

            await es.close()
        except Exception as exc:
            self.check_result("Elasticsearch Connection", False, str(exc))

    def check_kafka(self) -> None:
        self.print_header("KAFKA CONNECTION CHECK")

        bootstrap = self.settings.database.kafka_servers
        try:
            admin_client = AdminClient(
                {
                    "bootstrap.servers": bootstrap,
                    "socket.timeout.ms": 5000,
                    "api.version.request.timeout.ms": 5000,
                }
            )
            metadata = admin_client.list_topics(timeout=5)
            topics = list((metadata.topics or {}).keys())
            self.check_result("Kafka Connection", True, f"{len(topics)} topics ({bootstrap})")

            required_topics = [
                "instance_commands",
                "instance_events",
                "ontology_commands",
                "ontology_events",
                "pipeline-jobs",
                "objectify-jobs",
            ]
            missing = [t for t in required_topics if t not in topics]
            if missing:
                self.check_result("Required Kafka Topics", False, f"Missing: {missing}")
            else:
                self.check_result("Required Kafka Topics", True, "All required topics exist")
        except Exception as exc:
            self.check_result("Kafka Connection", False, str(exc))

    async def check_services(self) -> None:
        self.print_header("MICROSERVICES HEALTH CHECK")

        services = self.settings.services
        targets = [
            ("OMS", f"{services.oms_base_url}/health"),
            ("BFF", f"{services.bff_base_url}/api/v1/health"),
            ("Agent", f"{services.agent_base_url}/health"),
        ]
        self.check_result("Funnel Service", True, "Skipped (internal runtime)")

        async with aiohttp.ClientSession() as session:
            for name, url in targets:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            self.check_result(f"{name} Service", True, "Healthy")
                        else:
                            self.check_result(f"{name} Service", False, f"HTTP {resp.status}")
                except Exception as exc:
                    self.check_result(f"{name} Service", False, str(exc))

    def check_docker_config(self) -> None:
        self.print_header("DOCKER COMPOSE CHECK")

        docker_compose_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docker-compose.yml")
        if not os.path.exists(docker_compose_path):
            self.check_result("backend/docker-compose.yml", False, "File not found")
            return

        content = open(docker_compose_path, "r", encoding="utf-8").read()
        checks = [
            ("PostgreSQL Port", (":5432" in content) and (":5433" in content or "POSTGRES_PORT_HOST" in content)),
            ("Redis Port", "6379:6379" in content or "REDIS_PORT_HOST" in content),
            ("Elasticsearch Port", "9200:9200" in content),
            ("Kafka Port", ":9092" in content and ("39092" in content or "KAFKA_PORT_HOST" in content)),
            ("MinIO Port", ":9000" in content and ("MINIO_PORT_HOST" in content or "9000:9000" in content)),
        ]
        for name, ok in checks:
            self.check_result(name, bool(ok))

    async def check_workers(self) -> None:
        self.print_header("BACKGROUND WORKERS CHECK (BEST-EFFORT)")

        import subprocess

        workers = [
            "message_relay",
            "ontology_worker",
            "projection_worker",
            "instance_worker",
            "pipeline_worker",
        ]

        for worker in workers:
            try:
                result = subprocess.run(["pgrep", "-f", worker], capture_output=True, text=True, check=False)
                if result.returncode == 0:
                    pids = [pid for pid in result.stdout.strip().split("\n") if pid.strip()]
                    self.check_result(worker, True, f"Running (PID: {pids[0]})" if pids else "Running")
                else:
                    self.check_result(worker, False, "Not running")
            except Exception as exc:
                self.check_result(worker, False, str(exc))

    async def run_all_checks(self) -> bool:
        print("\n" + "=" * 70)
        print("🔥 SPICE HARVESTER ENVIRONMENT VALIDATION (SSoT)")
        print("=" * 70)

        self.validate_effective_settings()
        self.check_docker_config()
        self.check_kafka()

        await self.check_postgresql()
        await self.check_redis()
        await self.check_elasticsearch()
        await self.check_services()
        await self.check_workers()

        self.print_header("VALIDATION SUMMARY")
        total = len(self.results)
        passed = sum(1 for v in self.results.values() if v)
        failed = total - passed

        print(f"\n  📊 Results: {passed}/{total} checks passed")
        if failed:
            print(f"\n  ❌ Failed checks ({failed}):")
            for error in self.errors:
                print(f"    - {error}")
        else:
            print("\n  🎉 All checks passed! Environment looks healthy.")

        return failed == 0


async def main() -> int:
    validator = EnvironmentValidator()
    ok = await validator.run_all_checks()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
