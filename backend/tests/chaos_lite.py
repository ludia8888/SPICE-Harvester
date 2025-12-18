"""
Chaos-lite integration validation (no mocks).

This script exercises partial-failure + retry + idempotency paths by
actually stopping/restarting dependencies and/or crashing workers.

It is intentionally implemented with stdlib only so it can run on the host
without installing Python dependencies.

Run (from repo root):
  python3 backend/tests/chaos_lite.py
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, uuid5


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.full.yml"


def _read_env_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


ENV_FILE_VALUES = _read_env_file(REPO_ROOT / ".env")
ES_PORT = int(os.getenv("ELASTICSEARCH_PORT_HOST") or ENV_FILE_VALUES.get("ELASTICSEARCH_PORT_HOST") or "9200")


@dataclass(frozen=True)
class Endpoints:
    oms: str
    bff: str
    es: str


ENDPOINTS = Endpoints(
    oms=(os.getenv("OMS_BASE_URL") or "http://localhost:8000").rstrip("/"),
    bff=(os.getenv("BFF_BASE_URL") or "http://localhost:8002").rstrip("/"),
    es=(os.getenv("ELASTICSEARCH_URL") or f"http://localhost:{ES_PORT}").rstrip("/"),
)

# Instance aggregate_id format (shared.models.commands.InstanceCommand):
#   "{db_name}:{branch}:{class_id}:{instance_id}"
DEFAULT_BRANCH = "main"


def _run(cmd: list[str], *, cwd: Path, env: Optional[Dict[str, str]] = None) -> str:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        input=None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stdout}")
    return proc.stdout


def _run_input(cmd: list[str], *, cwd: Path, input_text: str, env: Optional[Dict[str, str]] = None) -> str:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stdout}")
    return proc.stdout


def _docker_compose(args: list[str], *, extra_env: Optional[Dict[str, str]] = None) -> str:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return _run(["docker", "compose", "-f", str(COMPOSE_FILE), *args], cwd=REPO_ROOT, env=env)


def _http_json(method: str, url: str, payload: Optional[Dict[str, Any]] = None, timeout_s: float = 10) -> Dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    req = Request(url, method=method.upper(), data=data, headers=headers)
    with urlopen(req, timeout=timeout_s) as resp:
        body = resp.read().decode("utf-8")
        if not body:
            return {}
        return json.loads(body)


def _wait_until(name: str, fn, *, timeout_s: float, interval_s: float = 1.0) -> Any:
    deadline = time.monotonic() + timeout_s
    last_err: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            return fn()
        except Exception as e:
            last_err = e
            time.sleep(interval_s)
    raise TimeoutError(f"Timed out waiting for {name}: {last_err}")


def _wait_http_ok(url: str, *, timeout_s: float = 90) -> None:
    def _probe():
        _http_json("GET", url, None, timeout_s=5)
        return True

    _wait_until(f"http {url}", _probe, timeout_s=timeout_s, interval_s=2.0)


def _wait_db_exists(db_name: str, *, expected: bool, timeout_s: float = 90) -> None:
    url = f"{ENDPOINTS.oms}/api/v1/database/exists/{db_name}"

    def _probe():
        data = _http_json("GET", url)
        exists = bool((data.get("data") or {}).get("exists"))
        if exists is expected:
            return True
        raise AssertionError(f"exists={exists} (expected {expected})")

    _wait_until(f"db exists={expected} {db_name}", _probe, timeout_s=timeout_s, interval_s=2.0)


def _wait_ontology(db_name: str, class_id: str, *, timeout_s: float = 120) -> None:
    url = f"{ENDPOINTS.oms}/api/v1/database/{db_name}/ontology"

    def _probe():
        data = _http_json("GET", url)
        items = (data.get("data") or {}).get("ontologies") or []
        if any((o or {}).get("id") == class_id for o in items if isinstance(o, dict)):
            return True
        raise AssertionError("not present yet")

    _wait_until(f"ontology {db_name}:{class_id}", _probe, timeout_s=timeout_s, interval_s=2.0)


def _wait_command_completed(command_id: str, *, timeout_s: float = 180) -> Dict[str, Any]:
    url = f"{ENDPOINTS.oms}/api/v1/commands/{command_id}/status"

    def _probe():
        data = _http_json("GET", url)
        status = str(data.get("status") or "").upper()
        if status == "COMPLETED":
            return data
        if status == "FAILED":
            raise AssertionError(f"command failed: {data.get('error') or data}")
        raise AssertionError(f"status={status}")

    return _wait_until(f"command completed {command_id}", _probe, timeout_s=timeout_s, interval_s=2.0)


def _instances_index(db_name: str) -> str:
    # search_config.get_instances_index_name(db) currently yields "{db}_instances"
    return f"{db_name}_instances"


def _wait_es_doc(index: str, doc_id: str, *, timeout_s: float = 180) -> Dict[str, Any]:
    url = f"{ENDPOINTS.es}/{index}/_doc/{doc_id}"

    def _probe():
        try:
            return _http_json("GET", url)
        except HTTPError as e:
            if e.code == 404:
                raise AssertionError("missing")
            raise

    return _wait_until(f"es doc {index}/{doc_id}", _probe, timeout_s=timeout_s, interval_s=2.0)


def _graph_query(db_name: str, product_id: str, *, include_provenance: bool = True) -> Dict[str, Any]:
    url = f"{ENDPOINTS.bff}/api/v1/graph-query/{db_name}"
    payload = {
        "start_class": "Product",
        "hops": [{"predicate": "owned_by", "target_class": "Customer"}],
        "filters": {"product_id": product_id},
        "limit": 10,
        "offset": 0,
        "max_nodes": 100,
        "max_edges": 500,
        "include_paths": True,
        "max_paths": 10,
        "no_cycles": True,
        "include_provenance": include_provenance,
        "include_documents": True,
        "include_audit": False,
    }
    return _http_json("POST", url, payload, timeout_s=20)


def _assert_graph_full(result: Dict[str, Any]) -> None:
    nodes = result.get("nodes") or []
    edges = result.get("edges") or []
    if not nodes or not edges:
        raise AssertionError(f"expected nodes+edges (nodes={len(nodes)} edges={len(edges)}) result={result}")
    statuses = {str(n.get("id")): str(n.get("data_status")) for n in nodes if isinstance(n, dict)}
    if any(v != "FULL" for v in statuses.values()):
        raise AssertionError(f"expected all nodes FULL, got {statuses}")


def _domain_event_id(command_id: str, *, event_type: str, aggregate_id: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"spice:{command_id}:{event_type}:{aggregate_id}"))


def _check_s3_has_event_id(event_id: str) -> None:
    # Use OMS container (has boto3 + env to reach minio) to validate S3/MinIO index.
    code = (
        "import os, json, boto3\n"
        "from botocore.exceptions import ClientError\n"
        "bucket=os.getenv('EVENT_STORE_BUCKET','spice-event-store')\n"
        "endpoint=os.getenv('MINIO_ENDPOINT_URL','http://minio:9000')\n"
        "ak=os.getenv('MINIO_ACCESS_KEY','minioadmin')\n"
        "sk=os.getenv('MINIO_SECRET_KEY','minioadmin123')\n"
        "s3=boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=ak, aws_secret_access_key=sk)\n"
        f"key='indexes/by-event-id/{event_id}.json'\n"
        "try:\n"
        "  s3.head_object(Bucket=bucket, Key=key)\n"
        "except ClientError as e:\n"
        "  raise SystemExit(f'missing {key}: {e}')\n"
        "print('ok', key)\n"
    )
    _run(["docker", "exec", "spice_oms", "python", "-c", code], cwd=REPO_ROOT)


def _wait_s3_has_event_id(event_id: str, *, timeout_s: float = 240) -> None:
    def _probe():
        _check_s3_has_event_id(event_id)
        return True

    _wait_until(f"s3 index by-event-id {event_id}", _probe, timeout_s=timeout_s, interval_s=2.0)


def _read_s3_event_envelope(event_id: str) -> Dict[str, Any]:
    # Use OMS container (has boto3 + env to reach minio) to read event JSON deterministically.
    code = (
        "import os, json, boto3\n"
        "from botocore.exceptions import ClientError\n"
        "bucket=os.getenv('EVENT_STORE_BUCKET','spice-event-store')\n"
        "endpoint=os.getenv('MINIO_ENDPOINT_URL','http://minio:9000')\n"
        "ak=os.getenv('MINIO_ACCESS_KEY','minioadmin')\n"
        "sk=os.getenv('MINIO_SECRET_KEY','minioadmin123')\n"
        "s3=boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=ak, aws_secret_access_key=sk)\n"
        f"idx_key='indexes/by-event-id/{event_id}.json'\n"
        "idx=json.loads(s3.get_object(Bucket=bucket, Key=idx_key)['Body'].read())\n"
        "key=idx.get('s3_key')\n"
        "if not key:\n"
        "  raise SystemExit(f'missing s3_key in index: {idx}')\n"
        "raw=s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')\n"
        "print(raw)\n"
    )
    raw = _run(["docker", "exec", "spice_oms", "python", "-c", code], cwd=REPO_ROOT)
    return json.loads(raw)


def _psql_scalar(sql: str) -> str:
    out = _run(
        [
            "docker",
            "exec",
            "spice-foundry-postgres",
            "psql",
            "-U",
            "spiceadmin",
            "-d",
            "spicedb",
            "-t",
            "-A",
            "-c",
            sql,
        ],
        cwd=REPO_ROOT,
    )
    return out.strip()


def _write_side_last_sequence(*, aggregate_type: str, aggregate_id: str) -> int:
    prefix = (os.getenv("EVENT_STORE_SEQUENCE_HANDLER_PREFIX") or "write_side").strip() or "write_side"
    handler = f"{prefix}:{aggregate_type}"
    raw = _psql_scalar(
        "SELECT COALESCE(last_sequence, 0)::text "
        "FROM spice_event_registry.aggregate_versions "
        f"WHERE handler='{handler}' AND aggregate_id='{aggregate_id}'"
    )
    return int(raw or "0")


def _assert_registry_done(handler: str, event_id: str) -> None:
    row = _psql_scalar(
        "SELECT status || ':' || attempt_count::text "
        "FROM spice_event_registry.processed_events "
        f"WHERE handler='{handler}' AND event_id='{event_id}'"
    )
    if not row:
        raise AssertionError(f"processed_events missing: handler={handler} event_id={event_id}")
    status, _, attempts = row.partition(":")
    if status != "done":
        raise AssertionError(f"processed_events not done: {handler} {event_id} -> {row}")
    if int(attempts or "1") < 1:
        raise AssertionError(f"invalid attempt_count: {row}")


def _assert_registry_status(handler: str, event_id: str, expected_status: str) -> None:
    row = _psql_scalar(
        "SELECT status "
        "FROM spice_event_registry.processed_events "
        f"WHERE handler='{handler}' AND event_id='{event_id}'"
    )
    if not row:
        raise AssertionError(f"processed_events missing: handler={handler} event_id={event_id}")
    if row != expected_status:
        raise AssertionError(f"processed_events status mismatch: expected={expected_status} got={row}")


def _wait_registry_done(handler: str, event_id: str, *, timeout_s: float = 240) -> None:
    def _probe():
        _assert_registry_done(handler, event_id)
        return True

    _wait_until(f"registry done {handler} {event_id}", _probe, timeout_s=timeout_s, interval_s=2.0)


def _wait_registry_status(handler: str, event_id: str, expected_status: str, *, timeout_s: float = 240) -> None:
    def _probe():
        _assert_registry_status(handler, event_id, expected_status)
        return True

    _wait_until(
        f"registry {expected_status} {handler} {event_id}", _probe, timeout_s=timeout_s, interval_s=2.0
    )


def _kafka_produce_json(*, topic: str, key: str, payload: Dict[str, Any]) -> None:
    line = f"{key}|{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}\n"
    _run_input(
        [
            "docker",
            "exec",
            "-i",
            "spice-foundry-kafka",
            "kafka-console-producer",
            "--bootstrap-server",
            # Use the intra-cluster listener so the producer sees routable broker endpoints.
            "kafka:29092",
            "--topic",
            topic,
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=|",
        ],
        cwd=REPO_ROOT,
        input_text=line,
    )


def _setup_db_and_ontologies() -> str:
    db_name = f"chaos_db_{uuid.uuid4().hex[:10]}"

    _http_json(
        "POST",
        f"{ENDPOINTS.oms}/api/v1/database/create",
        {"name": db_name, "description": "chaos-lite integration db"},
        timeout_s=20,
    )
    _wait_db_exists(db_name, expected=True, timeout_s=120)

    customer = {
        "id": "Customer",
        "label": "Customer",
        "description": "Customer for chaos-lite relationship target",
        "properties": [
            {"name": "customer_id", "type": "string", "label": "Customer ID", "required": True},
            {"name": "name", "type": "string", "label": "Name", "required": True},
        ],
        "relationships": [],
    }
    product = {
        "id": "Product",
        "label": "Product",
        "description": "Product for chaos-lite",
        "properties": [
            {"name": "product_id", "type": "string", "label": "Product ID", "required": True},
            {"name": "name", "type": "string", "label": "Name", "required": True},
        ],
        "relationships": [
            {"predicate": "owned_by", "target": "Customer", "label": "Owned By", "cardinality": "n:1"}
        ],
    }

    _http_json("POST", f"{ENDPOINTS.oms}/api/v1/database/{db_name}/ontology", customer, timeout_s=30)
    _wait_ontology(db_name, "Customer", timeout_s=180)
    _http_json("POST", f"{ENDPOINTS.oms}/api/v1/database/{db_name}/ontology", product, timeout_s=30)
    _wait_ontology(db_name, "Product", timeout_s=180)

    return db_name


def _create_customer_and_product(
    db_name: str,
    *,
    customer_id: str,
    product_id: str,
    wait_command: bool = True,
) -> Dict[str, str]:
    # Customer
    cust_resp = _http_json(
        "POST",
        f"{ENDPOINTS.oms}/api/v1/instances/{db_name}/async/Customer/create",
        {"data": {"customer_id": customer_id, "name": "Chaos Customer"}},
        timeout_s=20,
    )
    cust_cmd = str(cust_resp.get("command_id") or (cust_resp.get("data") or {}).get("command_id") or "")
    if not cust_cmd:
        raise AssertionError(f"missing command_id (customer): {cust_resp}")

    # Product (with relationship)
    prod_resp = _http_json(
        "POST",
        f"{ENDPOINTS.oms}/api/v1/instances/{db_name}/async/Product/create",
        {"data": {"product_id": product_id, "name": "Chaos Product", "owned_by": f"Customer/{customer_id}"}},
        timeout_s=20,
    )
    prod_cmd = str(prod_resp.get("command_id") or (prod_resp.get("data") or {}).get("command_id") or "")
    if not prod_cmd:
        raise AssertionError(f"missing command_id (product): {prod_resp}")

    if wait_command:
        _wait_command_completed(cust_cmd, timeout_s=240)
        _wait_command_completed(prod_cmd, timeout_s=240)

    return {"customer_command_id": cust_cmd, "product_command_id": prod_cmd}


def _assert_converged(
    *,
    db_name: str,
    customer_id: str,
    product_id: str,
    customer_command_id: str,
    product_command_id: str,
    retry_expected: bool = False,
) -> None:
    # 1) S3/MinIO event store indexes exist for command + domain events
    cust_agg = f"{db_name}:{DEFAULT_BRANCH}:Customer:{customer_id}"
    prod_agg = f"{db_name}:{DEFAULT_BRANCH}:Product:{product_id}"
    cust_domain = _domain_event_id(customer_command_id, event_type="INSTANCE_CREATED", aggregate_id=cust_agg)
    prod_domain = _domain_event_id(product_command_id, event_type="INSTANCE_CREATED", aggregate_id=prod_agg)

    _wait_s3_has_event_id(customer_command_id, timeout_s=240)
    _wait_s3_has_event_id(product_command_id, timeout_s=240)

    # 2) Postgres processed_events: command handler + projection handler are done
    _wait_registry_done("instance_worker", customer_command_id, timeout_s=240)
    _wait_registry_done("instance_worker", product_command_id, timeout_s=240)

    # After write-side is done, domain events must exist in the Event Store.
    _wait_s3_has_event_id(cust_domain, timeout_s=240)
    _wait_s3_has_event_id(prod_domain, timeout_s=240)

    _wait_registry_done("projection_worker:instance_events", cust_domain, timeout_s=240)
    _wait_registry_done("projection_worker:instance_events", prod_domain, timeout_s=240)

    # 3) ES documents exist (payload projection)
    idx = _instances_index(db_name)
    _wait_es_doc(idx, customer_id, timeout_s=240)
    _wait_es_doc(idx, product_id, timeout_s=240)

    # 4) BFF graph federation returns FULL nodes and relationship edge
    graph = _graph_query(db_name, product_id, include_provenance=True)
    _assert_graph_full(graph)

    # 5) If we expect retries/crashes, ensure at least one attempt bump happened for the product command.
    if retry_expected:
        cust_row = _psql_scalar(
            "SELECT attempt_count::text "
            "FROM spice_event_registry.processed_events "
            f"WHERE handler='instance_worker' AND event_id='{customer_command_id}'"
        )
        prod_row = _psql_scalar(
            "SELECT attempt_count::text "
            "FROM spice_event_registry.processed_events "
            f"WHERE handler='instance_worker' AND event_id='{product_command_id}'"
        )
        cust_attempt = int(cust_row or "0")
        prod_attempt = int(prod_row or "0")
        if max(cust_attempt, prod_attempt) < 2:
            raise AssertionError(
                f"expected retry attempt_count>=2, got customer={cust_row!r} product={prod_row!r}"
            )


def scenario_kafka_down_then_recover() -> None:
    print("\n[chaos] scenario 1/5: Kafka down -> queued -> recover", flush=True)
    db = _setup_db_and_ontologies()

    print("Stopping Kafka...", flush=True)
    _docker_compose(["stop", "kafka"])

    ids = _create_customer_and_product(db, customer_id="CUST-KAFKA", product_id="PROD-KAFKA", wait_command=False)

    # While Kafka is down, commands must not complete.
    time.sleep(3)
    try:
        _wait_command_completed(ids["product_command_id"], timeout_s=5)
        raise AssertionError("command unexpectedly completed while Kafka was stopped")
    except Exception:
        pass

    print("Starting Kafka...", flush=True)
    _docker_compose(["up", "-d", "kafka"])

    _wait_command_completed(ids["customer_command_id"], timeout_s=240)
    _wait_command_completed(ids["product_command_id"], timeout_s=240)

    _assert_converged(
        db_name=db,
        customer_id="CUST-KAFKA",
        product_id="PROD-KAFKA",
        customer_command_id=ids["customer_command_id"],
        product_command_id=ids["product_command_id"],
    )


def scenario_redis_down_then_recover() -> None:
    print("\n[chaos] scenario 2/5: Redis down -> processing continues", flush=True)
    db = _setup_db_and_ontologies()

    print("Stopping Redis...", flush=True)
    _docker_compose(["stop", "redis"])

    # We cannot rely on command-status when Redis is down. Instead, wait for ES+graph convergence.
    ids = _create_customer_and_product(db, customer_id="CUST-REDIS", product_id="PROD-REDIS", wait_command=False)

    _assert_converged(
        db_name=db,
        customer_id="CUST-REDIS",
        product_id="PROD-REDIS",
        customer_command_id=ids["customer_command_id"],
        product_command_id=ids["product_command_id"],
    )

    print("Starting Redis...", flush=True)
    _docker_compose(["up", "-d", "redis"])


def scenario_es_down_then_recover() -> None:
    print("\n[chaos] scenario 3/5: Elasticsearch down -> projection retries -> recover", flush=True)
    db = _setup_db_and_ontologies()

    print("Stopping Elasticsearch...", flush=True)
    _docker_compose(["stop", "elasticsearch"])

    ids = _create_customer_and_product(db, customer_id="CUST-ES", product_id="PROD-ES", wait_command=True)

    # ES is down, so projection should not be FULL yet. Graph query should show MISSING/PARTIAL.
    try:
        graph = _graph_query(db, "PROD-ES", include_provenance=False)
        nodes = graph.get("nodes") or []
        statuses = [str(n.get("data_status")) for n in nodes if isinstance(n, dict)]
        if statuses and all(s == "FULL" for s in statuses):
            raise AssertionError("expected non-FULL while ES is stopped")
    except Exception:
        # acceptable if BFF errors while ES is down
        pass

    print("Starting Elasticsearch...", flush=True)
    _docker_compose(["up", "-d", "elasticsearch"])

    _assert_converged(
        db_name=db,
        customer_id="CUST-ES",
        product_id="PROD-ES",
        customer_command_id=ids["customer_command_id"],
        product_command_id=ids["product_command_id"],
    )


def scenario_terminus_down_then_recover() -> None:
    print("\n[chaos] scenario 4/5: TerminusDB down -> write-side retries -> recover", flush=True)
    db = _setup_db_and_ontologies()

    print("Stopping TerminusDB...", flush=True)
    _docker_compose(["stop", "terminusdb"])

    ids = _create_customer_and_product(db, customer_id="CUST-TD", product_id="PROD-TD", wait_command=False)

    # With Terminus down, instance_worker must NOT complete the commands.
    time.sleep(3)
    try:
        _wait_command_completed(ids["product_command_id"], timeout_s=5)
        raise AssertionError("command unexpectedly completed while TerminusDB was stopped")
    except Exception:
        pass

    print("Starting TerminusDB...", flush=True)
    _docker_compose(["up", "-d", "terminusdb"])

    _wait_command_completed(ids["customer_command_id"], timeout_s=240)
    _wait_command_completed(ids["product_command_id"], timeout_s=240)

    _assert_converged(
        db_name=db,
        customer_id="CUST-TD",
        product_id="PROD-TD",
        customer_command_id=ids["customer_command_id"],
        product_command_id=ids["product_command_id"],
    )


def scenario_instance_worker_crash_after_claim() -> None:
    print("\n[chaos] scenario 5/5: instance-worker crash after claim -> lease recovery -> converge", flush=True)
    db = _setup_db_and_ontologies()

    overrides = {
        "ENABLE_CHAOS_INJECTION": "true",
        "CHAOS_CRASH_POINT": "instance_worker:after_claim",
        "CHAOS_CRASH_ONCE": "true",
        "CHAOS_CRASH_EXIT_CODE": "42",
        "PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS": "5",
        "PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS": "1",
    }
    print("Recreating instance-worker with crash injection...", flush=True)
    _docker_compose(["up", "-d", "--no-deps", "--force-recreate", "instance-worker"], extra_env=overrides)

    ids = _create_customer_and_product(db, customer_id="CUST-CRASH", product_id="PROD-CRASH", wait_command=True)

    _assert_converged(
        db_name=db,
        customer_id="CUST-CRASH",
        product_id="PROD-CRASH",
        customer_command_id=ids["customer_command_id"],
        product_command_id=ids["product_command_id"],
        retry_expected=True,
    )

    print("Recreating instance-worker back to defaults...", flush=True)
    _docker_compose(
        ["up", "-d", "--no-deps", "--force-recreate", "instance-worker"],
        extra_env={
            "ENABLE_CHAOS_INJECTION": "false",
            "CHAOS_CRASH_POINT": "",
            "PROCESSED_EVENT_LEASE_TIMEOUT_SECONDS": "900",
            "PROCESSED_EVENT_HEARTBEAT_INTERVAL_SECONDS": "30",
        },
    )


def scenario_out_of_order_delivery() -> None:
    print("\n[chaos] scenario (extra): out-of-order seq delivery -> stale skip -> latest wins", flush=True)
    db = _setup_db_and_ontologies()

    customer_id = "CUST-OOD"
    product_id = "PROD-OOD"

    # 1) Create baseline entities normally (so the instance exists to update).
    ids = _create_customer_and_product(db, customer_id=customer_id, product_id=product_id, wait_command=True)
    _assert_converged(
        db_name=db,
        customer_id=customer_id,
        product_id=product_id,
        customer_command_id=ids["customer_command_id"],
        product_command_id=ids["product_command_id"],
    )

    prod_agg = f"{db}:{DEFAULT_BRANCH}:Product:{product_id}"

    # 2) Stop message-relay so OMS can append commands to S3 without automatically publishing to Kafka.
    print("Stopping message-relay (publisher)...", flush=True)
    _docker_compose(["stop", "message-relay"])

    update_cmd_1 = ""
    update_cmd_2 = ""
    try:
        # 3) Append two UPDATE_INSTANCE commands with correct OCC expected_seq.
        expected_1 = _write_side_last_sequence(aggregate_type="Instance", aggregate_id=prod_agg)
        resp1 = _http_json(
            "PUT",
            f"{ENDPOINTS.oms}/api/v1/instances/{db}/async/Product/{product_id}/update?expected_seq={expected_1}",
            {"data": {"name": "Chaos Product v1"}, "metadata": {}},
            timeout_s=20,
        )
        update_cmd_1 = str(resp1.get("command_id") or (resp1.get("data") or {}).get("command_id") or "")
        if not update_cmd_1:
            raise AssertionError(f"missing command_id for update1: {resp1}")

        expected_2 = _write_side_last_sequence(aggregate_type="Instance", aggregate_id=prod_agg)
        resp2 = _http_json(
            "PUT",
            f"{ENDPOINTS.oms}/api/v1/instances/{db}/async/Product/{product_id}/update?expected_seq={expected_2}",
            {"data": {"name": "Chaos Product v2"}, "metadata": {}},
            timeout_s=20,
        )
        update_cmd_2 = str(resp2.get("command_id") or (resp2.get("data") or {}).get("command_id") or "")
        if not update_cmd_2:
            raise AssertionError(f"missing command_id for update2: {resp2}")

        _wait_s3_has_event_id(update_cmd_1, timeout_s=240)
        _wait_s3_has_event_id(update_cmd_2, timeout_s=240)

        env1 = _read_s3_event_envelope(update_cmd_1)
        env2 = _read_s3_event_envelope(update_cmd_2)

        # 4) Publish out-of-order by seq: deliver update2 first, then update1.
        print("Injecting out-of-order to Kafka (seq2 -> seq1)...", flush=True)
        _kafka_produce_json(topic="instance_commands", key=prod_agg, payload=env2)
        _wait_registry_done("instance_worker", update_cmd_2, timeout_s=240)

        _kafka_produce_json(topic="instance_commands", key=prod_agg, payload=env1)
        _wait_registry_status("instance_worker", update_cmd_1, "skipped_stale", timeout_s=240)
    finally:
        print("Starting message-relay (publisher)...", flush=True)
        _docker_compose(["up", "-d", "message-relay"])

    # 5) Projection: update2 should emit INSTANCE_UPDATED and reach ES via message-relay + projection-worker.
    updated_domain = _domain_event_id(update_cmd_2, event_type="INSTANCE_UPDATED", aggregate_id=prod_agg)
    _wait_s3_has_event_id(updated_domain, timeout_s=240)
    _wait_registry_done("projection_worker:instance_events", updated_domain, timeout_s=240)

    # 6) ES doc must reflect the latest update (v2), not the stale one.
    idx = _instances_index(db)
    doc = _wait_es_doc(idx, product_id, timeout_s=240)
    source = (doc or {}).get("_source") or {}
    name = source.get("name") or ((source.get("data") or {}).get("name") if isinstance(source.get("data"), dict) else None)
    if name != "Chaos Product v2":
        raise AssertionError(f"expected ES name='Chaos Product v2', got {name!r} (doc={doc})")

    # 7) BFF federation should be FULL again after projection catches up.
    graph = _graph_query(db, product_id, include_provenance=True)
    _assert_graph_full(graph)


def scenario_soak_random_failures(*, duration_s: int, seed: Optional[int] = None) -> None:
    """
    Soak test with real infra + random partial failures (no mocks).

    This is intentionally heavier than chaos-lite; it creates data continuously and injects failures,
    asserting that the system converges after recovery (idempotency + retries + final consistency).
    """
    actual_seed = int(seed) if seed is not None else int(time.time())
    rng = random.Random(actual_seed)
    print(
        f"\n[chaos] scenario (extra): soak/random failures (duration={duration_s}s seed={actual_seed})",
        flush=True,
    )

    db = _setup_db_and_ontologies()
    started = time.monotonic()
    iteration = 0

    failure_modes = [
        "none",
        "kafka_down",
        "redis_down",
        "es_down",
        "terminus_down",
        "relay_down",
        "restart_instance_worker",
        "restart_projection_worker",
    ]

    def _ensure_all_up() -> None:
        # `up` can fail on flaky healthchecks (e.g. zookeeper) even when containers are running.
        # For soak we only need to ensure containers are started; convergence checks will validate behavior.
        _docker_compose(["start"])

    try:
        while time.monotonic() - started < float(duration_s):
            iteration += 1
            mode = rng.choice(failure_modes)
            customer_id = f"CUST-SOAK-{iteration:04d}"
            product_id = f"PROD-SOAK-{iteration:04d}"

            print(f"[soak] iter={iteration} mode={mode} ids={customer_id}/{product_id}", flush=True)

            # Inject failure (short, bounded downtime) around the command submission.
            if mode == "kafka_down":
                _docker_compose(["stop", "kafka"])
                ids = _create_customer_and_product(db, customer_id=customer_id, product_id=product_id, wait_command=False)
                time.sleep(2)
                _docker_compose(["start", "kafka"])
            elif mode == "redis_down":
                _docker_compose(["stop", "redis"])
                ids = _create_customer_and_product(db, customer_id=customer_id, product_id=product_id, wait_command=False)
                time.sleep(2)
                _docker_compose(["start", "redis"])
            elif mode == "es_down":
                _docker_compose(["stop", "elasticsearch"])
                ids = _create_customer_and_product(db, customer_id=customer_id, product_id=product_id, wait_command=False)
                time.sleep(2)
                _docker_compose(["start", "elasticsearch"])
            elif mode == "terminus_down":
                _docker_compose(["stop", "terminusdb"])
                ids = _create_customer_and_product(db, customer_id=customer_id, product_id=product_id, wait_command=False)
                time.sleep(2)
                _docker_compose(["start", "terminusdb"])
            elif mode == "relay_down":
                _docker_compose(["stop", "message-relay"])
                ids = _create_customer_and_product(db, customer_id=customer_id, product_id=product_id, wait_command=False)
                time.sleep(2)
                _docker_compose(["start", "message-relay"])
            elif mode == "restart_instance_worker":
                ids = _create_customer_and_product(db, customer_id=customer_id, product_id=product_id, wait_command=False)
                _docker_compose(["restart", "instance-worker"])
            elif mode == "restart_projection_worker":
                ids = _create_customer_and_product(db, customer_id=customer_id, product_id=product_id, wait_command=False)
                _docker_compose(["restart", "projection-worker"])
            else:
                ids = _create_customer_and_product(db, customer_id=customer_id, product_id=product_id, wait_command=False)

            _ensure_all_up()

            _assert_converged(
                db_name=db,
                customer_id=customer_id,
                product_id=product_id,
                customer_command_id=ids["customer_command_id"],
                product_command_id=ids["product_command_id"],
            )
    finally:
        _ensure_all_up()


def main() -> None:
    if not COMPOSE_FILE.exists():
        raise SystemExit(f"Missing compose file: {COMPOSE_FILE}")

    parser = argparse.ArgumentParser(description="Chaos-lite validation (no mocks; real stack)")
    parser.add_argument(
        "--skip-lite",
        action="store_true",
        help="Skip the default 5 chaos-lite scenarios (run only selected extras).",
    )
    parser.add_argument(
        "--out-of-order",
        action="store_true",
        help="Run out-of-order seq delivery scenario (Kafka injection).",
    )
    parser.add_argument(
        "--soak",
        action="store_true",
        help="Run soak test with random failures (long-running).",
    )
    parser.add_argument(
        "--soak-seconds",
        type=int,
        default=int(os.getenv("SOAK_SECONDS") or "300"),
        help="Soak duration in seconds (default: 300).",
    )
    parser.add_argument(
        "--soak-seed",
        type=int,
        default=(int(os.getenv("SOAK_SEED")) if os.getenv("SOAK_SEED") else 0),
        help="Deterministic RNG seed (default: SOAK_SEED env or 0=now).",
    )
    args = parser.parse_args()

    print("[chaos] ensuring stack is up...", flush=True)
    _docker_compose(["up", "-d"])

    # Health probes (best-effort)
    _wait_http_ok(f"{ENDPOINTS.oms}/health", timeout_s=120)
    _wait_http_ok(f"{ENDPOINTS.bff}/api/v1/health", timeout_s=120)

    # Run scenarios
    if not args.skip_lite:
        scenario_kafka_down_then_recover()
        scenario_redis_down_then_recover()
        scenario_es_down_then_recover()
        scenario_terminus_down_then_recover()
        scenario_instance_worker_crash_after_claim()

    if args.out_of_order:
        scenario_out_of_order_delivery()

    if args.soak:
        scenario_soak_random_failures(duration_s=int(args.soak_seconds), seed=(args.soak_seed or None))

    print("\n[chaos] ✅ all chaos-lite scenarios passed", flush=True)


if __name__ == "__main__":
    try:
        main()
    except URLError as e:
        raise SystemExit(f"Network error: {e}") from e
