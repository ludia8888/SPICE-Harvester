from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_STRICT_RUNTIME_GLOBS = (
    "action_worker/main.py",
    "pipeline_worker/main.py",
    "instance_worker/main.py",
    "objectify_worker/main.py",
    "ontology_worker/main.py",
    "projection_worker/main.py",
    "message_relay/main.py",
    "connector_trigger_service/main.py",
    "action_outbox_worker/main.py",
    "writeback_materializer_worker/main.py",
    "shared/services/kafka/processed_event_worker.py",
    "shared/services/kafka/dlq_publisher.py",
    "shared/services/pipeline/pipeline_executor.py",
    "shared/config/settings.py",
    "shared/errors/runtime_exception_policy.py",
    "shared/utils/json_utils.py",
    "shared/utils/worker_runner.py",
    "shared/observability/context_propagation.py",
    "shared/observability/logging.py",
    "bff/middleware/auth.py",
    "oms/services/async_terminus.py",
    "oms/services/terminus/base.py",
    "oms/services/terminus/document.py",
    "oms/services/terminus/ontology.py",
    "oms/services/terminus/query.py",
    "mcp_servers/pipeline_mcp_errors.py",
    "mcp_servers/ontology_mcp_server.py",
    "mcp_servers/pipeline_tools/dataset_tools.py",
    "mcp_servers/pipeline_tools/pipeline_tools.py",
    "mcp_servers/pipeline_tools/plan_tools.py",
)


@pytest.mark.unit
def test_error_taxonomy_audit_guard() -> None:
    backend_dir = Path(__file__).resolve().parents[3]
    script_path = backend_dir / "shared" / "tools" / "error_taxonomy_audit.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--backend-root",
        str(backend_dir),
        "--fail-on-raw-http-without-code",
        "--fail-on-raw-code",
        "--fail-on-bare-except",
        "--fail-on-suppress-exception",
        "--fail-on-silent-broad-except",
        "--fail-on-lineage-fail-open",
        "--fail-on-commented-export",
        "--fail-on-doc-only-module",
    ]
    for pattern in _STRICT_RUNTIME_GLOBS:
        cmd.extend(["--runtime-scope-glob", pattern])
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        return

    output = (result.stdout or "") + ("\n" if result.stdout and result.stderr else "") + (result.stderr or "")
    raise AssertionError(f"Error taxonomy audit failed:\n{output}")
