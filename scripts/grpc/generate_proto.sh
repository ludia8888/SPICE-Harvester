#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROTO_ROOT="${REPO_ROOT}/backend/proto"
OUT_DIR="${REPO_ROOT}/backend/shared/generated/grpc"
PYTHON_BIN="${PYTHON:-python}"

mkdir -p "${OUT_DIR}"

"${PYTHON_BIN}" -m grpc_tools.protoc \
  -I "${PROTO_ROOT}" \
  --python_out="${OUT_DIR}" \
  --grpc_python_out="${OUT_DIR}" \
  "${PROTO_ROOT}/spice/oms/v1/oms_gateway.proto"

find "${OUT_DIR}" -type d -exec touch "{}/__init__.py" \;

"${PYTHON_BIN}" - <<'PY'
from pathlib import Path
import re

root = Path("backend/shared/generated/grpc")
for file_path in root.rglob("*_pb2_grpc.py"):
    content = file_path.read_text(encoding="utf-8")
    content = content.replace(
        "from spice.oms.v1 import oms_gateway_pb2 as spice_dot_oms_dot_v1_dot_oms__gateway__pb2",
        "from . import oms_gateway_pb2 as spice_dot_oms_dot_v1_dot_oms__gateway__pb2",
    )
    file_path.write_text(content, encoding="utf-8")

for file_path in root.rglob("*_pb2.py"):
    content = file_path.read_text(encoding="utf-8")
    content = content.replace("from google.protobuf import runtime_version as _runtime_version\n", "")
    content = re.sub(
        r"_runtime_version\.ValidateProtobufRuntimeVersion\(\n.*?\n\)\n",
        "",
        content,
        flags=re.DOTALL,
    )
    file_path.write_text(content, encoding="utf-8")
PY

echo "Generated gRPC stubs in: ${OUT_DIR}"
