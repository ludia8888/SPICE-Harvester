#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${REPO_ROOT}/backend/ssl/grpc"
mkdir -p "${OUT_DIR}"

pushd "${OUT_DIR}" >/dev/null

openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -subj "/CN=spice-grpc-ca" -out ca.crt

openssl genrsa -out server.key 2048
openssl req -new -key server.key -subj "/CN=oms" -out server.csr
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -sha256

openssl genrsa -out client.key 2048
openssl req -new -key client.key -subj "/CN=spice-internal-client" -out client.csr
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 3650 -sha256

rm -f server.csr client.csr ca.srl

popd >/dev/null
echo "✅ Generated local gRPC certs in ${OUT_DIR}"
