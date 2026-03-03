#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPEC_PATH="${REPO_ROOT}/docs-portal/static/generated/bff-openapi.json"
SDK_ROOT="${REPO_ROOT}/sdk"
PY_OUT="${SDK_ROOT}/python"
TS_OUT="${SDK_ROOT}/typescript"
GEN_IMAGE="${OPENAPI_GENERATOR_IMAGE:-openapitools/openapi-generator-cli:v7.8.0}"

if [[ ! -f "${SPEC_PATH}" ]]; then
  echo "❌ OpenAPI spec not found: ${SPEC_PATH}" >&2
  exit 1
fi

rm -rf "${PY_OUT}" "${TS_OUT}"
mkdir -p "${PY_OUT}" "${TS_OUT}"

docker run --rm \
  -u "$(id -u):$(id -g)" \
  -v "${REPO_ROOT}:/work" \
  "${GEN_IMAGE}" generate \
  -i /work/docs-portal/static/generated/bff-openapi.json \
  -g python \
  -o /work/sdk/python \
  --additional-properties=packageName=spice_harvester_sdk,projectName=spice-harvester-sdk

docker run --rm \
  -u "$(id -u):$(id -g)" \
  -v "${REPO_ROOT}:/work" \
  "${GEN_IMAGE}" generate \
  -i /work/docs-portal/static/generated/bff-openapi.json \
  -g typescript-fetch \
  -o /work/sdk/typescript \
  --additional-properties=npmName=@spice-harvester/sdk,npmVersion=1.0.0,supportsES6=true

echo "✅ Generated SDKs:"
echo " - ${PY_OUT}"
echo " - ${TS_OUT}"
