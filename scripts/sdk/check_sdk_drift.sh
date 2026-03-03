#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

"${REPO_ROOT}/scripts/sdk/generate_bff_sdks.sh"

if ! git -C "${REPO_ROOT}" diff --quiet -- sdk/python sdk/typescript; then
  echo "❌ SDK drift detected under sdk/python or sdk/typescript" >&2
  echo "Run: scripts/sdk/generate_bff_sdks.sh and commit regenerated SDK files." >&2
  exit 1
fi

echo "✅ SDK drift check passed."
