#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# BFF SDK MCP Connector - Claude.ai 웹 채팅용 원격 MCP 서버
#
# 사용법:
#   ./scripts/mcp/start_bff_connector.sh          # SSE + ngrok (기본)
#   ./scripts/mcp/start_bff_connector.sh --local   # SSE만 (ngrok 없이)
#   ./scripts/mcp/start_bff_connector.sh --port 8888
#
# Claude.ai 연결:
#   1. 이 스크립트 실행
#   2. 출력되는 ngrok URL 복사
#   3. Claude.ai → Settings → Integrations → Add MCP → URL 붙여넣기
# ─────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV="${REPO_ROOT}/.venv-tests-311"
SERVER="${REPO_ROOT}/backend/mcp_servers/bff_sdk_mcp_server.py"
PORT="${PORT:-9090}"
LOCAL_ONLY=false

# Parse args
while [[ $# -gt 0 ]]; do
  case $1 in
    --local) LOCAL_ONLY=true; shift ;;
    --port)  PORT="$2"; shift 2 ;;
    --port=*) PORT="${1#*=}"; shift ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

# Check prerequisites
if [[ ! -f "${VENV}/bin/python" ]]; then
  echo "Python venv not found: ${VENV}"
  exit 1
fi

if ! $LOCAL_ONLY && ! command -v ngrok &>/dev/null; then
  echo "ngrok not found. Install: brew install ngrok"
  echo "Or run with --local flag."
  exit 1
fi

# Kill stale processes on the port
lsof -ti :"${PORT}" 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1

echo "================================================"
echo "  BFF SDK MCP Connector"
echo "  Port: ${PORT}"
echo "  BFF:  ${BFF_BASE_URL:-http://localhost:8002}"
echo "================================================"
echo ""

# Start MCP SSE server
export PYTHONPATH="${REPO_ROOT}/backend"
export BFF_BASE_URL="${BFF_BASE_URL:-http://localhost:8002}"
export ADMIN_TOKEN="${ADMIN_TOKEN:-change_me}"

"${VENV}/bin/python" "${SERVER}" --transport sse --port "${PORT}" &
MCP_PID=$!

cleanup() {
  echo ""
  echo "Shutting down..."
  kill $MCP_PID 2>/dev/null || true
  if [[ -n "${NGROK_PID:-}" ]]; then
    kill $NGROK_PID 2>/dev/null || true
  fi
  exit 0
}
trap cleanup SIGINT SIGTERM

sleep 2

# Health check
if ! curl -sf "http://localhost:${PORT}/health" >/dev/null 2>&1; then
  echo "MCP server failed to start!"
  kill $MCP_PID 2>/dev/null
  exit 1
fi

echo "MCP server running on http://localhost:${PORT}"
echo "  Health: http://localhost:${PORT}/health"
echo "  SSE:    http://localhost:${PORT}/sse"
echo ""

if $LOCAL_ONLY; then
  echo "Local mode - no ngrok tunnel."
  echo "Press Ctrl+C to stop."
  wait $MCP_PID
else
  # Start ngrok tunnel
  ngrok http "${PORT}" --log=stdout &
  NGROK_PID=$!
  sleep 3

  # Get public URL from ngrok API
  NGROK_URL=$(curl -s http://localhost:4040/api/tunnels 2>/dev/null \
    | python3 -c "import sys,json; t=json.load(sys.stdin)['tunnels']; print(t[0]['public_url'])" 2>/dev/null || echo "")

  if [[ -z "${NGROK_URL}" ]]; then
    echo "ngrok tunnel URL not found. Check ngrok dashboard: http://localhost:4040"
  else
    echo "================================================"
    echo ""
    echo "  Claude.ai MCP Connector URL:"
    echo ""
    echo "    ${NGROK_URL}/sse"
    echo ""
    echo "================================================"
    echo ""
    echo "Claude.ai에서 연결하기:"
    echo "  1. claude.ai → Settings → Integrations"
    echo "  2. 'Add Integration' 클릭"
    echo "  3. 위 URL 입력"
    echo ""
  fi

  echo "Press Ctrl+C to stop."
  wait $MCP_PID
fi
