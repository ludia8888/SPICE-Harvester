#!/usr/bin/env bash

# SPICE System Deployment Script
# Builds and runs the complete local stack via Docker Compose.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🚀 Starting SPICE System Deployment..."

if ! command -v docker >/dev/null 2>&1; then
    echo "❌ Docker is not installed. Please install Docker first." >&2
    exit 1
fi

COMPOSE_CMD=()
if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD=(docker-compose)
elif docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD=(docker compose)
else
    echo "❌ Docker Compose is not available (expected 'docker-compose' or 'docker compose')." >&2
    exit 1
fi

# Parse command line arguments
ACTION="${1:-up}"
ENVIRONMENT="${2:-production}"

# Load repo-root `.env` (port overrides, etc) but don't override explicit env vars.
load_dotenv_defaults() {
    local dotenv_path="$1"
    [[ -f "$dotenv_path" ]] || return 0

    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line#"${line%%[![:space:]]*}"}" # ltrim
        line="${line%"${line##*[![:space:]]}"}" # rtrim

        [[ -z "$line" ]] && continue
        [[ "$line" == \#* ]] && continue

        line="${line#export }"

        if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
            local key="${BASH_REMATCH[1]}"
            local value="${BASH_REMATCH[2]}"

            if [[ "$value" =~ ^\"(.*)\"$ ]]; then
                value="${BASH_REMATCH[1]}"
            elif [[ "$value" =~ ^\'(.*)\'$ ]]; then
                value="${BASH_REMATCH[1]}"
            fi

            if [[ -z "${!key:-}" ]]; then
                export "$key=$value"
            fi
        fi
    done <"$dotenv_path"
}

load_dotenv_defaults "$REPO_ROOT/.env"

# Compose-aligned defaults (local host ports may be overridden by `.env`)
POSTGRES_PORT_HOST="${POSTGRES_PORT_HOST:-5433}"
REDIS_PORT_HOST="${REDIS_PORT_HOST:-6379}"
ELASTICSEARCH_PORT_HOST="${ELASTICSEARCH_PORT_HOST:-9200}"
MINIO_PORT_HOST="${MINIO_PORT_HOST:-9000}"
MINIO_CONSOLE_PORT_HOST="${MINIO_CONSOLE_PORT_HOST:-9001}"
KAFKA_PORT_HOST="${KAFKA_PORT_HOST:-9092}"

OMS_URL="${OMS_BASE_URL:-http://localhost:8000}"
BFF_URL="${BFF_BASE_URL:-http://localhost:8002}"
FUNNEL_URL="${FUNNEL_BASE_URL:-http://localhost:8003}"
MINIO_URL="${MINIO_ENDPOINT_URL:-http://localhost:${MINIO_PORT_HOST}}"

# Compose file selection:
# - Default: docker-compose.yml
# - If ENVIRONMENT matches a file path, use it.
# - If ENVIRONMENT is "https", use docker-compose-https.yml.
COMPOSE_FILE="docker-compose.yml"
# Prefer the repo-root full stack compose file if present (matches docs/README quickstart).
if [[ -f "../docker-compose.full.yml" ]]; then
    COMPOSE_FILE="../docker-compose.full.yml"
fi
if [[ -n "${ENVIRONMENT:-}" ]]; then
    if [[ -f "$ENVIRONMENT" ]]; then
        COMPOSE_FILE="$ENVIRONMENT"
    elif [[ "$ENVIRONMENT" == "https" && -f "docker-compose-https.yml" ]]; then
        COMPOSE_FILE="docker-compose-https.yml"
    fi
fi

compose() {
    "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" "$@"
}

wait_for_url() {
    local name="$1"
    local url="$2"
    local timeout_s="${3:-60}"
    local deadline
    deadline="$(( $(date +%s) + timeout_s ))"
    while [[ "$(date +%s)" -lt "$deadline" ]]; do
        if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
            echo "✅ $name healthy: $url"
            return 0
        fi
        sleep 1
    done
    echo "❌ Timed out waiting for $name: $url (timeout=${timeout_s}s)" >&2
    return 1
}

# Function to build images
build_images() {
    echo "🔨 Building Docker images..."
    compose build --no-cache
}

# Function to start services
start_services() {
    echo "🚀 Starting services..."
    compose up -d --remove-orphans

    echo "⏳ Waiting for services to be healthy..."
    # Prefer explicit HTTP health checks (more reliable than parsing compose output).
    wait_for_url "MinIO" "${MINIO_URL%/}/minio/health/live" 90
    wait_for_url "OMS" "${OMS_URL%/}/health" 120
    wait_for_url "BFF" "${BFF_URL%/}/api/v1/health" 120
    wait_for_url "Funnel" "${FUNNEL_URL%/}/health" 120
    wait_for_url "Elasticsearch" "http://localhost:${ELASTICSEARCH_PORT_HOST}/_cluster/health" 120 || true
    
    echo "✅ All services are running!"
    echo ""
    echo "📍 Service URLs:"
    echo "   - TerminusDB: http://localhost:6363"
    echo "   - OMS API: ${OMS_URL%/}"
    echo "   - BFF API: ${BFF_URL%/}"
    echo "   - Funnel API: ${FUNNEL_URL%/}"
    echo "   - MinIO: ${MINIO_URL%/}"
    echo "   - MinIO Console: http://localhost:${MINIO_CONSOLE_PORT_HOST}"
    echo ""
    echo "📊 View logs: ${COMPOSE_CMD[*]} -f $COMPOSE_FILE logs -f"
}

# Function to stop services
stop_services() {
    echo "🛑 Stopping services..."
    compose down
}

# Function to clean up
cleanup() {
    echo "🧹 Cleaning up..."
    compose down -v --remove-orphans
    docker system prune -f
}

# Function to show logs
show_logs() {
    compose logs -f
}

# Function to run tests
run_tests() {
    echo "🧪 Running production tests..."
    
    # Wait for services to be ready
    sleep 15
    
    # Run test script
    if [ -f "./run_production_tests.sh" ]; then
        ./run_production_tests.sh --full
    else
        echo "⚠️  Test script not found. Skipping tests."
    fi
}

# Main deployment logic
case $ACTION in
    "build")
        build_images
        ;;
    "up")
        build_images
        start_services
        ;;
    "start")
        start_services
        ;;
    "stop")
        stop_services
        ;;
    "restart")
        stop_services
        start_services
        ;;
    "clean")
        cleanup
        ;;
    "logs")
        show_logs
        ;;
    "test")
        start_services
        run_tests
        ;;
    "deploy")
        # Full deployment with tests
        build_images
        start_services
        run_tests
        echo "✅ Deployment complete!"
        ;;
    *)
        echo "Usage: $0 [build|up|start|stop|restart|clean|logs|test|deploy] [environment|compose-file]"
        echo ""
        echo "Commands:"
        echo "  build   - Build Docker images"
        echo "  up      - Build and start all services (default)"
        echo "  start   - Start services without building"
        echo "  stop    - Stop all services"
        echo "  restart - Restart all services"
        echo "  clean   - Stop services and remove volumes"
        echo "  logs    - Show service logs"
        echo "  test    - Run production tests"
        echo "  deploy  - Full deployment with tests"
        echo ""
        echo "Second argument:"
        echo "  production (default) - uses docker-compose.yml"
        echo "  https               - uses docker-compose-https.yml (if present)"
        echo "  <file>              - if it exists, treated as a compose file path"
        exit 1
        ;;
esac
