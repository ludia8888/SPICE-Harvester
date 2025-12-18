#!/usr/bin/env bash

# SPICE System Deployment Script
# Builds and runs the complete local stack via Docker Compose.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

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
    wait_for_url "MinIO" "http://localhost:9000/minio/health/live" 90
    wait_for_url "OMS" "http://localhost:8000/health" 120
    wait_for_url "BFF" "http://localhost:8002/api/v1/health" 120
    wait_for_url "Funnel" "http://localhost:8003/health" 120
    wait_for_url "Elasticsearch" "http://localhost:9200/_cluster/health" 120 || true
    
    echo "✅ All services are running!"
    echo ""
    echo "📍 Service URLs:"
    echo "   - TerminusDB: http://localhost:6363"
    echo "   - OMS API: http://localhost:8000"
    echo "   - BFF API: http://localhost:8002"
    echo "   - Funnel API: http://localhost:8003"
    echo "   - MinIO Console: http://localhost:9001"
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
