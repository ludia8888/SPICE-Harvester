#!/bin/bash
# 🔥 THINK ULTRA! 테스트 서비스 상태 확인 스크립트
# Enhanced service status checking with detailed diagnostics

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# PID directory and files
PID_DIR="$SCRIPT_DIR/.pids"
LOG_DIR="$SCRIPT_DIR/logs"
OMS_PID_FILE="$PID_DIR/oms.pid"
BFF_PID_FILE="$PID_DIR/bff.pid"

# Service configuration
OMS_PORT=8000
BFF_PORT=8002
OMS_HEALTH_URL="http://localhost:$OMS_PORT/health"
BFF_HEALTH_URL="http://localhost:$BFF_PORT/api/v1/health"

# Logging function with timestamps
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Read PID file function
read_pid_file() {
    local pid_file=$1
    if [[ -f "$pid_file" ]]; then
        cat "$pid_file"
    else
        echo ""
    fi
}

# Check if process is running
is_process_running() {
    local pid=$1
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Check port availability
is_port_listening() {
    local port=$1
    if lsof -i :$port > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Get process info by PID
get_process_info() {
    local pid=$1
    if [[ -n "$pid" ]] && is_process_running "$pid"; then
        local cmd
        cmd=$(ps -o comm= -p "$pid" 2>/dev/null)
        local cpu_mem
        cpu_mem=$(ps -o %cpu,%mem -p "$pid" --no-headers 2>/dev/null | xargs)
        local start_time
        start_time=$(ps -o lstart= -p "$pid" 2>/dev/null)
        echo "CMD: $cmd | CPU/MEM: $cpu_mem | Started: $start_time"
    else
        echo "Process not running"
    fi
}

# Enhanced health endpoint check with detailed diagnostics
check_health_endpoint() {
    local service_name=$1
    local url=$2
    local timeout=${3:-5}
    
    # Perform health check with detailed response capture
    local curl_output
    local http_status
    local curl_exit_code
    
    # Get both HTTP status and response body
    curl_output=$(curl -s --max-time "$timeout" -w "HTTP_STATUS:%{http_code}" "$url" 2>&1)
    curl_exit_code=$?
    
    # Extract HTTP status code
    if [[ "$curl_output" =~ HTTP_STATUS:([0-9]+)$ ]]; then
        http_status="${BASH_REMATCH[1]}"
        response_body="${curl_output%HTTP_STATUS:*}"
    else
        http_status=""
        response_body="$curl_output"
    fi
    
    # Analyze the results
    if [[ $curl_exit_code -eq 0 && "$http_status" == "200" ]]; then
        echo "✅ $service_name health check: OK (HTTP $http_status)"
        
        # Try to extract service info from response
        if echo "$response_body" | grep -q '"status"'; then
            local service_info
            service_info=$(echo "$response_body" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    status = data.get('status', 'N/A')
    service = data.get('service', 'N/A') 
    version = data.get('version', 'N/A')
    print(f'   Status: {status}, Service: {service}, Version: {version}')
except:
    pass
" 2>/dev/null)
            if [[ -n "$service_info" ]]; then
                echo "$service_info"
            fi
        fi
        
    else
        # Health check failed - provide detailed diagnostics
        echo "❌ $service_name health check: FAILED"
        
        # Analyze the failure reason
        case $curl_exit_code in
            0)
                # HTTP request succeeded but wrong status code
                echo "   🌐 HTTP Error: Status $http_status (expected 200)"
                if [[ -n "$response_body" && ${#response_body} -lt 200 ]]; then
                    echo "   📄 Response: $response_body"
                fi
                ;;
            6)
                echo "   🔗 Connection Error: Could not resolve host"
                echo "   💡 Check if service is running and port is correct"
                ;;
            7)
                echo "   🔗 Connection Error: Failed to connect to host"
                echo "   💡 Service may be down or port may be blocked"
                ;;
            28)
                echo "   ⏱️  Timeout Error: No response within ${timeout} seconds"
                echo "   💡 Service may be overloaded or stuck"
                ;;
            52)
                echo "   📡 Server Error: Empty response from server"
                echo "   💡 Service may have crashed during startup"
                ;;
            56)
                echo "   🔗 Network Error: Connection reset by peer"
                echo "   💡 Service may have rejected the connection"
                ;;
            *)
                echo "   🔧 Curl Error: Exit code $curl_exit_code"
                if [[ -n "$response_body" ]]; then
                    echo "   📄 Details: $response_body"
                fi
                ;;
        esac
        
        # Provide actionable suggestions for this context
        echo "   🛠️  Quick fixes:"
        echo "     • Check if service is running: ps aux | grep python"
        local port
        port=$(echo "$url" | sed -n 's/.*:\([0-9]*\).*/\1/p')
        if [[ -n "$port" ]]; then
            echo "     • Check port status: lsof -i :$port"
        fi
        echo "     • Restart services: ./start_test_services.sh"
    fi
}

# Get recent logs for service
show_recent_logs() {
    local service_name=$1
    local log_pattern=$2
    local lines=${3:-5}
    
    if [[ -d "$LOG_DIR" ]]; then
        local latest_log
        latest_log=$(find "$LOG_DIR" -name "$log_pattern" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)
        
        if [[ -n "$latest_log" && -f "$latest_log" ]]; then
            echo "📄 Recent $service_name logs (last $lines lines from $(basename "$latest_log")):"
            tail -n "$lines" "$latest_log" | sed 's/^/   | /'
        else
            echo "📄 No $service_name log files found in $LOG_DIR"
        fi
    else
        echo "📄 Log directory not found: $LOG_DIR"
    fi
}

# Check service detailed status
check_service_status() {
    local service_name=$1
    local pid_file=$2
    local port=$3
    local health_url=$4
    local log_pattern=$5
    
    echo ""
    echo "🔍 $service_name Service Status"
    echo "$(printf '%.40s' "----------------------------------------")"
    
    # Check PID file and process
    local pid
    pid=$(read_pid_file "$pid_file")
    
    if [[ -n "$pid" ]]; then
        echo "📝 PID File: $pid_file (PID: $pid)"
        if is_process_running "$pid"; then
            echo "🟢 Process: RUNNING"
            echo "   $(get_process_info "$pid")"
        else
            echo "🔴 Process: NOT RUNNING (stale PID file)"
        fi
    else
        echo "📝 PID File: NOT FOUND ($pid_file)"
        echo "🔴 Process: NOT RUNNING"
    fi
    
    # Check port
    if is_port_listening "$port"; then
        echo "🟢 Port $port: LISTENING"
        # Show which process is using the port
        local port_info
        port_info=$(lsof -i :$port -t 2>/dev/null | head -1)
        if [[ -n "$port_info" ]]; then
            local port_process
            port_process=$(ps -o comm= -p "$port_info" 2>/dev/null)
            echo "   Process: $port_process (PID: $port_info)"
            # Check if it matches our expected PID
            if [[ -n "$pid" && "$port_info" == "$pid" ]]; then
                echo "   ✅ Port matches expected PID"
            elif [[ -n "$pid" ]]; then
                echo "   ⚠️  Port used by different process than PID file"
            fi
        fi
    else
        echo "🔴 Port $port: NOT LISTENING"
    fi
    
    # Check health endpoint
    echo "🏥 Health Check:"
    check_health_endpoint "$service_name" "$health_url"
    
    # Show recent logs
    echo ""
    show_recent_logs "$service_name" "$log_pattern"
}

# Main execution
log_message "🔥 THINK ULTRA! Checking test services status..."
echo "=============================================="

# Overall system checks
echo "🖥️  System Overview"
echo "$(printf '%.40s' "----------------------------------------")"
echo "📁 PID Directory: $PID_DIR"
echo "📁 Log Directory: $LOG_DIR"
echo "🕐 Current Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "👤 User: $(whoami)"
echo "💻 Working Directory: $(pwd)"

# Check individual services
check_service_status "OMS" "$OMS_PID_FILE" "$OMS_PORT" "$OMS_HEALTH_URL" "oms_*.log"
check_service_status "BFF" "$BFF_PID_FILE" "$BFF_PORT" "$BFF_HEALTH_URL" "bff_*.log"

# Summary and recommendations
echo ""
echo "=============================================="
echo "📊 Summary & Recommendations"
echo "=============================================="

# Count running services
RUNNING_SERVICES=0
EXPECTED_SERVICES=2

# Check OMS
oms_pid=$(read_pid_file "$OMS_PID_FILE")
if [[ -n "$oms_pid" ]] && is_process_running "$oms_pid" && is_port_listening "$OMS_PORT"; then
    ((RUNNING_SERVICES++))
fi

# Check BFF
bff_pid=$(read_pid_file "$BFF_PID_FILE")
if [[ -n "$bff_pid" ]] && is_process_running "$bff_pid" && is_port_listening "$BFF_PORT"; then
    ((RUNNING_SERVICES++))
fi

echo "🎯 Services Running: $RUNNING_SERVICES/$EXPECTED_SERVICES"

if [[ $RUNNING_SERVICES -eq $EXPECTED_SERVICES ]]; then
    echo "✅ All services are operational!"
    echo ""
    echo "📋 Available Actions:"
    echo "  Run tests:"
    echo "    cd $SCRIPT_DIR && python tests/runners/run_complex_types_tests.py"
    echo ""
    echo "  Stop services:"
    echo "    $SCRIPT_DIR/stop_test_services.sh"
    echo ""
    echo "  View live logs:"
    if [[ -d "$LOG_DIR" ]]; then
        local latest_oms_log
        latest_oms_log=$(find "$LOG_DIR" -name "oms_*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)
        local latest_bff_log
        latest_bff_log=$(find "$LOG_DIR" -name "bff_*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)
        
        if [[ -n "$latest_oms_log" ]]; then
            echo "    tail -f $latest_oms_log  # OMS"
        fi
        if [[ -n "$latest_bff_log" ]]; then
            echo "    tail -f $latest_bff_log  # BFF"
        fi
    fi
elif [[ $RUNNING_SERVICES -eq 0 ]]; then
    echo "❌ No services are running"
    echo ""
    echo "🚀 To start services:"
    echo "    $SCRIPT_DIR/start_test_services.sh"
else
    echo "⚠️  Partial service failure detected"
    echo ""
    echo "🔧 Troubleshooting:"
    echo "  1. Stop all services: $SCRIPT_DIR/stop_test_services.sh"
    echo "  2. Start services: $SCRIPT_DIR/start_test_services.sh"
    echo "  3. Check logs in: $LOG_DIR"
    echo ""
    echo "🛠️  Manual cleanup (if needed):"
    if [[ -n "$oms_pid" ]]; then
        echo "    kill $oms_pid  # Stop OMS"
    fi
    if [[ -n "$bff_pid" ]]; then
        echo "    kill $bff_pid  # Stop BFF"
    fi
    echo "    rm -rf $PID_DIR  # Clean PID files"
fi

echo ""
echo "🔄 Service Management:"
echo "  Status: $SCRIPT_DIR/check_test_services.sh"
echo "  Start:  $SCRIPT_DIR/start_test_services.sh"
echo "  Stop:   $SCRIPT_DIR/stop_test_services.sh"
