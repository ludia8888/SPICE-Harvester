#!/bin/bash
# 🔥 THINK ULTRA! 테스트를 위한 서비스 시작 스크립트
# Enhanced with proper PID management and structured logging
#
# Environment Variables for Log Rotation Configuration:
#   LOG_MAINTENANCE_DIR     - Log directory (default: ./logs)
#   LOG_MAX_SIZE_MB         - Max file size before rotation in MB (default: 5)
#   LOG_MAX_FILES           - Max rotated files to keep (default: 5)  
#   LOG_COMPRESS_AFTER_DAYS - Compress files after N days (default: 1)
#   LOG_DELETE_AFTER_DAYS   - Delete files after N days (default: 7)
#   LOG_SERVICE_NAMES       - Space-separated service names (default: "oms bff")
#
# Example usage:
#   LOG_MAX_SIZE_MB=10 LOG_DELETE_AFTER_DAYS=14 ./start_test_services.sh

# Get script directory and source unified PYTHONPATH configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/setup_pythonpath.sh"

# Configure Python environment (PYTHONPATH + directory change)
if ! configure_python_environment; then
    echo "❌ Failed to configure Python environment for test services" >&2
    exit 1
fi

# Create directories for PID files and logs
PID_DIR="$SCRIPT_DIR/.pids"
LOG_DIR="${LOG_MAINTENANCE_DIR:-$SCRIPT_DIR/logs}"
mkdir -p "$PID_DIR" "$LOG_DIR"

# Log rotation configuration (same as maintain_logs.sh for consistency)
LOG_MAX_SIZE_MB="${LOG_MAX_SIZE_MB:-5}"
LOG_MAX_FILES="${LOG_MAX_FILES:-5}"
LOG_COMPRESS_AFTER_DAYS="${LOG_COMPRESS_AFTER_DAYS:-1}"
LOG_DELETE_AFTER_DAYS="${LOG_DELETE_AFTER_DAYS:-7}"
LOG_SERVICE_NAMES="${LOG_SERVICE_NAMES:-oms bff}"

# PID file paths
OMS_PID_FILE="$PID_DIR/oms.pid"
BFF_PID_FILE="$PID_DIR/bff.pid"

# Log file paths with timestamps
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
OMS_LOG_FILE="$LOG_DIR/oms_$TIMESTAMP.log"
BFF_LOG_FILE="$LOG_DIR/bff_$TIMESTAMP.log"

# Logging function with timestamps
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# PID management functions
write_pid_file() {
    local pid=$1
    local pid_file=$2
    echo "$pid" > "$pid_file"
    log_message "PID $pid written to $pid_file"
}

read_pid_file() {
    local pid_file=$1
    if [[ -f "$pid_file" ]]; then
        cat "$pid_file"
    else
        echo ""
    fi
}

is_process_running() {
    local pid=$1
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

cleanup_stale_pid() {
    local pid_file=$1
    local service_name=$2
    local pid
    pid=$(read_pid_file "$pid_file")
    
    if [[ -n "$pid" ]] && ! is_process_running "$pid"; then
        log_message "Cleaning up stale PID file for $service_name (PID: $pid)"
        rm -f "$pid_file"
    fi
}

# Enhanced service checking
check_service() {
    local port=$1
    local name=$2
    local pid_file=$3
    
    # First cleanup any stale PID files
    cleanup_stale_pid "$pid_file" "$name"
    
    # Check if PID file exists and process is running
    local pid
    pid=$(read_pid_file "$pid_file")
    if [[ -n "$pid" ]] && is_process_running "$pid"; then
        log_message "⚠️  $name is already running (PID: $pid, Port: $port)"
        return 0
    fi
    
    # Double-check with port binding
    if lsof -i :$port > /dev/null 2>&1; then
        log_message "⚠️  Port $port is occupied by another process"
        return 0
    fi
    
    return 1
}

# Enhanced health check with detailed error diagnostics
check_service_health() {
    local service_name=$1
    local health_url=$2
    local pid_file=$3
    local log_file=$4
    
    # Perform health check with detailed response capture
    local curl_output
    local http_status
    local curl_exit_code
    
    # Get both HTTP status and response body
    curl_output=$(curl -s --max-time 10 -w "HTTP_STATUS:%{http_code}" "$health_url" 2>&1)
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
        log_message "✅ $service_name is healthy (HTTP $http_status)"
        
        # Try to extract service info from response
        if echo "$response_body" | grep -q '"status"'; then
            local service_info
            service_info=$(echo "$response_body" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    if isinstance(data, dict):
        status = data.get('status', 'N/A')
        version = data.get('version', 'N/A')
        print(f'   Status: {status}, Version: {version}')
    else:
        print('   Service responded with valid JSON')
except:
    pass
" 2>/dev/null)
            if [[ -n "$service_info" ]]; then
                echo "$service_info"
            fi
        fi
        
    else
        # Health check failed - provide detailed diagnostics
        log_message "❌ $service_name failed health check"
        
        # Analyze the failure reason
        case $curl_exit_code in
            0)
                # HTTP request succeeded but wrong status code
                log_message "   🌐 HTTP Error: Status $http_status (expected 200)"
                if [[ -n "$response_body" && ${#response_body} -lt 500 ]]; then
                    log_message "   📄 Response: $response_body"
                fi
                ;;
            6)
                log_message "   🔗 Connection Error: Could not resolve host"
                log_message "   💡 Check if service is running and port is correct"
                ;;
            7)
                log_message "   🔗 Connection Error: Failed to connect to host"
                log_message "   💡 Service may be down or port may be blocked"
                ;;
            28)
                log_message "   ⏱️  Timeout Error: No response within 10 seconds"
                log_message "   💡 Service may be overloaded or stuck"
                ;;
            52)
                log_message "   📡 Server Error: Empty response from server"
                log_message "   💡 Service may have crashed during startup"
                ;;
            56)
                log_message "   🔗 Network Error: Connection reset by peer"
                log_message "   💡 Service may have rejected the connection"
                ;;
            *)
                log_message "   🔧 Curl Error: Exit code $curl_exit_code"
                if [[ -n "$response_body" ]]; then
                    log_message "   📄 Details: $response_body"
                fi
                ;;
        esac
        
        # Check process status for additional context
        local pid
        pid=$(read_pid_file "$pid_file")
        if [[ -n "$pid" ]] && is_process_running "$pid"; then
            log_message "   🔍 Process Status: Running (PID: $pid)"
            
            # Extract port from URL
            local port
            port=$(echo "$health_url" | sed -n 's/.*:\([0-9]*\).*/\1/p')
            
            # Check if port is listening
            if [[ -n "$port" ]] && lsof -i :"$port" > /dev/null 2>&1; then
                log_message "   🔌 Port Status: Listening on port $port"
                log_message "   🧩 Diagnosis: Service is running but health endpoint is failing"
            else
                log_message "   🔌 Port Status: Not listening${port:+ on port $port}"
                log_message "   🧩 Diagnosis: Service process exists but not accepting connections"
            fi
            
            # Show recent logs for context
            log_message "   📋 Recent logs (last 3 lines):"
            if [[ -f "$log_file" ]]; then
                tail -3 "$log_file" | sed 's/^/     | /'
            else
                log_message "     | No log file found: $log_file"
            fi
            
        else
            log_message "   🔍 Process Status: Not running"
            log_message "   🧩 Diagnosis: Service process is not active"
            
            if [[ -f "$log_file" ]]; then
                log_message "   📋 Last few log lines before crash:"
                tail -3 "$log_file" | sed 's/^/     | /'
            fi
        fi
        
        # Provide actionable suggestions
        log_message "   🛠️  Suggested actions:"
        log_message "     1. Check service logs: tail -f $log_file"
        log_message "     2. Restart service manually if needed"
        local port
        port=$(echo "$health_url" | sed -n 's/.*:\([0-9]*\).*/\1/p')
        if [[ -n "$port" ]]; then
            log_message "     3. Verify port availability: lsof -i :$port"
        fi
        log_message "     4. Check system resources: ps aux | grep python"
    fi
}

log_message "🔥 THINK ULTRA! Starting services for testing..."
echo "=============================================="

# Check existing services with enhanced PID management
check_service 8000 "OMS" "$OMS_PID_FILE"
OMS_RUNNING=$?
check_service 8002 "BFF" "$BFF_PID_FILE"
BFF_RUNNING=$?

# Perform log maintenance before starting services
log_message "🧹 Performing quick log maintenance..."
log_message "   📋 Log rotation config: ${LOG_MAX_SIZE_MB}MB max size, ${LOG_MAX_FILES} max files, compress after ${LOG_COMPRESS_AFTER_DAYS}d, delete after ${LOG_DELETE_AFTER_DAYS}d"
if python -c "
import sys
import os
sys.path.insert(0, '$SCRIPT_DIR')

from shared.utils.log_rotation import LogRotationManager

try:
    # Create log rotation manager with configurable settings
    manager = LogRotationManager(
        log_dir='$LOG_DIR',
        max_size_mb=$LOG_MAX_SIZE_MB,
        max_files=$LOG_MAX_FILES,
        compress_after_days=$LOG_COMPRESS_AFTER_DAYS,
        delete_after_days=$LOG_DELETE_AFTER_DAYS
    )
    
    # Only perform rotation if needed, skip compression and cleanup for speed
    services = '$LOG_SERVICE_NAMES'.split()
    rotated_count = 0
    
    for service_name in services:
        import glob
        from pathlib import Path
        
        # Find current log files for this service
        pattern = os.path.join('$LOG_DIR', f'{service_name}_*.log')
        current_logs = glob.glob(pattern)
        
        for log_file_str in current_logs:
            log_file = Path(log_file_str)
            
            # Skip already rotated files
            if '_rotated_' in log_file.name:
                continue
            
            if manager.should_rotate(log_file):
                if manager.rotate_log_file(log_file, service_name):
                    rotated_count += 1
    
    print(f'LOG_ROTATION_SUCCESS=1')
    print(f'FILES_ROTATED={rotated_count}')
    sys.exit(0)
    
except Exception as e:
    print(f'LOG_ROTATION_SUCCESS=0')
    print(f'LOG_ROTATION_ERROR={str(e)}')
    sys.exit(0)  # Don't fail startup for log rotation issues
" 2>/dev/null; then
    eval "$(python -c "
import sys
import os
sys.path.insert(0, '$SCRIPT_DIR')

from shared.utils.log_rotation import LogRotationManager

try:
    manager = LogRotationManager(
        log_dir='$LOG_DIR',
        max_size_mb=$LOG_MAX_SIZE_MB,
        max_files=$LOG_MAX_FILES,
        compress_after_days=$LOG_COMPRESS_AFTER_DAYS,
        delete_after_days=$LOG_DELETE_AFTER_DAYS
    )
    services = '$LOG_SERVICE_NAMES'.split()
    rotated_count = 0
    
    for service_name in services:
        import glob
        from pathlib import Path
        
        pattern = os.path.join('$LOG_DIR', f'{service_name}_*.log')
        current_logs = glob.glob(pattern)
        
        for log_file_str in current_logs:
            log_file = Path(log_file_str)
            
            if '_rotated_' in log_file.name:
                continue
            
            if manager.should_rotate(log_file):
                if manager.rotate_log_file(log_file, service_name):
                    rotated_count += 1
    
    print(f'LOG_ROTATION_SUCCESS=1')
    print(f'FILES_ROTATED={rotated_count}')
    
except Exception as e:
    print(f'LOG_ROTATION_SUCCESS=0')
    print(f'LOG_ROTATION_ERROR={str(e)}')
" 2>/dev/null)"
    
    if [[ "$LOG_ROTATION_SUCCESS" == "1" ]]; then
        if [[ "${FILES_ROTATED:-0}" -gt 0 ]]; then
            log_message "   ✅ Rotated $FILES_ROTATED log files"
        else
            log_message "   ✅ No log rotation needed"
        fi
    else
        log_message "   ⚠️  Log rotation failed: ${LOG_ROTATION_ERROR:-unknown error}"
        log_message "   📋 Run 'maintain_logs.sh' manually if needed"
    fi
else
    log_message "   ⚠️  Log rotation check failed (continuing anyway)"
fi

# Start OMS if not running
if [ $OMS_RUNNING -ne 0 ]; then
    log_message "🚀 Starting OMS on port 8000..."
    cd "$SCRIPT_DIR/oms"
    
    # Start OMS with structured logging
    {
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] === OMS Service Started ==="
        python main.py 2>&1
    } > "$OMS_LOG_FILE" &
    
    OMS_PID=$!
    write_pid_file "$OMS_PID" "$OMS_PID_FILE"
    log_message "   OMS PID: $OMS_PID (Log: $OMS_LOG_FILE)"
    
    # Wait for service to initialize
    sleep 3
else
    log_message "✅ OMS is already running"
fi

# Start BFF if not running
if [ $BFF_RUNNING -ne 0 ]; then
    log_message "🚀 Starting BFF on port 8002..."
    cd "$SCRIPT_DIR/bff"
    
    # Start BFF with structured logging
    {
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] === BFF Service Started ==="
        python main.py 2>&1
    } > "$BFF_LOG_FILE" &
    
    BFF_PID=$!
    write_pid_file "$BFF_PID" "$BFF_PID_FILE"
    log_message "   BFF PID: $BFF_PID (Log: $BFF_LOG_FILE)"
    
    # Wait for service to initialize
    sleep 3
else
    log_message "✅ BFF is already running"
fi

# Enhanced health verification with detailed error reporting
echo ""
log_message "🔍 Verifying services..."
sleep 2

# Check OMS health with detailed diagnostics
check_service_health "OMS" "http://localhost:8000/health" "$OMS_PID_FILE" "$OMS_LOG_FILE"

# Check BFF health with detailed diagnostics
check_service_health "BFF" "http://localhost:8002/api/v1/health" "$BFF_PID_FILE" "$BFF_LOG_FILE"

echo ""
echo "=============================================="
log_message "✅ Services ready for testing!"
echo ""
echo "📋 Available Commands:"
echo "  Run tests:"
echo "    cd $SCRIPT_DIR && python tests/runners/run_complex_types_tests.py"
echo ""
echo "  Stop services (recommended):"
echo "    $SCRIPT_DIR/stop_test_services.sh"
echo ""
echo "  Stop services (manual):"
if [[ -f "$OMS_PID_FILE" ]]; then
    oms_pid=$(read_pid_file "$OMS_PID_FILE")
    echo "    kill $oms_pid  # OMS"
fi
if [[ -f "$BFF_PID_FILE" ]]; then
    bff_pid=$(read_pid_file "$BFF_PID_FILE")
    echo "    kill $bff_pid  # BFF"
fi
echo ""
echo "  View logs:"
if [[ -f "$OMS_LOG_FILE" ]]; then
    echo "    tail -f $OMS_LOG_FILE  # OMS"
fi
if [[ -f "$BFF_LOG_FILE" ]]; then
    echo "    tail -f $BFF_LOG_FILE  # BFF"
fi
echo "    ls -la $LOG_DIR/  # All logs"
echo ""
echo "  Service status:"
echo "    $SCRIPT_DIR/check_test_services.sh"
