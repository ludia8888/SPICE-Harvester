#!/bin/bash
# 🔥 THINK ULTRA! 테스트 서비스 종료 스크립트
# Enhanced with proper PID management and graceful shutdown

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# PID directory and files
PID_DIR="$SCRIPT_DIR/.pids"
OMS_PID_FILE="$PID_DIR/oms.pid"
BFF_PID_FILE="$PID_DIR/bff.pid"

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

# Graceful service stop function
stop_service() {
    local service_name=$1
    local pid_file=$2
    local timeout=${3:-10}  # Default 10 seconds timeout
    
    local pid
    pid=$(read_pid_file "$pid_file")
    
    if [[ -z "$pid" ]]; then
        log_message "📝 No PID file found for $service_name ($pid_file)"
        return 0
    fi
    
    if ! is_process_running "$pid"; then
        log_message "📝 $service_name process (PID: $pid) is not running"
        rm -f "$pid_file"
        log_message "🧹 Cleaned up stale PID file for $service_name"
        return 0
    fi
    
    log_message "🛑 Stopping $service_name (PID: $pid)..."
    
    # Step 1: Send SIGTERM for graceful shutdown
    if kill -TERM "$pid" 2>/dev/null; then
        log_message "   📤 Sent SIGTERM to $service_name"
        
        # Wait for graceful shutdown
        local count=0
        while is_process_running "$pid" && [[ $count -lt $timeout ]]; do
            sleep 1
            ((count++))
            if [[ $((count % 3)) -eq 0 ]]; then
                log_message "   ⏳ Waiting for $service_name to shutdown gracefully... ($count/${timeout}s)"
            fi
        done
        
        # Check if process stopped gracefully
        if ! is_process_running "$pid"; then
            log_message "   ✅ $service_name stopped gracefully"
            rm -f "$pid_file"
            log_message "   🧹 Cleaned up PID file for $service_name"
            return 0
        fi
    fi
    
    # Step 2: Force kill if graceful shutdown failed
    log_message "   ⚠️  Graceful shutdown failed, forcing termination..."
    if kill -KILL "$pid" 2>/dev/null; then
        log_message "   💥 Sent SIGKILL to $service_name"
        sleep 2
        
        if ! is_process_running "$pid"; then
            log_message "   ✅ $service_name force-stopped"
            rm -f "$pid_file"
            log_message "   🧹 Cleaned up PID file for $service_name"
            return 0
        else
            log_message "   ❌ Failed to stop $service_name (PID: $pid)"
            return 1
        fi
    else
        log_message "   ❌ Failed to send SIGKILL to $service_name (PID: $pid)"
        return 1
    fi
}

# Show current service status before stopping
show_service_status() {
    log_message "🔍 Current service status:"
    
    # Check OMS
    local oms_pid
    oms_pid=$(read_pid_file "$OMS_PID_FILE")
    if [[ -n "$oms_pid" ]] && is_process_running "$oms_pid"; then
        log_message "   OMS: Running (PID: $oms_pid)"
    elif [[ -n "$oms_pid" ]]; then
        log_message "   OMS: PID file exists but process not running (PID: $oms_pid)"
    else
        log_message "   OMS: Not running (no PID file)"
    fi
    
    # Check BFF
    local bff_pid
    bff_pid=$(read_pid_file "$BFF_PID_FILE")
    if [[ -n "$bff_pid" ]] && is_process_running "$bff_pid"; then
        log_message "   BFF: Running (PID: $bff_pid)"
    elif [[ -n "$bff_pid" ]]; then
        log_message "   BFF: PID file exists but process not running (PID: $bff_pid)"
    else
        log_message "   BFF: Not running (no PID file)"
    fi
}

# Main execution
log_message "🔥 THINK ULTRA! Stopping test services..."
echo "=============================================="

# Check if PID directory exists
if [[ ! -d "$PID_DIR" ]]; then
    log_message "📝 No PID directory found ($PID_DIR)"
    log_message "✅ No services to stop"
    exit 0
fi

# Show current status
show_service_status

echo ""
log_message "🛑 Initiating service shutdown..."

# Stop services with different timeouts based on service type
STOP_ERRORS=0

# Stop OMS (longer timeout as it might have ongoing operations)
if ! stop_service "OMS" "$OMS_PID_FILE" 15; then
    ((STOP_ERRORS++))
fi

# Stop BFF (shorter timeout as it's just a proxy)
if ! stop_service "BFF" "$BFF_PID_FILE" 10; then
    ((STOP_ERRORS++))
fi

# Final status check
echo ""
log_message "🔍 Final status verification..."

# Verify all services are stopped
ALL_STOPPED=true

oms_pid=$(read_pid_file "$OMS_PID_FILE")
if [[ -n "$oms_pid" ]] && is_process_running "$oms_pid"; then
    log_message "❌ OMS is still running (PID: $oms_pid)"
    ALL_STOPPED=false
fi

bff_pid=$(read_pid_file "$BFF_PID_FILE")
if [[ -n "$bff_pid" ]] && is_process_running "$bff_pid"; then
    log_message "❌ BFF is still running (PID: $bff_pid)"
    ALL_STOPPED=false
fi

# Clean up empty PID directory if all services stopped
if [[ "$ALL_STOPPED" == "true" ]]; then
    if [[ -d "$PID_DIR" ]] && [[ -z "$(ls -A "$PID_DIR" 2>/dev/null)" ]]; then
        rmdir "$PID_DIR" 2>/dev/null
        log_message "🧹 Cleaned up empty PID directory"
    fi
fi

# Final summary
echo ""
echo "=============================================="
if [[ "$ALL_STOPPED" == "true" && $STOP_ERRORS -eq 0 ]]; then
    log_message "✅ All services stopped successfully!"
    echo ""
    echo "📋 Next steps:"
    echo "  Start services again:"
    echo "    $SCRIPT_DIR/start_test_services.sh"
    echo ""
    echo "  Check service status:"
    echo "    $SCRIPT_DIR/check_test_services.sh"
    exit 0
elif [[ "$ALL_STOPPED" == "true" ]]; then
    log_message "⚠️  All services stopped, but some errors occurred during shutdown"
    exit 1
else
    log_message "❌ Some services failed to stop properly"
    echo ""
    echo "🛠️ Manual cleanup may be required:"
    if [[ -n "$oms_pid" ]] && is_process_running "$oms_pid"; then
        echo "  kill -9 $oms_pid  # Force kill OMS"
    fi
    if [[ -n "$bff_pid" ]] && is_process_running "$bff_pid"; then
        echo "  kill -9 $bff_pid  # Force kill BFF"
    fi
    echo "  rm -rf $PID_DIR  # Clean up PID files"
    exit 1
fi