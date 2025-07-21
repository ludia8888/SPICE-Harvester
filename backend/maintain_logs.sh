#!/bin/bash
# 🔥 THINK ULTRA! 주기적 로그 유지보수 스크립트
# 로그 파일 로테이션, 압축, 정리를 자동으로 수행

# Get script directory and source unified PYTHONPATH configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/setup_pythonpath.sh"

# Configure Python environment (PYTHONPATH + directory change)
if ! configure_python_environment; then
    echo "❌ Failed to configure Python environment for log maintenance" >&2
    exit 1
fi

# Default configuration (can be overridden by environment variables)
LOG_DIR="${LOG_MAINTENANCE_DIR:-$SCRIPT_DIR/logs}"
MAX_SIZE_MB="${LOG_MAX_SIZE_MB:-5}"
MAX_FILES="${LOG_MAX_FILES:-5}"
COMPRESS_AFTER_DAYS="${LOG_COMPRESS_AFTER_DAYS:-1}"
DELETE_AFTER_DAYS="${LOG_DELETE_AFTER_DAYS:-7}"
SERVICE_NAMES="${LOG_SERVICE_NAMES:-oms bff}"

# Logging function with timestamps
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Validate configuration
validate_config() {
    local errors=0
    
    # Check if log directory exists
    if [[ ! -d "$LOG_DIR" ]]; then
        log_message "⚠️  Log directory does not exist: $LOG_DIR"
        log_message "🛠️  Creating log directory..."
        if ! mkdir -p "$LOG_DIR"; then
            log_message "❌ Failed to create log directory: $LOG_DIR"
            ((errors++))
        else
            log_message "✅ Created log directory: $LOG_DIR"
        fi
    fi
    
    # Validate numeric parameters
    if ! [[ "$MAX_SIZE_MB" =~ ^[0-9]+$ ]] || [[ "$MAX_SIZE_MB" -lt 1 ]]; then
        log_message "❌ Invalid MAX_SIZE_MB: $MAX_SIZE_MB (must be positive integer)"
        ((errors++))
    fi
    
    if ! [[ "$MAX_FILES" =~ ^[0-9]+$ ]] || [[ "$MAX_FILES" -lt 1 ]]; then
        log_message "❌ Invalid MAX_FILES: $MAX_FILES (must be positive integer)"
        ((errors++))
    fi
    
    if ! [[ "$COMPRESS_AFTER_DAYS" =~ ^[0-9]+$ ]] || [[ "$COMPRESS_AFTER_DAYS" -lt 0 ]]; then
        log_message "❌ Invalid COMPRESS_AFTER_DAYS: $COMPRESS_AFTER_DAYS (must be non-negative integer)"
        ((errors++))
    fi
    
    if ! [[ "$DELETE_AFTER_DAYS" =~ ^[0-9]+$ ]] || [[ "$DELETE_AFTER_DAYS" -lt 1 ]]; then
        log_message "❌ Invalid DELETE_AFTER_DAYS: $DELETE_AFTER_DAYS (must be positive integer)"
        ((errors++))
    fi
    
    if [[ "$DELETE_AFTER_DAYS" -le "$COMPRESS_AFTER_DAYS" ]]; then
        log_message "❌ DELETE_AFTER_DAYS ($DELETE_AFTER_DAYS) must be greater than COMPRESS_AFTER_DAYS ($COMPRESS_AFTER_DAYS)"
        ((errors++))
    fi
    
    return $errors
}

# Show current configuration
show_config() {
    log_message "🔧 Log Maintenance Configuration:"
    echo "   Log Directory: $LOG_DIR"
    echo "   Max Size per File: ${MAX_SIZE_MB}MB"
    echo "   Max Rotated Files: $MAX_FILES"
    echo "   Compress After: $COMPRESS_AFTER_DAYS days"
    echo "   Delete After: $DELETE_AFTER_DAYS days" 
    echo "   Service Names: $SERVICE_NAMES"
    echo ""
}

# Check disk space before and after maintenance
check_disk_space() {
    local location="$1"
    local description="$2"
    
    if command -v df >/dev/null 2>&1; then
        local disk_info
        disk_info=$(df -h "$LOG_DIR" 2>/dev/null | tail -1)
        if [[ -n "$disk_info" ]]; then
            local used available use_percent
            used=$(echo "$disk_info" | awk '{print $3}')
            available=$(echo "$disk_info" | awk '{print $4}')
            use_percent=$(echo "$disk_info" | awk '{print $5}')
            
            log_message "💾 Disk space $location: Used: $used, Available: $available ($use_percent)"
        fi
    fi
}

# Get directory size
get_directory_size() {
    local dir="$1"
    if [[ -d "$dir" ]] && command -v du >/dev/null 2>&1; then
        du -sh "$dir" 2>/dev/null | cut -f1
    else
        echo "N/A"
    fi
}

# Run Python log maintenance
run_maintenance() {
    log_message "🔥 Starting log maintenance with Python utility..."
    
    # Convert service names string to array for Python
    local services_array=($SERVICE_NAMES)
    
    # Run Python log rotation script
    local python_cmd="
import sys
import os
sys.path.insert(0, '$SCRIPT_DIR')

from shared.utils.log_rotation import LogRotationManager

# Create log rotation manager with current configuration
manager = LogRotationManager(
    log_dir='$LOG_DIR',
    max_size_mb=$MAX_SIZE_MB,
    max_files=$MAX_FILES,
    compress_after_days=$COMPRESS_AFTER_DAYS,
    delete_after_days=$DELETE_AFTER_DAYS
)

# Service names to process
services = [$(printf "'%s'," "${services_array[@]}" | sed 's/,$//')']

try:
    # Perform maintenance
    stats = manager.perform_maintenance(services)
    
    # Output statistics for shell script
    print(f'ROTATED_FILES={stats[\"rotated_files\"]}')
    print(f'COMPRESSED_FILES={stats[\"compressed_files\"]}')
    print(f'DELETED_FILES={stats[\"deleted_files\"]}')
    print(f'FREED_SPACE_MB={stats[\"freed_space_mb\"]:.2f}')
    print(f'LIMITED_FILES={stats[\"limited_files\"]}')
    print(f'ERROR_COUNT={len(stats[\"errors\"])}')
    
    if stats['errors']:
        print('ERRORS_LIST=' + '|'.join(stats['errors']))
    else:
        print('ERRORS_LIST=')
    
    # Exit with error if there were problems
    sys.exit(0 if len(stats['errors']) == 0 else 1)
    
except Exception as e:
    print(f'PYTHON_ERROR={str(e)}')
    sys.exit(1)
"
    
    # Execute Python maintenance and capture output
    local python_output
    if python_output=$(python -c "$python_cmd" 2>&1); then
        # Parse Python output
        local rotated_files compressed_files deleted_files freed_space_mb limited_files error_count errors_list python_error
        
        eval "$python_output"
        
        # Check for Python errors
        if [[ -n "$python_error" ]]; then
            log_message "❌ Python maintenance failed: $python_error"
            return 1
        fi
        
        # Report statistics
        log_message "📊 Maintenance completed successfully:"
        echo "   Files rotated: ${rotated_files:-0}"
        echo "   Files compressed: ${compressed_files:-0}"
        echo "   Files deleted: ${deleted_files:-0}"
        echo "   Space freed: ${freed_space_mb:-0}MB"
        echo "   Files limited: ${limited_files:-0}"
        
        if [[ "${error_count:-0}" -gt 0 ]] && [[ -n "$errors_list" ]]; then
            log_message "⚠️  Errors occurred during maintenance:"
            IFS='|' read -ra error_array <<< "$errors_list"
            for error in "${error_array[@]}"; do
                echo "   - $error"
            done
            return 1
        fi
        
        return 0
        
    else
        log_message "❌ Python maintenance script failed:"
        echo "$python_output"
        return 1
    fi
}

# Main execution
main() {
    log_message "🔥 THINK ULTRA! Log Maintenance Starting..."
    echo "=============================================="
    
    # Show configuration
    show_config
    
    # Validate configuration
    if ! validate_config; then
        log_message "❌ Configuration validation failed"
        exit 1
    fi
    
    # Check initial disk space and directory size
    local initial_size
    initial_size=$(get_directory_size "$LOG_DIR")
    log_message "📁 Initial log directory size: $initial_size"
    check_disk_space "before maintenance"
    
    echo ""
    
    # Run maintenance
    local maintenance_success=0
    if run_maintenance; then
        maintenance_success=1
    fi
    
    # Check final disk space and directory size
    echo ""
    local final_size
    final_size=$(get_directory_size "$LOG_DIR")
    log_message "📁 Final log directory size: $final_size"
    check_disk_space "after maintenance"
    
    echo ""
    echo "=============================================="
    
    if [[ $maintenance_success -eq 1 ]]; then
        log_message "✅ Log maintenance completed successfully!"
        echo ""
        echo "📋 Next Steps:"
        echo "  Manual maintenance: $SCRIPT_DIR/maintain_logs.sh"
        echo "  Check logs: ls -la $LOG_DIR/"
        echo "  Service management: $SCRIPT_DIR/check_test_services.sh"
        echo ""
        echo "💡 Schedule this script to run regularly via cron:"
        echo "  # Add to crontab: crontab -e"
        echo "  # Run every hour: 0 * * * * $SCRIPT_DIR/maintain_logs.sh"
        echo "  # Run daily at 2am: 0 2 * * * $SCRIPT_DIR/maintain_logs.sh"
        
        exit 0
    else
        log_message "❌ Log maintenance completed with errors"
        echo ""
        echo "🛠️  Troubleshooting:"
        echo "  Check configuration: LOG_MAX_SIZE_MB, LOG_MAX_FILES, etc."
        echo "  Check permissions: ls -la $LOG_DIR"
        echo "  Check disk space: df -h $LOG_DIR"
        echo "  Manual cleanup: rm $LOG_DIR/*_rotated_*.log.gz"
        
        exit 1
    fi
}

# Show usage if help requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "🔥 THINK ULTRA! Log Maintenance Script"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Environment Variables:"
    echo "  LOG_MAINTENANCE_DIR     Log directory path (default: ./logs)"
    echo "  LOG_MAX_SIZE_MB         Max file size before rotation in MB (default: 5)"
    echo "  LOG_MAX_FILES           Max rotated files to keep (default: 5)"
    echo "  LOG_COMPRESS_AFTER_DAYS Compress files after N days (default: 1)"
    echo "  LOG_DELETE_AFTER_DAYS   Delete files after N days (default: 7)"
    echo "  LOG_SERVICE_NAMES       Space-separated service names (default: 'oms bff')"
    echo ""
    echo "Examples:"
    echo "  $0                                      # Use defaults"
    echo "  LOG_MAX_SIZE_MB=10 $0                   # Larger rotation size"
    echo "  LOG_DELETE_AFTER_DAYS=14 $0             # Keep logs longer"
    echo "  LOG_SERVICE_NAMES='oms bff funnel' $0   # More services"
    echo ""
    echo "Cron Examples:"
    echo "  0 * * * * $SCRIPT_DIR/maintain_logs.sh   # Every hour"
    echo "  0 2 * * * $SCRIPT_DIR/maintain_logs.sh   # Daily at 2am"
    exit 0
fi

# Run main function
main "$@"