#!/bin/bash
# 🔥 THINK ULTRA!! Unified PYTHONPATH Configuration Script
# This script is now a lightweight wrapper around the Python implementation
# to ensure single source of truth and eliminate duplication

# Function to configure Python environment using Python module
configure_python_environment() {
    # Get script directory for relative path resolution
    local script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    
    # First, set minimal PYTHONPATH to allow importing our configuration module
    export PYTHONPATH="$script_dir:$PYTHONPATH"
    
    # Use Python module for the actual configuration (single source of truth)
    if python -c "from shared.utils.pythonpath_setup import configure_python_environment; import sys; sys.exit(0 if configure_python_environment() else 1)"; then
        # Change to backend directory (Python module sets PYTHONPATH, we handle directory change)
        local backend_dir
        backend_dir=$(python -c "from shared.utils.pythonpath_setup import detect_backend_directory; print(detect_backend_directory() or '')")
        
        if [[ -n "$backend_dir" && -d "$backend_dir" ]]; then
            cd "$backend_dir" || {
                echo "❌ Failed to change to backend directory: $backend_dir" >&2
                return 1
            }
        else
            echo "❌ Could not determine backend directory" >&2
            return 1
        fi
        
        return 0
    else
        echo "❌ Python environment configuration failed" >&2
        return 1
    fi
}

# If script is run directly (not sourced), run the configuration
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    configure_python_environment
fi