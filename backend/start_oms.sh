#!/bin/bash
# 🔥 THINK ULTRA!! OMS Startup Script

# Get script directory and source unified PYTHONPATH configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/setup_pythonpath.sh"

# Configure Python environment (PYTHONPATH + directory change)
if ! configure_python_environment; then
    echo "❌ Failed to configure Python environment for OMS" >&2
    exit 1
fi

# Start OMS using module path
echo "🔥 THINK ULTRA!! Starting OMS..."
python -m oms.main