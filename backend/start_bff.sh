#!/bin/bash
# 🔥 THINK ULTRA!! BFF Startup Script

# Get script directory and source unified PYTHONPATH configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/setup_pythonpath.sh"

# Configure Python environment (PYTHONPATH + directory change)
if ! configure_python_environment; then
    echo "❌ Failed to configure Python environment for BFF" >&2
    exit 1
fi

# Start BFF using module path
echo "🔥 THINK ULTRA!! Starting BFF..."
python -m bff.main