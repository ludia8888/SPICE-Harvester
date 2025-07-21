#!/bin/bash

# 🔥 THINK ULTRA! Coverage Analysis Runner
# Enhanced test coverage analysis with comprehensive reporting

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
COVERAGE_SCRIPT="${SCRIPT_DIR}/run_coverage_report.py"
PROJECT_ROOT="${SCRIPT_DIR}"
TEST_PATTERN="tests/"
INCLUDE_INTEGRATION=true
INCLUDE_PERFORMANCE=false
QUICK_MODE=false
VERBOSE=false

# Function to show usage
show_usage() {
    echo "🔥 THINK ULTRA! Coverage Analysis Runner"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --test-pattern PATTERN     Test pattern to run (default: tests/)"
    echo "  --no-integration          Exclude integration tests"
    echo "  --include-performance     Include performance tests"
    echo "  --quick                   Quick analysis (unit tests only)"
    echo "  --project-root PATH       Project root directory"
    echo "  --verbose                 Verbose output"
    echo "  --help                    Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                        # Run full coverage analysis"
    echo "  $0 --quick               # Quick unit test coverage only"
    echo "  $0 --include-performance # Include performance benchmarks"
    echo "  $0 --test-pattern tests/unit/  # Only unit tests"
    echo ""
}

# Function to log messages
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Python is available
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is required but not installed"
        exit 1
    fi
    
    # Check if coverage script exists
    if [[ ! -f "$COVERAGE_SCRIPT" ]]; then
        log_error "Coverage script not found: $COVERAGE_SCRIPT"
        exit 1
    fi
    
    # Check if coverage script is executable
    if [[ ! -x "$COVERAGE_SCRIPT" ]]; then
        log_warning "Making coverage script executable..."
        chmod +x "$COVERAGE_SCRIPT"
    fi
    
    # Check if pytest is available
    if ! python3 -c "import pytest" &> /dev/null; then
        log_warning "pytest not found, attempting to install test dependencies..."
        if [[ -f "${PROJECT_ROOT}/pyproject.toml" ]]; then
            pip install -e "${PROJECT_ROOT}[test]" || {
                log_error "Failed to install test dependencies"
                exit 1
            }
        else
            log_error "Could not find pyproject.toml to install dependencies"
            exit 1
        fi
    fi
    
    # Check if psutil is available (needed for performance tests)
    if [[ "$INCLUDE_PERFORMANCE" == true ]]; then
        if ! python3 -c "import psutil" &> /dev/null; then
            log_warning "psutil not found, required for performance tests"
            if [[ -f "${PROJECT_ROOT}/pyproject.toml" ]]; then
                pip install psutil || {
                    log_error "Failed to install psutil"
                    exit 1
                }
            fi
        fi
    fi
    
    log_success "Prerequisites check completed"
}

# Function to prepare environment
prepare_environment() {
    log_info "Preparing test environment..."
    
    # Set PYTHONPATH
    export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"
    
    # Create results directory
    mkdir -p "${PROJECT_ROOT}/tests/results/coverage"
    
    # Clean up old coverage files
    rm -f "${PROJECT_ROOT}/coverage.xml" "${PROJECT_ROOT}/.coverage"
    rm -rf "${PROJECT_ROOT}/htmlcov"
    
    log_success "Environment prepared"
}

# Function to run coverage analysis
run_coverage_analysis() {
    log_info "Starting comprehensive coverage analysis..."
    echo "==================================================================================="
    echo "🔥 THINK ULTRA! Coverage Analysis - $(date)"
    echo "==================================================================================="
    
    # Build arguments for coverage script
    local args=()
    args+=("--project-root" "$PROJECT_ROOT")
    args+=("--test-pattern" "$TEST_PATTERN")
    
    if [[ "$INCLUDE_INTEGRATION" != true ]]; then
        args+=("--no-integration")
    fi
    
    if [[ "$INCLUDE_PERFORMANCE" == true ]]; then
        args+=("--include-performance")
    fi
    
    # Run the coverage analysis
    if [[ "$VERBOSE" == true ]]; then
        log_info "Running: python3 $COVERAGE_SCRIPT ${args[*]}"
    fi
    
    cd "$PROJECT_ROOT"
    
    if python3 "$COVERAGE_SCRIPT" "${args[@]}"; then
        log_success "Coverage analysis completed successfully"
        return 0
    else
        local exit_code=$?
        log_error "Coverage analysis failed with exit code $exit_code"
        return $exit_code
    fi
}

# Function to show coverage summary
show_coverage_summary() {
    log_info "Coverage Analysis Summary"
    echo "==================================================================================="
    
    # Show location of reports
    echo -e "${CYAN}📄 Generated Reports:${NC}"
    echo "  • HTML Report:     htmlcov/index.html"
    echo "  • XML Report:      coverage.xml"
    echo "  • Detailed Reports: tests/results/coverage/"
    echo ""
    
    # Show how to view HTML report
    if [[ -f "${PROJECT_ROOT}/htmlcov/index.html" ]]; then
        echo -e "${CYAN}🌐 View HTML Report:${NC}"
        echo "  open htmlcov/index.html"
        echo ""
    fi
    
    # Show latest coverage files
    local coverage_dir="${PROJECT_ROOT}/tests/results/coverage"
    if [[ -d "$coverage_dir" ]]; then
        echo -e "${CYAN}📊 Latest Reports:${NC}"
        ls -la "$coverage_dir"/*.json 2>/dev/null | tail -3 || echo "  No JSON reports found"
        echo ""
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --test-pattern)
            TEST_PATTERN="$2"
            shift 2
            ;;
        --no-integration)
            INCLUDE_INTEGRATION=false
            shift
            ;;
        --include-performance)
            INCLUDE_PERFORMANCE=true
            shift
            ;;
        --quick)
            QUICK_MODE=true
            TEST_PATTERN="tests/unit/"
            INCLUDE_INTEGRATION=false
            INCLUDE_PERFORMANCE=false
            shift
            ;;
        --project-root)
            PROJECT_ROOT="$2"
            COVERAGE_SCRIPT="${PROJECT_ROOT}/run_coverage_report.py"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    # Show configuration
    if [[ "$VERBOSE" == true ]]; then
        echo -e "${MAGENTA}🔧 Configuration:${NC}"
        echo "  Project Root: $PROJECT_ROOT"
        echo "  Test Pattern: $TEST_PATTERN"
        echo "  Include Integration: $INCLUDE_INTEGRATION"
        echo "  Include Performance: $INCLUDE_PERFORMANCE"
        echo "  Quick Mode: $QUICK_MODE"
        echo ""
    fi
    
    # Execute steps
    check_prerequisites
    prepare_environment
    
    local start_time=$(date +%s)
    
    if run_coverage_analysis; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        show_coverage_summary
        
        echo "==================================================================================="
        log_success "Coverage analysis completed in ${duration} seconds"
        echo "==================================================================================="
        
        exit 0
    else
        local exit_code=$?
        echo "==================================================================================="
        log_error "Coverage analysis failed"
        echo "==================================================================================="
        
        exit $exit_code
    fi
}

# Run main function
main "$@"