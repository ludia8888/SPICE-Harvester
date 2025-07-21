# Production Readiness Test Suite

This directory contains comprehensive tests to verify that the BFF -> OMS -> TerminusDB workflow is production ready.

## Test Coverage

The test suite covers:

1. **Health Check Endpoints** - Verify both services are healthy
2. **Database Operations** - Create, list, get, delete databases
3. **Ontology Management** - Create and manage ontology classes with multilingual labels
4. **Error Handling** - Validate proper error responses for invalid inputs
5. **Branching & Versioning** - Test branch creation and version history
6. **Query Operations** - Test data retrieval and filtering
7. **Performance & Limits** - Test large payloads and concurrent requests
8. **Cleanup** - Ensure test data is properly cleaned up

## Running the Tests

### Option 1: Automated Full Test Suite (Recommended)

Run the complete production test suite with automatic service startup:

```bash
cd backend/oms
./run_production_tests.sh
```

This script will:
- Install all dependencies
- Start OMS on port 8000
- Start BFF on port 8002
- Run comprehensive tests
- Generate a detailed test report
- Clean up services when done

### Option 2: Manual Testing

If you want to run services manually:

1. Start OMS:
```bash
cd backend/oms
python main.py
# or with custom port
OMS_PORT=8000 python main.py
```

2. Start BFF:
```bash
cd backend/bff
python main.py
# or with custom port
BFF_PORT=8002 python main.py
```

3. Start Funnel (Type Inference Service):
```bash
cd backend/funnel
python main.py
# or with custom port
FUNNEL_PORT=8003 python main.py
```

4. Run tests:
```bash
# Quick manual test
python tests/integration/oms/manual_test.py

# Full test suite
python tests/integration/oms/test_production_ready.py

# Or using pytest
pytest tests/integration/oms/test_production_ready.py -v
```

### Option 3: Start Services Helper

Use the service starter script:

```bash
cd backend
python start_services.py
```

This will start both services and keep them running until you press Ctrl+C.

## Test Results

After running the full test suite, you'll get:

1. **Console Output** - Real-time test progress with ✅ PASSED or ❌ FAILED indicators
2. **JSON Report** - Detailed report saved as `production_test_report_<timestamp>.json`
3. **Summary** - Overall assessment of production readiness

## Success Criteria

The system is considered production ready when:
- All tests pass (100% success rate)
- Services respond within acceptable time limits
- Error handling is robust
- Concurrent requests are handled properly
- Data integrity is maintained

## Troubleshooting

If tests fail:

1. Check service logs:
   - OMS logs: `OMS_*.log` in the OMS directory
   - BFF logs: `BFF_*.log` in the BFF directory

2. Verify TerminusDB is running:
   ```bash
   docker ps | grep terminusdb
   ```

3. Check port availability:
   ```bash
   lsof -i :8000  # OMS port
   lsof -i :8002  # BFF port
   ```

4. Review the detailed JSON test report for specific failure details

## Edge Cases Tested

- Invalid database names (with special characters)
- Missing required fields in requests
- Duplicate resource creation
- Non-existent resource access
- Malformed JSON payloads
- Large payloads (100+ properties)
- Concurrent requests (20 simultaneous)
- Multilingual label mapping
- Complex ontology structures

## Production Deployment Checklist

Once tests pass:

1. ✅ All tests passing
2. ✅ Services configured with production settings
3. ✅ Environment variables properly set
4. ✅ Database connections validated
5. ✅ Monitoring and logging configured
6. ✅ Error tracking enabled
7. ✅ Rate limiting configured
8. ✅ CORS settings reviewed
9. ✅ Authentication/authorization enabled
10. ✅ SSL/TLS certificates configured