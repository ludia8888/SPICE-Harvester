# Production Readiness Test Suite

**Last Updated**: 2025-07-26

This directory contains comprehensive tests to verify that the BFF -> OMS -> TerminusDB workflow is production ready.

## 🔥 Recent Updates

### Performance Improvements (2025-07-26)
- **HTTP Connection Pooling**: 50 keep-alive, 100 max connections
- **Concurrent Request Control**: Semaphore(50) to prevent TerminusDB overload
- **Response Time**: Target <5s (previously 29.8s average)
- **Success Rate**: Target >95% (previously 70.3%)

### Error Handling Fixes
- **404 Propagation**: Non-existent resources now correctly return 404 (not 500)
- **409 Conflicts**: Duplicate IDs return proper conflict status
- **Error Messages**: Detailed Korean error messages with proper HTTP codes

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
# or with custom port (default is 8000)
OMS_PORT=8000 python main.py
```

**Note**: OMS now uses port 8000 by default (not 8005)

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
- Services respond within acceptable time limits (<5s average)
- Error handling is robust (proper 404, 409, 400 status codes)
- Concurrent requests are handled properly (>95% success rate)
- Data integrity is maintained
- No timeout errors under load (1000 concurrent requests)

### Current Performance Metrics
- **Load Test**: 1000 concurrent requests
- **Success Rate**: 70.3% → 95%+ (target)
- **Average Response Time**: 29.8s → <5s (target)
- **Timeout Rate**: 29.1% → <5% (target)

## Troubleshooting

If tests fail:

1. Check service logs:
   - OMS logs: `oms.log` in the backend directory
   - BFF logs: `bff.log` in the backend directory
   - Check for "Exception type" entries for error details

2. Verify TerminusDB is running:
   ```bash
   docker ps | grep terminusdb
   # Check if running on port 6364 (not 6363)
   ```

3. Check port availability:
   ```bash
   lsof -i :8000  # OMS port
   lsof -i :8002  # BFF port
   lsof -i :8004  # Funnel port (not 8003)
   ```

4. Review the detailed JSON test report for specific failure details

5. Common Issues:
   - **AsyncOntologyNotFoundError**: Fixed - was import error
   - **500 instead of 404**: Fixed - proper error propagation
   - **Port conflicts**: Check if ports are already in use
   - **TerminusDB auth**: Key is 'admin' not 'admin123'

## Edge Cases Tested

- Invalid database names (with special characters)
- Missing required fields in requests
- Duplicate resource creation (409 Conflict)
- Non-existent resource access (404 Not Found)
- Malformed JSON payloads (400 Bad Request)
- Large payloads (100+ properties)
- Concurrent requests (50-1000 simultaneous)
- Multilingual label mapping
- Complex ontology structures
- BFF property name auto-generation from labels
- XSD type mapping (STRING → xsd:string)
- Metadata schema caching and deduplication

## Production Deployment Checklist

Once tests pass:

1. ✅ All tests passing (including high-load tests)
2. ✅ Services configured with production settings
3. ✅ Environment variables properly set:
   - `TERMINUS_SERVER_URL=http://localhost:6364`
   - `TERMINUS_KEY=admin`
   - `OMS_PORT=8000`
   - `BFF_PORT=8002`
   - `FUNNEL_PORT=8004`
4. ✅ Database connections validated with pooling
5. ✅ Monitoring and logging configured
6. ✅ Error tracking enabled (proper HTTP status codes)
7. ✅ Rate limiting configured (Semaphore 50)
8. ✅ CORS settings reviewed (Service Factory)
9. ✅ Authentication/authorization enabled
10. ✅ SSL/TLS certificates configured
11. ✅ HTTP connection pooling optimized
12. ✅ ApiResponse format standardized