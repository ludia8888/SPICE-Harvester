# 🔥 SPICE HARVESTER Port Configuration Guide

## Overview
All service ports are now centrally configured through environment variables, eliminating hardcoded port conflicts.

## Default Port Assignments
- **OMS (Ontology Management Service)**: 8000
- **BFF (Backend for Frontend)**: 8002
- **Funnel (Type Inference Service)**: 8004 (now active!)
- **TerminusDB**: 6364

## Configuration Methods

### 1. Environment Variables
Set these in your `.env` file or shell environment:
```bash
OMS_PORT=8000
BFF_PORT=8002
FUNNEL_PORT=8004
OMS_BASE_URL=http://localhost:8000  # Optional
BFF_BASE_URL=http://localhost:8002  # Optional
FUNNEL_BASE_URL=http://localhost:8004  # Optional
```

### 2. Using ServiceConfig
All services now use `shared/config/service_config.py`:
```python
from shared.config.service_config import ServiceConfig

# Get ports
oms_port = ServiceConfig.get_oms_port()  # Returns 8000 or $OMS_PORT
bff_port = ServiceConfig.get_bff_port()  # Returns 8002 or $BFF_PORT
funnel_port = ServiceConfig.get_funnel_port()  # Returns 8004 or $FUNNEL_PORT

# Get URLs
oms_url = ServiceConfig.get_oms_url()  # Returns http://localhost:8000 or $OMS_BASE_URL
bff_url = ServiceConfig.get_bff_url()  # Returns http://localhost:8002 or $BFF_BASE_URL
funnel_url = ServiceConfig.get_funnel_url()  # Returns http://localhost:8004 or $FUNNEL_BASE_URL
```

## Changes Made

### OMS (oms/main.py)
- Changed from hardcoded `port=8001` to `port=ServiceConfig.get_oms_port()`
- Now uses port 8000 by default (matching all tests and scripts)

### BFF (bff/main.py)
- Changed from hardcoded `port=8002` to `port=ServiceConfig.get_bff_port()`
- OMS client initialization now uses `ServiceConfig.get_oms_url()`

### BFF OMS Client (bff/services/oms_client.py)
- Changed from hardcoded `"http://localhost:8000"` to `ServiceConfig.get_oms_url()`
- Automatically picks up OMS_BASE_URL or constructs from OMS_HOST/OMS_PORT

### Test Configuration (test_config.py)
- Now uses ServiceConfig as fallback for all URLs
- TEST_* environment variables still take precedence for test overrides

### Funnel Data Processor
- Changed from hardcoded BFF URL to `ServiceConfig.get_bff_url()`

## Running Services

### Option 1: Using Default Ports
```bash
# Terminal 1 - Start OMS (port 8000)
cd oms
python main.py

# Terminal 2 - Start Funnel (port 8004)
cd funnel
python main.py

# Terminal 3 - Start BFF (port 8002)
cd bff
python main.py
```

### Option 2: Using Custom Ports
```bash
# Terminal 1 - Start OMS on port 8001
cd oms
OMS_PORT=8001 python main.py

# Terminal 2 - Start BFF and connect to OMS on 8001
cd bff
OMS_BASE_URL=http://localhost:8001 python main.py
```

### Option 3: Using start_services.py (Recommended!)
Start all services with one command:
```bash
cd backend
python start_services.py
```

This will automatically start:
- OMS on port 8000
- Funnel on port 8004  
- BFF on port 8002

And provide helpful API endpoints for schema suggestion!

## Docker Configuration
Docker Compose already uses environment variables:
```yaml
services:
  oms:
    ports:
      - "${OMS_PORT:-8000}:8000"
  bff:
    environment:
      - OMS_BASE_URL=${OMS_BASE_URL:-http://oms:8000}
    ports:
      - "${BFF_PORT:-8002}:8002"
```

## Testing
Tests automatically use the configured ports:
```bash
# Run tests with default ports
pytest

# Run tests with custom OMS port
TEST_OMS_URL=http://localhost:8001 pytest
```

## Benefits
1. **No More Port Conflicts**: Services always know where to find each other
2. **Environment-Specific Config**: Easy to change ports for dev/staging/prod
3. **Backward Compatible**: All existing scripts and tests continue to work
4. **Centralized Management**: One place to configure all service locations
5. **Docker-Friendly**: Works seamlessly in containerized environments