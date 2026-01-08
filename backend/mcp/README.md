# MCP (Model Context Protocol) Integration

This directory contains the MCP integration for SPICE HARVESTER, enabling connection with Context7 and other MCP-compatible services.

## Overview

MCP (Model Context Protocol) is a standardized protocol for AI model interactions with external tools and services. This integration allows SPICE HARVESTER to:

- Connect with Context7 for enhanced knowledge management
- Access TerminusDB through MCP interface
- Integrate with filesystem, database, and git operations
- Provide unified interface for multiple MCP servers

## Components

### 1. MCP Configuration (`/mcp-config.json`)
Main configuration file defining all MCP servers:
- **context7**: Context7 knowledge management server
- **filesystem**: File system access server
- **database**: PostgreSQL database server
- **terminusdb**: Custom TerminusDB MCP server
- **elasticsearch**: Search and analytics server
- **git**: Version control server

### 2. TerminusDB MCP Server (`terminus_mcp_server.py`)
Custom MCP server exposing TerminusDB functionality:
- Database management (create, list, delete)
- Ontology operations (create, read, update)
- Branch management
- Query execution

### 3. MCP Client (`mcp_client.py`)
Client implementation for connecting to MCP servers:
- `MCPClientManager`: Manages multiple MCP connections
- `Context7Client`: Specialized client for Context7 operations

### 4. Context7 Router (`/bff/routers/context7.py`)
FastAPI router providing REST endpoints for Context7:
- `/api/v1/context7/search`: Search knowledge base
- `/api/v1/context7/context/{entity_id}`: Get entity context
- `/api/v1/context7/knowledge`: Add new knowledge
- `/api/v1/context7/link`: Create entity relationships
- `/api/v1/context7/analyze/ontology`: Analyze ontology with AI

## Setup

### 1. Install MCP Dependencies

```bash
# Install MCP Python SDK
pip install mcp

# Install MCP servers via npm
npm install -g @modelcontextprotocol/server-filesystem
npm install -g @modelcontextprotocol/server-postgres
npm install -g @modelcontextprotocol/server-elasticsearch
npm install -g @modelcontextprotocol/server-git
npm install -g @context7/mcp-server
```

### 2. Configure Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# Context7 Configuration
CONTEXT7_API_KEY=your_api_key_here
CONTEXT7_WORKSPACE=your_workspace_name

# MCP Configuration
MCP_CONFIG_PATH=./mcp-config.json
MCP_LOG_LEVEL=info
MCP_TIMEOUT=30000
```

Docker 환경에서는 `MCP_CONFIG_PATH=/app/mcp-config.json` 처럼 컨테이너 내부 경로를 사용하세요.

### 3. Start MCP Servers

#### Option A: Manual Start
```bash
# Start TerminusDB MCP Server
python backend/mcp/terminus_mcp_server.py

# Context7 server starts automatically when accessed
```

#### Option B: Using Docker Compose
Add to `docker-compose.yml`:

```yaml
mcp-terminus:
  build:
    context: .
    dockerfile: ./backend/mcp/Dockerfile
  container_name: spice_mcp_terminus
  environment:
    - TERMINUS_SERVER_URL=http://terminusdb:6363
    - TERMINUS_USER=admin
    - TERMINUS_KEY=admin123
  networks:
    - spice_network
```

`bff` 서비스에서 Context7을 쓰는 경우:
- 컨테이너에 `nodejs`/`npm`이 설치되어 있어야 합니다. (`npx` 사용)
- `mcp-config.json`을 컨테이너에 마운트하고 `MCP_CONFIG_PATH`를 설정하세요.

## Usage Examples

### Python Client Usage

```python
from backend.mcp.mcp_client import get_context7_client, get_mcp_manager

# Using Context7 client
async def search_knowledge():
    client = get_context7_client()
    results = await client.search("ontology design patterns", limit=5)
    return results

# Using generic MCP manager
async def list_databases():
    manager = get_mcp_manager()
    result = await manager.call_tool(
        "terminusdb",
        "list_databases",
        {}
    )
    return result
```

### REST API Usage

```bash
# Search Context7
curl -X POST http://localhost:8002/api/v1/context7/search \
  -H "Content-Type: application/json" \
  -d '{"query": "ontology patterns", "limit": 10}'

# Add knowledge
curl -X POST http://localhost:8002/api/v1/context7/knowledge \
  -H "Content-Type: application/json" \
  -d '{
    "title": "MVCC Implementation",
    "content": "Multi-Version Concurrency Control implementation details...",
    "tags": ["database", "concurrency", "mvcc"]
  }'

# Analyze ontology
curl -X POST http://localhost:8002/api/v1/context7/analyze/ontology \
  -H "Content-Type: application/json" \
  -d '{
    "ontology_id": "Person",
    "db_name": "my_database",
    "include_suggestions": true
  }'
```

## Architecture

```
┌─────────────────────────────────────────────┐
│              BFF Service                     │
│        (FastAPI + Context7 Router)           │
└─────────────────┬───────────────────────────┘
                  │
         ┌────────▼────────┐
         │  MCP Client     │
         │    Manager      │
         └────────┬────────┘
                  │
    ┌─────────────┼─────────────┐
    │             │             │
┌───▼───┐    ┌───▼───┐    ┌───▼───┐
│Context7│    │Terminus│    │  FS   │
│ Server │    │  MCP   │    │ Server│
└────────┘    └────────┘    └────────┘
```

## Benefits

1. **Unified Interface**: Single protocol for multiple services
2. **AI Enhancement**: Context7 provides AI-powered knowledge management
3. **Extensibility**: Easy to add new MCP servers
4. **Standardization**: Following MCP standard ensures compatibility
5. **Decoupling**: Services communicate through well-defined protocol

## Troubleshooting

### Connection Issues
- Verify MCP servers are running: `ps aux | grep mcp`
- Check environment variables are set correctly
- Review logs: `tail -f logs/mcp_*.log`

### Context7 Issues
- Ensure API key is valid
- Check workspace name matches your Context7 account
- Verify network connectivity to Context7 servers

### TerminusDB MCP Issues
- Confirm TerminusDB is running: `curl http://localhost:6363`
- Check authentication credentials
- Review terminus_mcp_server.py logs

## Future Enhancements

- [ ] Add caching layer for Context7 responses
- [ ] Implement MCP server health monitoring
- [ ] Add support for more MCP servers
- [ ] Create MCP server for Elasticsearch operations
- [ ] Implement connection pooling for better performance
- [ ] Add metrics and observability

## References

- [MCP Specification](https://modelcontextprotocol.io/docs)
- [Context7 Documentation](https://context7.ai/docs)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
