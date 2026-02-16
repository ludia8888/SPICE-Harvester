"""
Compatibility shim for legacy imports.

Canonical MCP client implementation moved to `shared.services.mcp_client`
to avoid bff<->mcp_servers package cycles.
"""

from shared.services.mcp_client import (  # noqa: F401
    Context7Client,
    MCPClientManager,
    MCPServerConfig,
    get_context7_client,
    get_mcp_manager,
)
