"""
MCP Client for integrating with various MCP servers
Provides unified interface for Context7 and other MCP services
"""

import asyncio
import json
import logging
import os
from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client

logger = logging.getLogger(__name__)


@dataclass
class MCPServerConfig:
    """Configuration for an MCP server"""
    name: str
    command: str
    args: List[str]
    env: Dict[str, str] = None
    config: Dict[str, Any] = None


class MCPClientManager:
    """
    Manager for multiple MCP client connections
    Handles Context7 and other MCP servers
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = self._resolve_config_path(config_path)
        self.servers: Dict[str, MCPServerConfig] = {}
        self.clients: Dict[str, ClientSession] = {}
        self._sessions: Dict[str, AsyncExitStack] = {}
        self._load_config()
        
    @staticmethod
    def _resolve_config_path(config_path: Optional[str]) -> str:
        candidates: List[Path] = []
        if config_path:
            candidates.append(Path(config_path))

        env_path = (os.getenv("MCP_CONFIG_PATH") or "").strip()
        if env_path:
            candidates.append(Path(env_path))

        candidates.append(Path.cwd() / "mcp-config.json")
        candidates.append(Path(__file__).resolve().parents[2] / "mcp-config.json")

        for candidate in candidates:
            if candidate.exists():
                return str(candidate)

        return str(candidates[0]) if candidates else "mcp-config.json"

    def _load_config(self):
        """Load MCP configuration from file"""
        try:
            config_path = Path(self.config_path)
            if not config_path.exists():
                logger.error(
                    "MCP config not found: %s (set MCP_CONFIG_PATH to override)",
                    self.config_path,
                )
                return

            with config_path.open("r", encoding="utf-8") as f:
                config = json.load(f)
                
            for name, server_config in config.get("mcpServers", {}).items():
                self.servers[name] = MCPServerConfig(
                    name=name,
                    command=server_config["command"],
                    args=server_config["args"],
                    env=server_config.get("env", {}),
                    config=server_config.get("config", {})
                )
                
            logger.info(f"Loaded {len(self.servers)} MCP server configurations")
            
        except Exception as e:
            logger.error(f"Failed to load MCP config: {e}")
    
    async def connect_server(self, server_name: str) -> Optional[ClientSession]:
        """
        Connect to a specific MCP server
        
        Args:
            server_name: Name of the server to connect to
            
        Returns:
            Connected MCP client or None
        """
        if server_name not in self.servers:
            logger.error(f"Server {server_name} not found in configuration")
            return None
            
        if server_name in self.clients:
            return self.clients[server_name]
            
        server_config = self.servers[server_name]
        
        stack = AsyncExitStack()
        try:
            # Prepare environment variables
            env = os.environ.copy()
            if server_config.env:
                for key, value in server_config.env.items():
                    # Expand environment variables
                    env[key] = os.path.expandvars(value)
            
            # Create client and connect
            read_stream, write_stream = await stack.enter_async_context(
                stdio_client(
                    server_config.command,
                    *server_config.args,
                    env=env,
                )
            )
            client = ClientSession(read_stream, write_stream)
            await stack.enter_async_context(client)
            await client.initialize()

            self.clients[server_name] = client
            self._sessions[server_name] = stack
            logger.info("Connected to MCP server: %s", server_name)

            return client
                
        except Exception as e:
            logger.error(f"Failed to connect to {server_name}: {e}")
            await stack.aclose()
            return None
    
    async def disconnect_server(self, server_name: str):
        """Disconnect from a specific MCP server"""
        stack = self._sessions.pop(server_name, None)
        if stack:
            await stack.aclose()
        if server_name in self.clients:
            del self.clients[server_name]
        logger.info("Disconnected from MCP server: %s", server_name)
    
    async def call_tool(
        self,
        server_name: str,
        tool_name: str,
        arguments: Dict[str, Any]
    ) -> Any:
        """
        Call a tool on a specific MCP server
        
        Args:
            server_name: Name of the server
            tool_name: Name of the tool to call
            arguments: Tool arguments
            
        Returns:
            Tool execution result
        """
        client = await self.connect_server(server_name)
        if not client:
            raise ConnectionError(f"Could not connect to {server_name}")
            
        try:
            result = await client.call_tool(tool_name, arguments)
            return result
        except Exception as e:
            logger.error(f"Error calling tool {tool_name} on {server_name}: {e}")
            raise
    
    async def list_tools(self, server_name: str) -> List[Dict[str, Any]]:
        """List available tools from a server"""
        client = await self.connect_server(server_name)
        if not client:
            raise ConnectionError(f"Could not connect to {server_name}")
            
        try:
            tools = await client.list_tools()
            return tools
        except Exception as e:
            logger.error(f"Error listing tools from {server_name}: {e}")
            raise


class Context7Client:
    """
    Specialized client for Context7 MCP server
    Provides high-level interface for Context7 operations
    """
    
    def __init__(self, mcp_manager: MCPClientManager):
        self.mcp_manager = mcp_manager
        self.server_name = "context7"
        
    async def search(
        self,
        query: str,
        limit: int = 10,
        *,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search Context7 knowledge base
        
        Args:
            query: Search query
            limit: Maximum results
            filters: Optional server-side filters
            
        Returns:
            Search results
        """
        payload = {"query": query, "limit": limit}
        if filters:
            payload["filters"] = filters

        return await self.mcp_manager.call_tool(
            self.server_name,
            "search",
            payload,
        )
    
    async def get_context(self, entity_id: str) -> Dict[str, Any]:
        """
        Get context for a specific entity
        
        Args:
            entity_id: Entity identifier
            
        Returns:
            Entity context information
        """
        return await self.mcp_manager.call_tool(
            self.server_name,
            "get_context",
            {"entity_id": entity_id}
        )
    
    async def add_knowledge(
        self,
        title: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Add knowledge to Context7
        
        Args:
            title: Knowledge title
            content: Knowledge content
            metadata: Additional metadata
            
        Returns:
            Creation result
        """
        return await self.mcp_manager.call_tool(
            self.server_name,
            "add_knowledge",
            {
                "title": title,
                "content": content,
                "metadata": metadata or {}
            }
        )
    
    async def link_entities(
        self,
        source_id: str,
        target_id: str,
        relationship: str,
        *,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create relationship between entities
        
        Args:
            source_id: Source entity ID
            target_id: Target entity ID
            relationship: Relationship type
            
        Returns:
            Link creation result
        """
        payload = {
            "source_id": source_id,
            "target_id": target_id,
            "relationship": relationship,
        }
        if properties:
            payload["properties"] = properties

        return await self.mcp_manager.call_tool(
            self.server_name,
            "link_entities",
            payload,
        )
    
    async def analyze_ontology(self, ontology_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze ontology with Context7
        
        Args:
            ontology_data: Ontology to analyze
            
        Returns:
            Analysis results
        """
        return await self.mcp_manager.call_tool(
            self.server_name,
            "analyze_ontology",
            {"ontology": ontology_data}
        )


# Singleton instance
_mcp_manager: Optional[MCPClientManager] = None


def get_mcp_manager() -> MCPClientManager:
    """Get or create MCP manager singleton"""
    global _mcp_manager
    if _mcp_manager is None:
        _mcp_manager = MCPClientManager()
    return _mcp_manager


def get_context7_client() -> Context7Client:
    """Get Context7 client"""
    return Context7Client(get_mcp_manager())
