"""
MCP Client for integrating with various MCP servers
Provides unified interface for Context7 and other MCP services
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

import httpx
from mcp import Client
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
    
    def __init__(self, config_path: str = "mcp-config.json"):
        self.config_path = config_path
        self.servers: Dict[str, MCPServerConfig] = {}
        self.clients: Dict[str, Client] = {}
        self._load_config()
        
    def _load_config(self):
        """Load MCP configuration from file"""
        try:
            with open(self.config_path, 'r') as f:
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
    
    async def connect_server(self, server_name: str) -> Optional[Client]:
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
        
        try:
            # Prepare environment variables
            env = os.environ.copy()
            if server_config.env:
                for key, value in server_config.env.items():
                    # Expand environment variables
                    env[key] = os.path.expandvars(value)
            
            # Create client and connect
            async with stdio_client(
                server_config.command,
                *server_config.args,
                env=env
            ) as (read_stream, write_stream):
                client = Client(f"{server_name}-client", {})
                await client.connect(read_stream, write_stream)
                
                self.clients[server_name] = client
                logger.info(f"Connected to MCP server: {server_name}")
                
                return client
                
        except Exception as e:
            logger.error(f"Failed to connect to {server_name}: {e}")
            return None
    
    async def disconnect_server(self, server_name: str):
        """Disconnect from a specific MCP server"""
        if server_name in self.clients:
            # MCP clients handle cleanup in context manager
            del self.clients[server_name]
            logger.info(f"Disconnected from MCP server: {server_name}")
    
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
        
    async def search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search Context7 knowledge base
        
        Args:
            query: Search query
            limit: Maximum results
            
        Returns:
            Search results
        """
        return await self.mcp_manager.call_tool(
            self.server_name,
            "search",
            {"query": query, "limit": limit}
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
        relationship: str
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
        return await self.mcp_manager.call_tool(
            self.server_name,
            "link_entities",
            {
                "source_id": source_id,
                "target_id": target_id,
                "relationship": relationship
            }
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