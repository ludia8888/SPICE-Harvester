#!/usr/bin/env python3
"""
TerminusDB MCP Server
Provides TerminusDB capabilities through Model Context Protocol
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from mcp import Server
from mcp.server import Request, Response
from mcp.server.stdio import stdio_server
from pydantic import BaseModel

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig
from shared.models.ontology import OntologyBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TerminusDBMCPServer:
    """
    MCP Server for TerminusDB operations
    Exposes TerminusDB functionality through standardized MCP interface
    """
    
    def __init__(self):
        self.server = Server("terminus-mcp-server")
        self.terminus_service: Optional[AsyncTerminusService] = None
        self._setup_handlers()
        
    def _setup_handlers(self):
        """Setup MCP request handlers"""
        
        @self.server.list_tools()
        async def list_tools() -> List[Dict[str, Any]]:
            """List available TerminusDB tools"""
            return [
                {
                    "name": "create_database",
                    "description": "Create a new TerminusDB database",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"},
                            "description": {"type": "string"}
                        },
                        "required": ["db_name"]
                    }
                },
                {
                    "name": "list_databases",
                    "description": "List all available databases",
                    "inputSchema": {
                        "type": "object",
                        "properties": {}
                    }
                },
                {
                    "name": "create_ontology",
                    "description": "Create an ontology class",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"},
                            "class_id": {"type": "string"},
                            "label": {"type": "string"},
                            "description": {"type": "string"},
                            "properties": {"type": "array"},
                            "relationships": {"type": "array"}
                        },
                        "required": ["db_name", "class_id", "label"]
                    }
                },
                {
                    "name": "get_ontology",
                    "description": "Get ontology information",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"},
                            "class_id": {"type": "string"}
                        },
                        "required": ["db_name"]
                    }
                },
                {
                    "name": "list_branches",
                    "description": "List branches in a database",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"}
                        },
                        "required": ["db_name"]
                    }
                },
                {
                    "name": "create_branch",
                    "description": "Create a new branch",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"},
                            "branch_name": {"type": "string"},
                            "from_branch": {"type": "string"}
                        },
                        "required": ["db_name", "branch_name"]
                    }
                },
                {
                    "name": "execute_query",
                    "description": "Execute a WOQL query",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "db_name": {"type": "string"},
                            "query": {"type": "object"}
                        },
                        "required": ["db_name", "query"]
                    }
                }
            ]
        
        @self.server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> Any:
            """Execute a TerminusDB tool"""
            
            if not self.terminus_service:
                await self._connect_terminus()
            
            try:
                if name == "create_database":
                    result = await self.terminus_service.create_database(
                        arguments["db_name"],
                        arguments.get("description", "")
                    )
                    return {"success": result, "db_name": arguments["db_name"]}
                    
                elif name == "list_databases":
                    databases = await self.terminus_service.list_databases()
                    return {"databases": databases}
                    
                elif name == "create_ontology":
                    ontology = OntologyBase(
                        id=arguments["class_id"],
                        label=arguments["label"],
                        description=arguments.get("description"),
                        properties=arguments.get("properties", []),
                        relationships=arguments.get("relationships", [])
                    )
                    result = await self.terminus_service.create_ontology(
                        arguments["db_name"],
                        ontology
                    )
                    return {"success": True, "ontology": result.dict()}
                    
                elif name == "get_ontology":
                    result = await self.terminus_service.get_ontology(
                        arguments["db_name"],
                        arguments.get("class_id")
                    )
                    return {"ontologies": [o.dict() for o in result]}
                    
                elif name == "list_branches":
                    branches = await self.terminus_service.list_branches(
                        arguments["db_name"]
                    )
                    return {"branches": branches}
                    
                elif name == "create_branch":
                    result = await self.terminus_service.create_branch(
                        arguments["db_name"],
                        arguments["branch_name"],
                        arguments.get("from_branch", "main")
                    )
                    return {"success": result, "branch": arguments["branch_name"]}
                    
                elif name == "execute_query":
                    result = await self.terminus_service.execute_query(
                        arguments["db_name"],
                        arguments["query"]
                    )
                    return {"results": result}
                    
                else:
                    return {"error": f"Unknown tool: {name}"}
                    
            except Exception as e:
                logger.error(f"Error executing tool {name}: {e}")
                return {"error": str(e)}
        
        @self.server.list_resources()
        async def list_resources() -> List[Dict[str, Any]]:
            """List available TerminusDB resources"""
            
            if not self.terminus_service:
                await self._connect_terminus()
            
            try:
                databases = await self.terminus_service.list_databases()
                resources = []
                
                for db in databases:
                    resources.append({
                        "uri": f"terminus://database/{db.get('name', db)}",
                        "name": db.get('name', db),
                        "description": f"TerminusDB database: {db.get('name', db)}",
                        "mimeType": "application/terminus+json"
                    })
                
                return resources
                
            except Exception as e:
                logger.error(f"Error listing resources: {e}")
                return []
        
        @self.server.read_resource()
        async def read_resource(uri: str) -> str:
            """Read a TerminusDB resource"""
            
            if not self.terminus_service:
                await self._connect_terminus()
            
            try:
                # Parse URI: terminus://database/{db_name}
                if uri.startswith("terminus://database/"):
                    db_name = uri.replace("terminus://database/", "")
                    
                    # Get database info and ontologies
                    ontologies = await self.terminus_service.get_ontology(db_name)
                    branches = await self.terminus_service.list_branches(db_name)
                    
                    return json.dumps({
                        "database": db_name,
                        "branches": branches,
                        "ontologies": [o.dict() for o in ontologies],
                        "ontology_count": len(ontologies)
                    }, indent=2)
                    
                return json.dumps({"error": f"Unknown resource: {uri}"})
                
            except Exception as e:
                logger.error(f"Error reading resource {uri}: {e}")
                return json.dumps({"error": str(e)})
    
    async def _connect_terminus(self):
        """Connect to TerminusDB"""
        connection_info = ConnectionConfig(
            server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
            user=os.getenv("TERMINUS_USER", "admin"),
            account=os.getenv("TERMINUS_ACCOUNT", "admin"),
            key=os.getenv("TERMINUS_KEY", "admin123")
        )
        
        self.terminus_service = AsyncTerminusService(connection_info)
        await self.terminus_service.connect()
        logger.info("Connected to TerminusDB")
    
    async def run(self):
        """Run the MCP server"""
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(read_stream, write_stream)


async def main():
    """Main entry point"""
    server = TerminusDBMCPServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(main())