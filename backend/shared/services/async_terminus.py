"""
Async TerminusDB Service for BFF
This is a lightweight version for BFF to connect to TerminusDB
"""

import httpx
import logging
from typing import Any, Dict, Optional
from shared.models.config import ConnectionConfig

logger = logging.getLogger(__name__)


class AsyncTerminusService:
    """Lightweight TerminusDB service for BFF"""
    
    def __init__(self, connection_info: ConnectionConfig):
        self.connection_info = connection_info
        self.client = None
        
    async def connect(self) -> None:
        """Establish connection to TerminusDB"""
        self.client = httpx.AsyncClient(
            base_url=self.connection_info.server_url,
            auth=(self.connection_info.user, self.connection_info.key),
            timeout=30.0
        )
        logger.info(f"Connected to TerminusDB at {self.connection_info.server_url}")
        
    async def ping(self) -> bool:
        """Check if TerminusDB is accessible"""
        try:
            if not self.client:
                await self.connect()
            response = await self.client.get("/api/info")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"TerminusDB ping failed: {e}")
            return False
            
    async def close(self) -> None:
        """Close the connection"""
        if self.client:
            await self.client.aclose()
            self.client = None