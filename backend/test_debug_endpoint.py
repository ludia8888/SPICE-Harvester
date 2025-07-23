#!/usr/bin/env python3
"""
Test debug endpoint
"""

import asyncio
import httpx

async def test_debug():
    """Test debug endpoint"""
    
    async with httpx.AsyncClient() as client:
        # Test debug endpoint
        resp = await client.post(
            "http://localhost:8002/debug/test",
            json={"test": "data", "foo": "bar"}
        )
        print(f"Debug endpoint status: {resp.status_code}")
        print(f"Response: {resp.json()}")


if __name__ == "__main__":
    asyncio.run(test_debug())