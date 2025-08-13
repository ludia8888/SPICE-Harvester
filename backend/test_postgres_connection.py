#!/usr/bin/env python3
import asyncio
import asyncpg

async def test_connection():
    # Direct connection to Docker PostgreSQL
    conn = await asyncpg.connect(
        host='127.0.0.1',
        port=5432,
        user='admin',
        password='spice123!',
        database='spicedb'
    )
    
    # Test query
    result = await conn.fetchval("SELECT COUNT(*) FROM spice_outbox.outbox")
    print(f"✅ Connection successful! Outbox entries: {result}")
    
    await conn.close()

asyncio.run(test_connection())