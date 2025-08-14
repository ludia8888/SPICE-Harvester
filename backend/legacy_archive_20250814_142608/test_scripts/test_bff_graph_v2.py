#!/usr/bin/env python3
"""
Test BFF Graph Endpoints with GraphFederationServiceV2
THINK ULTRA³ - Production Ready Implementation
"""

import asyncio
import aiohttp
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_bff_graph():
    """Test BFF graph endpoints"""
    bff_url = 'http://localhost:8002'
    db_name = 'graph_federation_test'
    
    logger.info('🔥 Testing BFF Graph Endpoint with GraphFederationServiceV2')
    logger.info('=' * 60)
    
    async with aiohttp.ClientSession() as session:
        # 1. Health check
        logger.info('\n1️⃣ Testing health endpoint...')
        try:
            async with session.get(f'{bff_url}/api/v1/graph-query/health', timeout=5) as resp:
                if resp.status == 200:
                    health = await resp.json()
                    logger.info(f'   ✅ Health: {health}')
                else:
                    logger.info(f'   ❌ Health check failed: {resp.status}')
        except Exception as e:
            logger.info(f'   ❌ BFF not running or health endpoint error: {e}')
            return
        
        # 2. Simple query
        logger.info('\n2️⃣ Testing simple query (all Products)...')
        simple_query = {
            'class_name': 'Product',
            'limit': 10
        }
        
        async with session.post(
            f'{bff_url}/api/v1/graph-query/{db_name}/simple',
            json=simple_query
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                logger.info(f'   ✅ Found {result.get("count", 0)} products')
                docs = result.get('documents', [])
                for doc in docs[:2]:
                    logger.info(f'      - {doc.get("name")} (${doc.get("price")})')
            else:
                error = await resp.text()
                logger.info(f'   ❌ Simple query failed: {resp.status}')
                logger.info(f'   Error: {error[:200]}')
        
        # 3. Multi-hop query
        logger.info('\n3️⃣ Testing multi-hop query (Product -> Client)...')
        multi_hop_query = {
            'start_class': 'Product',
            'hops': [
                {'predicate': 'owned_by', 'target_class': 'Client'}
            ],
            'filters': {'product_id': 'PROD001'},
            'limit': 10
        }
        
        async with session.post(
            f'{bff_url}/api/v1/graph-query/{db_name}',
            json=multi_hop_query
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                logger.info(f'   ✅ Multi-hop success: {result.get("count", 0)} nodes')
                
                # Show edges
                for edge in result.get('edges', []):
                    logger.info(f'      Edge: {edge["from_node"]} --{edge["predicate"]}--> {edge["to_node"]}')
                
                # Show nodes
                for node in result.get('nodes', [])[:3]:
                    logger.info(f'      Node: {node["id"]} ({node["type"]})')
                    if node.get('data'):
                        logger.info(f'        Name: {node["data"].get("name")}')
            else:
                error = await resp.text()
                logger.info(f'   ❌ Multi-hop query failed: {resp.status}')
                logger.info(f'   Error: {error[:200]}')
        
        # 4. Path finding
        logger.info('\n4️⃣ Testing path finding (Product -> Client)...')
        async with session.get(
            f'{bff_url}/api/v1/graph-query/{db_name}/paths',
            params={
                'source_class': 'Product',
                'target_class': 'Client',
                'max_depth': 3
            }
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                logger.info(f'   ✅ Found {result.get("count", 0)} paths')
                for i, path in enumerate(result.get('paths', []), 1):
                    logger.info(f'      Path {i}: {path}')
            else:
                error = await resp.text()
                logger.info(f'   ❌ Path finding failed: {resp.status}')
                logger.info(f'   Error: {error[:200]}')
    
    logger.info('\n✅ BFF Graph endpoint test complete!')


async def main():
    """Main execution"""
    await test_bff_graph()


if __name__ == "__main__":
    asyncio.run(main())