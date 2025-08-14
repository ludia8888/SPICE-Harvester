#!/usr/bin/env python3
"""Test ontology creation issue"""

import asyncio
import aiohttp
import json
from datetime import datetime

async def test_ontology_creation():
    print('🔍 Testing ontology creation...')
    
    test_db = f'test_ontology_{datetime.now().strftime("%H%M%S")}'
    
    async with aiohttp.ClientSession() as session:
        # 1. Create test database
        print(f'Creating database: {test_db}')
        try:
            resp = await session.post(
                'http://localhost:8000/api/v1/database/create',
                json={'name': test_db, 'description': 'Test'}
            )
            print(f'  Database creation: {resp.status}')
            await asyncio.sleep(3)
        except Exception as e:
            print(f'  ❌ Database creation failed: {e}')
            return
        
        # 2. Create simple ontology without relationships
        print('Creating simple ontology without relationships...')
        simple_ontology = {
            'id': 'TestClass',
            'label': 'Test Class',
            'description': 'Simple test',
            'properties': [
                {'name': 'test_id', 'type': 'string', 'required': True},
                {'name': 'test_name', 'type': 'string', 'required': False}
            ]
        }
        
        try:
            resp = await session.post(
                f'http://localhost:8000/api/v1/database/{test_db}/ontology',
                json=simple_ontology,
                timeout=aiohttp.ClientTimeout(total=10)
            )
            print(f'  Simple ontology creation: {resp.status}')
            if resp.status != 200:
                text = await resp.text()
                print(f'  Response: {text[:200]}')
        except asyncio.TimeoutError:
            print('  ❌ Timeout creating simple ontology')
        except Exception as e:
            print(f'  ❌ Error: {e}')
        
        # 3. Create ontology with relationships
        print('Creating ontology with relationships...')
        complex_ontology = {
            'id': 'TestProduct',
            'label': 'Test Product',
            'description': 'Test with relationship',
            'properties': [
                {'name': 'product_id', 'type': 'string', 'required': True}
            ],
            'relationships': [
                {
                    'predicate': 'test_owned_by',
                    'label': 'Owned By',
                    'target': 'TestClass',
                    'cardinality': 'n:1'
                }
            ]
        }
        
        try:
            resp = await session.post(
                f'http://localhost:8000/api/v1/database/{test_db}/ontology',
                json=complex_ontology,
                timeout=aiohttp.ClientTimeout(total=10)
            )
            print(f'  Complex ontology creation: {resp.status}')
            if resp.status != 200:
                text = await resp.text()
                print(f'  Response: {text[:200]}')
        except asyncio.TimeoutError:
            print('  ❌ Timeout creating complex ontology')
        except Exception as e:
            print(f'  ❌ Error: {e}')
        
        # 4. Clean up
        print(f'Cleaning up {test_db}...')
        try:
            resp = await session.delete(f'http://localhost:8000/api/v1/database/{test_db}')
            print(f'  Cleanup: {resp.status}')
        except:
            pass

if __name__ == "__main__":
    asyncio.run(test_ontology_creation())