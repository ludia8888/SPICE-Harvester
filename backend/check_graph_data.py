#!/usr/bin/env python3
"""
Check if graph federation test data exists
"""

import httpx
import json
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def check_data():
    auth = ('admin', 'admin')
    db_name = 'graph_federation_test'
    
    logger.info("🔍 Checking Graph Federation Test Data")
    logger.info("=" * 60)
    
    async with httpx.AsyncClient() as client:
        # Check if database exists
        db_resp = await client.get(
            f'http://localhost:6363/api/db/admin/{db_name}',
            auth=auth
        )
        if db_resp.status_code != 200:
            logger.error(f'❌ Database {db_name} not found')
            # Try to list all databases
            list_resp = await client.get(
                'http://localhost:6363/api/db/admin',
                auth=auth
            )
            if list_resp.status_code == 200:
                dbs = list_resp.json()
                logger.info("Available databases:")
                for db in dbs:
                    logger.info(f"  - {db['name']}")
            return
        
        logger.info(f'✅ Database {db_name} exists')
        
        # Get all instance documents
        resp = await client.get(
            f'http://localhost:6363/api/document/admin/{db_name}',
            params={'graph_type': 'instance'},
            auth=auth
        )
        
        if resp.status_code == 200:
            docs = []
            for line in resp.text.strip().split('\n'):
                if line:
                    docs.append(json.loads(line))
            
            logger.info(f'\nFound {len(docs)} documents in TerminusDB:')
            
            by_type = {}
            for doc in docs:
                doc_type = doc.get('@type')
                if doc_type not in by_type:
                    by_type[doc_type] = []
                by_type[doc_type].append(doc)
            
            for doc_type, type_docs in by_type.items():
                logger.info(f'\n  {doc_type}: {len(type_docs)} instances')
                for doc in type_docs[:2]:
                    logger.info(f'    - {doc.get("@id")}')
                    logger.info(f'      es_doc_id: {doc.get("es_doc_id")}')
                    if doc.get('owned_by'):
                        logger.info(f'      owned_by: {doc.get("owned_by")}')
                    if doc.get('ordered_by'):
                        logger.info(f'      ordered_by: {doc.get("ordered_by")}')
        else:
            logger.error(f'❌ Failed to get documents: {resp.status_code}')
    
    # Also check Elasticsearch
    logger.info("\n" + "=" * 60)
    logger.info("Checking Elasticsearch data...")
    
    import aiohttp
    index_name = f"instances_{db_name.replace('-', '_')}"
    
    async with aiohttp.ClientSession() as session:
        query = {"query": {"match_all": {}}, "size": 100}
        async with session.post(
            f"http://localhost:9201/{index_name}/_search",
            json=query
        ) as resp:
            if resp.status == 200:
                result = await resp.json()
                hits = result["hits"]["hits"]
                logger.info(f'Found {len(hits)} documents in Elasticsearch')
                
                by_type = {}
                for hit in hits:
                    doc = hit["_source"]
                    doc_type = doc.get("@type") or doc.get("class_id")
                    if doc_type not in by_type:
                        by_type[doc_type] = []
                    by_type[doc_type].append(hit["_id"])
                
                for doc_type, ids in by_type.items():
                    logger.info(f'  {doc_type}: {len(ids)} instances')
            else:
                logger.error(f'Failed to query Elasticsearch: {resp.status}')


asyncio.run(check_data())