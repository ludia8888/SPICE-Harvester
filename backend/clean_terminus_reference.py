#!/usr/bin/env python3
"""
ULTRA FIX: Clean TerminusDB Stale Reference
stale descriptor path를 정리하여 database list 조회 문제 해결
"""

import asyncio
import httpx
import json
import os
import base64

from shared.config.service_config import ServiceConfig
from shared.models.config import ConnectionConfig

async def clean_stale_reference():
    print('🧹 ULTRA CLEAN: TerminusDB Stale Reference Cleanup')
    print('=' * 60)
    
    # Get connection config
    connection_info = ConnectionConfig(
        server_url=ServiceConfig.get_terminus_url(),
        user=os.getenv('TERMINUS_USER', 'admin'),
        account=os.getenv('TERMINUS_ACCOUNT', 'admin'), 
        key=os.getenv('TERMINUS_KEY', 'admin123'),
    )
    
    # Problem reference
    bad_reference = "admin/advanced_version_test_1753087860/tag/v1.0.0"
    problem_db_name = "advanced_version_test_1753087860"
    
    print(f'🎯 Target stale reference: {bad_reference}')
    print(f'🎯 Problem database name: {problem_db_name}')
    
    async with httpx.AsyncClient() as client:
        # Create Basic Auth token
        print('\n🔑 Step 1: Create Basic Auth token')
        auth_string = f"{connection_info.user}:{connection_info.key}"
        auth_bytes = auth_string.encode('ascii')
        auth_token = base64.b64encode(auth_bytes).decode('ascii')
        headers = {'Authorization': f'Basic {auth_token}'}
        print('   ✅ Auth token created')
        
        # Try to delete the problematic database directly
        print(f'\n🗑️  Step 2: Try to delete problematic database: {problem_db_name}')
        delete_url = f'{connection_info.server_url}/api/db/{connection_info.account}/{problem_db_name}'
        
        try:
            delete_resp = await client.delete(delete_url, headers=headers)
            print(f'   Delete status: {delete_resp.status_code}')
            print(f'   Delete response: {delete_resp.text[:300]}')
            
            if delete_resp.status_code == 200:
                print('   ✅ Successfully deleted problematic database')
            elif delete_resp.status_code == 404:
                print('   ℹ️  Database not found (expected)')
            else:
                print(f'   ⚠️  Unexpected response: {delete_resp.text}')
                
        except Exception as e:
            print(f'   Exception during delete: {e}')
        
        # Try to delete any tags or branches associated with it
        print(f'\n🏷️  Step 3: Try to clean up tags/branches for: {problem_db_name}')
        tag_delete_url = f'{connection_info.server_url}/api/db/{connection_info.account}/{problem_db_name}/tag/v1.0.0'
        
        try:
            tag_delete_resp = await client.delete(tag_delete_url, headers=headers)
            print(f'   Tag delete status: {tag_delete_resp.status_code}')
            print(f'   Tag delete response: {tag_delete_resp.text[:200]}')
        except Exception as e:
            print(f'   Tag delete exception: {e}')
        
        # Now test if database list works
        print('\n📋 Step 4: Test if database list is fixed')
        db_list_url = f'{connection_info.server_url}/api/db/{connection_info.account}'
        
        try:
            list_resp = await client.get(db_list_url, headers=headers)
            print(f'   List status: {list_resp.status_code}')
            
            if list_resp.status_code == 200:
                dbs = list_resp.json()
                print(f'   🎉 SUCCESS! Database list working!')
                print(f'   Available databases: {dbs}')
                return True
            else:
                print(f'   ❌ Still failing: {list_resp.text[:200]}')
                return False
                
        except Exception as e:
            print(f'   List test exception: {e}')
            return False

if __name__ == "__main__":
    result = asyncio.run(clean_stale_reference())
    if result:
        print('\n✅ CLEANUP SUCCESSFUL! Database list should work now.')
    else:
        print('\n❌ CLEANUP INCOMPLETE. May need more drastic measures.')