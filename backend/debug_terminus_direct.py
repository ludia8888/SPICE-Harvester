#!/usr/bin/env python3
"""
ULTRA DEBUG: Direct TerminusDB Investigation
직접 TerminusDB에 접근하여 데이터베이스 생성/조회 문제의 근본 원인 파악
"""

import asyncio
import httpx
import json
from shared.config.settings import get_settings
from shared.models.config import ConnectionConfig

async def debug_terminusdb_directly():
    print('🔍 ULTRA DEBUG: Direct TerminusDB Investigation')
    print('=' * 60)
    
    # Get connection config
    cfg = get_settings().database
    connection_info = ConnectionConfig(
        server_url=cfg.terminus_url.rstrip("/"),
        user=cfg.terminus_user,
        account=cfg.terminus_account,
        key=cfg.terminus_password,
    )
    
    print(f'📋 TerminusDB Config:')
    print(f'   Server: {connection_info.server_url}')
    print(f'   Account: {connection_info.account}')
    print(f'   User: {connection_info.user}')
    
    async with httpx.AsyncClient() as client:
        # Step 1: Create Basic Auth token (same as OMS)
        print('\n🔑 Step 1: Create Basic Auth token for TerminusDB')
        import base64
        
        try:
            # Create Basic Auth token like OMS does
            auth_string = f"{connection_info.user}:{connection_info.key}"
            auth_bytes = auth_string.encode('ascii')
            auth_token = base64.b64encode(auth_bytes).decode('ascii')
            headers = {'Authorization': f'Basic {auth_token}'}
            print('   ✅ Basic Auth token created')
        except Exception as e:
            print(f'   ❌ Auth creation exception: {e}')
            return
        
        # Step 2: Check existing databases directly
        print('\n📋 Step 2: Check existing databases in TerminusDB')
        db_list_url = f'{connection_info.server_url}/api/db/{connection_info.account}'
        try:
            db_resp = await client.get(db_list_url, headers=headers)
            print(f'   Database list status: {db_resp.status_code}')
            
            if db_resp.status_code == 200:
                try:
                    dbs = db_resp.json()
                    print(f'   TerminusDB databases: {dbs}')
                    print(f'   Database count: {len(dbs) if isinstance(dbs, list) else "not a list"}')
                except json.JSONDecodeError:
                    print(f'   Raw response: {db_resp.text[:300]}')
            else:
                error_text = db_resp.text
                print(f'   ❌ Database list failed: {error_text[:500]}')
                if 'bad descriptor path' in error_text.lower():
                    print('   🚨 BAD DESCRIPTOR PATH ERROR CONFIRMED!')
                    print('   This explains why databases do not appear in lists!')
        except Exception as e:
            print(f'   ❌ Database list exception: {e}')
        
        # Step 3: Try to create database directly in TerminusDB
        print('\n🔨 Step 3: Create database directly in TerminusDB')
        test_db_name = 'direct_test_db'
        create_url = f'{connection_info.server_url}/api/db/{connection_info.account}/{test_db_name}'
        create_data = {'comment': 'Direct creation test', 'label': 'Direct Test DB'}
        
        try:
            create_resp = await client.post(create_url, json=create_data, headers=headers)
            print(f'   Direct create status: {create_resp.status_code}')
            print(f'   Direct create response: {create_resp.text[:300]}')
            
            if create_resp.status_code in [200, 201]:
                print('   ✅ Direct creation successful')
                
                # Step 4: Check if it appears in list
                print('\n📋 Step 4: Check if directly created DB appears in list')
                try:
                    db_resp2 = await client.get(db_list_url, headers=headers) 
                    print(f'   List status after create: {db_resp2.status_code}')
                    
                    if db_resp2.status_code == 200:
                        dbs2 = db_resp2.json()
                        print(f'   Updated database list: {dbs2}')
                        found = any('direct_test_db' in str(db) for db in (dbs2 if isinstance(dbs2, list) else [dbs2]))
                        print(f'   Direct DB found in list: {found}')
                    else:
                        print(f'   ❌ List still failing: {db_resp2.text[:200]}')
                except Exception as e:
                    print(f'   ❌ List check exception: {e}')
                
                # Cleanup
                try:
                    delete_resp = await client.delete(create_url, headers=headers)
                    print(f'   Delete status: {delete_resp.status_code}')
                except Exception as e:
                    print(f'   Cleanup exception: {e}')
            else:
                print(f'   ❌ Direct creation failed: {create_resp.text}')
        except Exception as e:
            print(f'   ❌ Creation exception: {e}')

if __name__ == "__main__":
    asyncio.run(debug_terminusdb_directly())
