#!/usr/bin/env python3
"""
ULTRA DEBUG: Link Dependency Test
먼저 Person 클래스를 생성한 후 Team의 link property가 작동하는지 테스트
"""

import asyncio
import httpx

async def debug_link_dependency():
    print('🔍 ULTRA DEBUG: Link Dependency Test')
    print('=' * 50)
    
    async with httpx.AsyncClient() as client:
        # Create DB in OMS
        db_name = 'link_dependency_test'
        oms_db = await client.post('http://localhost:8000/api/v1/database/create',
                                 json={'name': db_name, 'description': 'Link dependency test'})
        print(f'OMS DB Create: {oms_db.status_code}')
        
        if oms_db.status_code in [200, 201]:
            
            # Step 1: Create Person class first
            print('\n👤 Step 1: Create Person class')
            person_data = {
                'id': 'Person',
                'label': 'Person', 
                'description': 'A person',
                'properties': [
                    {
                        'name': 'name',
                        'type': 'string',
                        'required': True,
                        'label': 'Person Name'
                    },
                    {
                        'name': 'age',
                        'type': 'integer',
                        'required': False,
                        'label': 'Age'
                    }
                ]
            }
            
            person_create = await client.post(f'http://localhost:8000/api/v1/ontology/{db_name}/create',
                                            json=person_data)
            print(f'   Person Create: {person_create.status_code}')
            
            if person_create.status_code in [200, 201]:
                print('   ✅ Person class created successfully')
                
                # Step 2: Now create Team with link to Person
                print('\n👥 Step 2: Create Team class with link to Person')
                team_data = {
                    'id': 'Team',
                    'label': 'Team',
                    'description': 'A work team',
                    'properties': [
                        {
                            'name': 'name',
                            'type': 'string',
                            'required': True,
                            'label': 'Team Name'
                        },
                        {
                            'name': 'manager',
                            'type': 'link',
                            'linkTarget': 'Person',  # Now Person exists
                            'required': False,
                            'label': 'Team Manager'
                        }
                    ]
                }
                
                team_create = await client.post(f'http://localhost:8000/api/v1/ontology/{db_name}/create',
                                              json=team_data)
                print(f'   Team Create: {team_create.status_code}')
                
                if team_create.status_code in [200, 201]:
                    print('   🎉 SUCCESS! Team with link property created!')
                else:
                    print(f'   ❌ Team creation still failed: {team_create.text[:400]}')
                    
            else:
                print(f'   ❌ Person creation failed: {person_create.text[:300]}')
                
        # Cleanup
        await client.delete(f'http://localhost:8000/api/v1/database/{db_name}')

if __name__ == "__main__":
    asyncio.run(debug_link_dependency())