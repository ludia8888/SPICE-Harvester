#!/usr/bin/env python3
"""
ULTRA DEBUG: Link Property Transformation Test
직접 OMS에 linkTarget으로 테스트하여 transformation 로직 검증
"""

import asyncio
import httpx

async def debug_transformation():
    print('🔍 ULTRA DEBUG: Transformation Verification')
    print('=' * 50)
    
    # Test the transformation directly with OMS
    team_data_transformed = {
        'id': 'Team',
        'label': 'Team', 
        'description': 'Work team',
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
                'linkTarget': 'Person',  # Use linkTarget directly
                'required': False,
                'label': 'Team Manager'
            }
        ]
    }
    
    async with httpx.AsyncClient() as client:
        # Create DB in OMS
        db_name = 'direct_transform_test'
        oms_db = await client.post('http://localhost:8000/api/v1/database/create',
                                 json={'name': db_name, 'description': 'Direct test'})
        print(f'OMS DB Create: {oms_db.status_code}')
        
        if oms_db.status_code in [200, 201]:
            # Test with correctly transformed data
            oms_team = await client.post(f'http://localhost:8000/api/v1/ontology/{db_name}/create',
                                       json=team_data_transformed)
            print(f'OMS Team Create (linkTarget): {oms_team.status_code}')
            
            if oms_team.status_code in [200, 201]:
                print('✅ Direct OMS creation with linkTarget works!')
            else:
                print(f'❌ Even direct OMS failed: {oms_team.text[:400]}')
                
                # Maybe there's another issue - try without some fields
                minimal_team = {
                    'id': 'MinimalTeam',
                    'label': 'Minimal Team',
                    'properties': [
                        {
                            'name': 'manager2',
                            'type': 'link', 
                            'linkTarget': 'Person',
                            'label': 'Manager 2'
                        }
                    ]
                }
                
                minimal_test = await client.post(f'http://localhost:8000/api/v1/ontology/{db_name}/create',
                                              json=minimal_team)
                print(f'Minimal Team Test: {minimal_test.status_code}')
                if minimal_test.status_code != 200:
                    print(f'Minimal failed too: {minimal_test.text[:300]}')
                    
        # Cleanup
        await client.delete(f'http://localhost:8000/api/v1/database/{db_name}')

if __name__ == "__main__":
    asyncio.run(debug_transformation())