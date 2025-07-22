#!/usr/bin/env python3
"""Quick test to check if Order is a reserved word in TerminusDB"""

import httpx
import json
import asyncio

async def test_order_class():
    """Direct API test for Order class creation"""
    
    base_url = "http://localhost:6363"
    auth = ("admin", "admin")
    db_name = "test_order_reserved"
    
    async with httpx.AsyncClient(auth=auth) as client:
        # 1. Create database
        print("Creating database...")
        response = await client.post(
            f"{base_url}/api/db/admin/{db_name}",
            json={
                "label": "Test Order Reserved",
                "comment": "Testing if Order is reserved"
            }
        )
        print(f"Database creation: {response.status_code}")
        
        # 2. Try to create Order class directly
        print("\nCreating Order class...")
        order_schema = {
            "@type": "Class",
            "@id": "Order",
            "@key": {"@type": "Random"},
            "@documentation": {
                "@comment": "Test Order class"
            },
            "orderNumber": "xsd:string"
        }
        
        response = await client.post(
            f"{base_url}/api/document/admin/{db_name}",
            params={
                "graph_type": "schema",
                "author": "admin",
                "message": "Creating Order class"
            },
            json=[order_schema]
        )
        
        print(f"Order creation status: {response.status_code}")
        print(f"Response: {response.text[:500]}")
        
        if response.status_code != 200:
            print(f"\n❌ Failed to create Order class!")
            print(f"Full response: {response.text}")
        else:
            print(f"\n✅ Order class created successfully!")
        
        # 3. Try other potentially reserved names
        test_names = ["Query", "Transaction", "System", "Product", "CustomerOrder"]
        
        for name in test_names:
            print(f"\nTesting {name}...")
            test_schema = {
                "@type": "Class",
                "@id": name,
                "@key": {"@type": "Random"},
                "testProp": "xsd:string"
            }
            
            response = await client.post(
                f"{base_url}/api/document/admin/{db_name}",
                params={
                    "graph_type": "schema",
                    "author": "admin",
                    "message": f"Creating {name} class"
                },
                json=[test_schema]
            )
            
            if response.status_code == 200:
                print(f"  ✅ {name}: OK")
            else:
                print(f"  ❌ {name}: Failed ({response.status_code})")
        
        # 4. Cleanup
        print("\nCleaning up...")
        await client.delete(f"{base_url}/api/db/admin/{db_name}")
        print("Done!")

if __name__ == "__main__":
    asyncio.run(test_order_class())