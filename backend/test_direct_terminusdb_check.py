#!/usr/bin/env python3
"""
🔥 ULTRA: Direct TerminusDB API check
Test if the ontology actually exists in TerminusDB directly
"""

import asyncio
import httpx
import json
from datetime import datetime

async def test_direct_terminusdb():
    """Test direct TerminusDB API to see if ontology exists"""
    
    # Use the database from the recent test
    test_db = "test_es_perfect_20250809_204526"  # The test that showed "Command processed successfully"
    test_class = "PerfectTestProduct"
    
    TERMINUS_URL = "http://localhost:6363"
    AUTH = ("admin", "admin123")
    
    print("=" * 80)
    print("🔥 TESTING DIRECT TERMINUSDB API")
    print(f"Database: {test_db}")
    print(f"Class: {test_class}")
    print("=" * 80)
    
    async with httpx.AsyncClient() as client:
        
        # 1. Check if database exists
        print(f"\n1️⃣ Check if database exists...")
        resp = await client.get(
            f"{TERMINUS_URL}/api/db/admin/{test_db}",
            auth=AUTH
        )
        print(f"   Database status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"   ✅ Database exists")
        else:
            print(f"   ❌ Database doesn't exist")
            print(f"   Response: {resp.text[:200]}")
            return
        
        # 2. Check specific document by ID
        print(f"\n2️⃣ Check specific schema document by ID...")
        resp = await client.get(
            f"{TERMINUS_URL}/api/document/admin/{test_db}",
            params={"graph_type": "schema", "id": test_class},
            auth=AUTH
        )
        print(f"   Document by ID status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"   ✅ Document found by ID!")
            try:
                doc = resp.json()
                print(f"   Document: {json.dumps(doc, indent=2)}")
            except:
                print(f"   Response text: {resp.text}")
        else:
            print(f"   ❌ Document not found by ID")
            print(f"   Response: {resp.text[:200]}")
        
        # 3. List ALL schema documents
        print(f"\n3️⃣ List ALL schema documents...")
        resp = await client.get(
            f"{TERMINUS_URL}/api/document/admin/{test_db}",
            params={"graph_type": "schema"},
            auth=AUTH
        )
        print(f"   All documents status: {resp.status_code}")
        if resp.status_code == 200:
            print(f"   ✅ Got schema documents!")
            try:
                docs = resp.json()
                if isinstance(docs, list):
                    print(f"   Found {len(docs)} documents:")
                    for i, doc in enumerate(docs):
                        doc_id = doc.get("@id", "unknown")
                        doc_type = doc.get("@type", "unknown")
                        print(f"     {i+1}. ID: {doc_id}, Type: {doc_type}")
                        
                        # Check if this is our target class
                        if doc_id == test_class:
                            print(f"       🎯 FOUND OUR CLASS!")
                            print(f"       Full document: {json.dumps(doc, indent=8)}")
                else:
                    print(f"   Single document: {json.dumps(docs, indent=2)}")
            except Exception as e:
                print(f"   Error parsing response: {e}")
                print(f"   Response text: {resp.text}")
        else:
            print(f"   ❌ Could not list documents")
            print(f"   Response: {resp.text[:200]}")
            
        # 4. Compare with what BFF API would do
        print(f"\n4️⃣ Test what BFF API endpoint would do...")
        
        # Simulate what the BFF might be doing
        # Let's try different variations
        endpoints_to_try = [
            f"/api/document/admin/{test_db}?graph_type=schema&id={test_class}",
            f"/api/document/admin/{test_db}?id={test_class}",
            f"/api/document/admin/{test_db}/{test_class}",
            f"/api/document/admin/{test_db}",
        ]
        
        for endpoint in endpoints_to_try:
            resp = await client.get(f"{TERMINUS_URL}{endpoint}", auth=AUTH)
            print(f"   {endpoint}: {resp.status_code}")
            if resp.status_code == 200:
                print(f"     ✅ This endpoint works!")
            
        print("\n" + "=" * 80)
        print("🎯 DIRECT TERMINUSDB CHECK COMPLETE!")
        print("=" * 80)

if __name__ == "__main__":
    asyncio.run(test_direct_terminusdb())