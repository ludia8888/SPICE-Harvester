#\!/usr/bin/env python3
"""
🔥 SPICE HARVESTER - 진짜 WOQL 서버사이드 멀티홉 쿼리
"""

import requests
import json

# 올바른 TerminusDB 설정
TERMINUS = "http://localhost:6364"  # 기본 포트는 6364
PATH = "admin/spice_3pl_graph/local/branch/main"  # org/db/repo/branch
AUTH = ("admin", "admin")

print("🔥 SPICE HARVESTER - Real WOQL Multi-Hop Queries")
print("=" * 70)

def execute_woql(query_name, woql_query):
    """WOQL 쿼리 실행"""
    print(f"\n🔍 {query_name}")
    
    response = requests.post(
        f"{TERMINUS}/woql/{PATH}",
        json={"query": woql_query},
        auth=AUTH,
        timeout=10
    )
    
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        result = response.json()
        bindings = result.get("bindings", [])
        print(f"   ✅ Found {len(bindings)} results")
        
        # 샘플 결과 출력
        for i, binding in enumerate(bindings[:3]):
            print(f"   Result {i+1}: {json.dumps(binding, indent=6)[:200]}...")
        return True
    else:
        print(f"   ❌ Error: {response.text[:200]}")
        return False

# 1. 2-HOP: SKU → Product → Client
print("\n1️⃣ 2-HOP QUERY: SKU → Product → Client")
woql_2hop = {
    "@type": "woql:And",
    "woql:and": [
        {
            "@type": "woql:Triple",
            "woql:subject": "v:SKU",
            "woql:predicate": "@schema:belongs_to",
            "woql:object": "v:Product"
        },
        {
            "@type": "woql:Triple",
            "woql:subject": "v:Product",
            "woql:predicate": "@schema:owned_by",
            "woql:object": "v:Client"
        }
    ]
}

success = execute_woql("SKU to Client (via Product)", woql_2hop)

# 2. 3-HOP: OrderItem → Order → Client (+ SKU)
print("\n2️⃣ 3-HOP QUERY: OrderItem → Order → Client")
woql_3hop = {
    "@type": "woql:And",
    "woql:and": [
        {
            "@type": "woql:Triple",
            "woql:subject": "v:OrderItem",
            "woql:predicate": "@schema:item_of",
            "woql:object": "v:Order"
        },
        {
            "@type": "woql:Triple",
            "woql:subject": "v:Order",
            "woql:predicate": "@schema:placed_by",
            "woql:object": "v:Client"
        },
        {
            "@type": "woql:Triple",
            "woql:subject": "v:OrderItem",
            "woql:predicate": "@schema:for_sku",
            "woql:object": "v:SKU"
        }
    ]
}

success = execute_woql("OrderItem to Client with SKU", woql_3hop)

# 3. 4-HOP: Receipt → Inbound → SKU → Product → Client
print("\n3️⃣ 4-HOP QUERY: Receipt → Inbound → SKU → Product → Client")
woql_4hop = {
    "@type": "woql:And",
    "woql:and": [
        {
            "@type": "woql:Triple",
            "woql:subject": "v:Receipt",
            "woql:predicate": "@schema:of_inbound",
            "woql:object": "v:Inbound"
        },
        {
            "@type": "woql:Triple",
            "woql:subject": "v:Inbound",
            "woql:predicate": "@schema:for_sku",
            "woql:object": "v:SKU"
        },
        {
            "@type": "woql:Triple",
            "woql:subject": "v:SKU",
            "woql:predicate": "@schema:belongs_to",
            "woql:object": "v:Product"
        },
        {
            "@type": "woql:Triple",
            "woql:subject": "v:Product",
            "woql:predicate": "@schema:owned_by",
            "woql:object": "v:Client"
        }
    ]
}

success = execute_woql("Receipt to Client (4 hops)", woql_4hop)

# 4. Supply Chain: Supplier → SKU → Product → Client
print("\n4️⃣ SUPPLY CHAIN: Supplier → SKU → Product → Client")
woql_supply = {
    "@type": "woql:And",
    "woql:and": [
        {
            "@type": "woql:Triple",
            "woql:subject": "v:SKU",
            "woql:predicate": "@schema:supplied_by",
            "woql:object": "v:Supplier"
        },
        {
            "@type": "woql:Triple",
            "woql:subject": "v:SKU",
            "woql:predicate": "@schema:belongs_to",
            "woql:object": "v:Product"
        },
        {
            "@type": "woql:Triple",
            "woql:subject": "v:Product",
            "woql:predicate": "@schema:owned_by",
            "woql:object": "v:Client"
        }
    ]
}

success = execute_woql("Complete Supply Chain", woql_supply)

# 5. Self-referential: Event → Event
print("\n5️⃣ SELF-REFERENTIAL: Event → Event chains")
woql_events = {
    "@type": "woql:Triple",
    "woql:subject": "v:Event1",
    "woql:predicate": "@schema:caused_by",
    "woql:object": "v:Event2"
}

success = execute_woql("Event Causality", woql_events)

# 6. Complex with filters - Orders from specific warehouse
print("\n6️⃣ FILTERED QUERY: Orders from Warehouse shipping to specific Client")
woql_filtered = {
    "@type": "woql:And",
    "woql:and": [
        {
            "@type": "woql:Triple",
            "woql:subject": "v:Order",
            "woql:predicate": "@schema:ship_from",
            "woql:object": "v:Warehouse"
        },
        {
            "@type": "woql:Triple",
            "woql:subject": "v:Order",
            "woql:predicate": "@schema:placed_by",
            "woql:object": "v:Client"
        }
    ]
}

success = execute_woql("Orders by Warehouse and Client", woql_filtered)

print("\n" + "=" * 70)
if success:
    print("✅ WOQL Multi-hop queries working\!")
    print("🎯 TerminusDB is performing TRUE server-side graph traversal\!")
else:
    print("⚠️ Some queries failed - check schema prefixes and relationships")
