#\!/usr/bin/env python3
"""
🔥 TerminusDB 11.x 올바른 WOQL 멀티홉 쿼리
"""

import requests
import json

# TerminusDB 11.x 설정
TERMINUS = "http://localhost:6364"  # 실제 실행 포트
DB = "spice_3pl_graph"
AUTH = ("admin", "admin")

print("🔥 SPICE HARVESTER - TerminusDB 11.x WOQL Multi-Hop")
print("=" * 70)

# TerminusDB 11.x는 /api/woql 엔드포인트 사용
def execute_woql(query_name, woql_query):
    """WOQL 쿼리 실행 - TerminusDB 11.x API"""
    print(f"\n🔍 {query_name}")
    
    # TerminusDB 11.x 형식
    response = requests.post(
        f"{TERMINUS}/api/woql/admin/{DB}",
        json={"query": woql_query},
        auth=AUTH,
        timeout=10
    )
    
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        result = response.json()
        print(f"   Response: {json.dumps(result, indent=2)[:300]}...")
        return True
    else:
        print(f"   Error: {response.text[:300]}")
        return False

# 간단한 테스트 - 모든 Product 찾기
print("\n1️⃣ Simple Test: Find all Products")
simple_query = {
    "@type": "woql:Triple",
    "woql:subject": "v:Product",
    "woql:predicate": "rdf:type",
    "woql:object": "@schema:Product"
}

execute_woql("All Products", simple_query)

# 2-hop 테스트 - 변수 형식 조정
print("\n2️⃣ 2-HOP: Product with Client relationship")
two_hop = {
    "@type": "woql:Triple",
    "woql:subject": "v:Product",
    "woql:predicate": "owned_by",
    "woql:object": "v:Client"
}

execute_woql("Product-Client relationships", two_hop)

print("\n" + "=" * 70)
print("💡 Note: TerminusDB 11.x uses /api/woql endpoint")
print("   Full multi-hop may require TerminusDB-specific query syntax")
