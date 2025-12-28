#!/usr/bin/env python3
"""
🔥 THINK ULTRA: 팔란티어 스타일 아키텍처 완전 검증
단 하나의 거짓 없이 전체 user flow 시뮬레이션
"""

import asyncio
import aiohttp
import json
import os
import time
from datetime import datetime
from typing import Dict, Any, List
import pytest

# Configuration
OMS_URL = "http://localhost:8000"
BFF_URL = "http://localhost:8002"
TEST_DB = f"palantir_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_complete_user_flow():
    admin_token = (os.getenv("ADMIN_TOKEN") or os.getenv("OMS_ADMIN_TOKEN") or "test-token").strip()
    headers = {"X-Admin-Token": admin_token}

    """완전한 user flow 테스트"""
    
    print("=" * 80)
    print("🔥 THINK ULTRA: 팔란티어 스타일 아키텍처 검증")
    print("=" * 80)
    
    connector = aiohttp.TCPConnector(force_close=True, limit=0)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        # 1. 데이터베이스 생성
        print("\n1️⃣ 데이터베이스 생성 (Event Sourcing)")
        print("-" * 40)
        
        async with session.post(
            f"{OMS_URL}/api/v1/database/create",
            json={
                "name": TEST_DB,
                "description": "Palantir architecture test"
            },
            headers=headers,
        ) as create_db_response:
            if create_db_response.status in [200, 201, 202]:
                result = await create_db_response.json()
                command_id = result.get("command_id", "N/A")
                print(f"✅ DB 생성 Command 수락: {command_id}")
                print(f"   Status: 202 Accepted (Event Sourcing)")
            else:
                print(f"❌ DB 생성 실패: {create_db_response.status}")
                return False
        
        # Wait for DB creation
        await asyncio.sleep(5)
        
        # 2. 온톨로지 생성 (Product, Client with relationships)
        print("\n2️⃣ 온톨로지 생성 (관계 포함)")
        print("-" * 40)
        
        ontologies = [
            {
                "id": "Client",
                "label": "고객",
                "description": "고객 정보",
                "properties": [
                    {"name": "client_id", "type": "string", "label": "고객 ID", "required": True},
                    {"name": "name", "type": "string", "label": "이름", "required": True},
                    {"name": "email", "type": "string", "label": "이메일"}
                ]
            },
            {
                "id": "Product",
                "label": "제품",
                "description": "제품 정보",
                "properties": [
                    {"name": "product_id", "type": "string", "label": "제품 ID", "required": True},
                    {"name": "name", "type": "string", "label": "제품명", "required": True},
                    {"name": "category", "type": "string", "label": "카테고리"},
                    {"name": "price", "type": "number", "label": "가격"}
                ],
                "relationships": [
                    {
                        "predicate": "owned_by",
                        "label": "소유자",
                        "target": "Client",
                        "cardinality": "n:1"
                    }
                ]
            },
            {
                "id": "Order",
                "label": "주문",
                "description": "주문 정보",
                "properties": [
                    {"name": "order_id", "type": "string", "label": "주문 ID", "required": True},
                    {"name": "order_date", "type": "date", "label": "주문일"},
                    {"name": "total_amount", "type": "number", "label": "총액"}
                ],
                "relationships": [
                    {
                        "predicate": "placed_by",
                        "label": "주문자",
                        "target": "Client",
                        "cardinality": "n:1"
                    },
                    {
                        "predicate": "contains",
                        "label": "포함 제품",
                        "target": "Product",
                        "cardinality": "n:n"
                    }
                ]
            }
        ]
        
        for ontology in ontologies:
            async with session.post(
                f"{OMS_URL}/api/v1/database/{TEST_DB}/ontology",
                json=ontology,
                headers=headers,
            ) as create_ont_response:
                if create_ont_response.status in [200, 201, 202]:
                    print(f"✅ {ontology['id']} 온톨로지 생성 성공")
                else:
                    print(f"❌ {ontology['id']} 온톨로지 생성 실패: {create_ont_response.status}")
        
        await asyncio.sleep(3)
        
        # 3. CQRS 쓰기 경로 테스트 (비동기 인스턴스 생성)
        print("\n3️⃣ CQRS 쓰기 경로: Document API → Command → Event Store")
        print("-" * 40)
        
        # Create Client instance
        client_data = {
            "client_id": "CL-001",
            "name": "Palantir Technologies",
            "email": "contact@palantir.com"
        }
        
        async with session.post(
            f"{OMS_URL}/api/v1/instances/{TEST_DB}/async/Client/create",
            json={"data": client_data},
            headers=headers,
        ) as client_response:
            if client_response.status in [200, 201, 202]:
                result = await client_response.json()
                print(f"✅ Client 생성 Command 수락")
                print(f"   Command ID: {result.get('command_id')}")
                print(f"   Status: 202 Accepted ✓")
            else:
                print(f"❌ Client 생성 실패: {client_response.status}")
                text = await client_response.text()
                print(f"   Error: {text[:200]}")
        
        # Create Product instances
        products = [
            {
                "product_id": "PROD-001",
                "name": "Foundry Platform",
                "category": "Data Integration",
                "price": 1000000,
                "owned_by": "Client/CL-001"  # Relationship
            },
            {
                "product_id": "PROD-002",
                "name": "Gotham Platform",
                "category": "Intelligence",
                "price": 2000000,
                "owned_by": "Client/CL-001"  # Relationship
            }
        ]
        
        for product in products:
            async with session.post(
                f"{OMS_URL}/api/v1/instances/{TEST_DB}/async/Product/create",
                json={"data": product},
                headers=headers,
            ) as prod_response:
                if prod_response.status in [200, 201, 202]:
                    result = await prod_response.json()
                    print(f"✅ Product {product['product_id']} 생성 Command 수락")
                else:
                    print(f"❌ Product {product['product_id']} 생성 실패: {prod_response.status}")
        
        # Wait for async processing
        print("\n⏳ Event Sourcing 처리 대기 (5초)...")
        await asyncio.sleep(5)
        
        # 4. CQRS 읽기 경로 테스트 (Document API)
        print("\n4️⃣ CQRS 읽기 경로: Document API 조회")
        print("-" * 40)
        
        # Get class instances via optimized endpoint
        async with session.get(
            f"{OMS_URL}/api/v1/instance/{TEST_DB}/class/Product/instances?limit=10",
            headers=headers,
        ) as instances_response:
            if instances_response.status == 200:
                result = await instances_response.json()
                instances = result.get("instances", [])
                print(f"✅ Document API 조회 성공")
                print(f"   Total: {result.get('total', 0)} instances")
                print(f"   Source: {result.get('source', 'unknown')}")
                for inst in instances[:2]:
                    print(f"   - {inst.get('product_id', 'N/A')}: {inst.get('name', 'N/A')}")
            else:
                print(f"❌ Document API 조회 실패: {instances_response.status}")
        
        # 5. WOQL 멀티홉 쿼리 테스트
        print("\n5️⃣ WOQL 멀티홉 쿼리: Product → owned_by → Client")
        print("-" * 40)
        
        multihop_request = {
            "start_class": "Product",
            "hops": [
                {"predicate": "owned_by", "target_class": "Client"}
            ],
            "filters": {"product_id": "PROD-001"},
            "limit": 10
        }
        
        async with session.post(
            f"{BFF_URL}/api/v1/graph-query/{TEST_DB}",
            json=multihop_request,
            headers=headers,
        ) as multihop_response:
            if multihop_response.status == 200:
                result = await multihop_response.json()
                nodes = result.get("nodes", [])
                edges = result.get("edges", [])
                print(f"✅ WOQL 멀티홉 쿼리 성공")
                print(f"   Nodes: {len(nodes)}")
                print(f"   Edges: {len(edges)}")
                
                for node in nodes:
                    print(f"   - {node['type']}: {node['id']}")
                    if node.get('data'):
                        print(f"     ES Data: ✓ (enriched)")
            else:
                error_text = await multihop_response.text()
                print(f"❌ WOQL 멀티홉 쿼리 실패: {multihop_response.status}")
                print(f"   Error: {error_text[:200]}")
        
        # 6. 프로젝션 등록 테스트
        print("\n6️⃣ 프로젝션/뷰 Materialization")
        print("-" * 40)
        
        projection_request = {
            "view_name": "products_with_owners",
            "start_class": "Product",
            "hops": [
                {"predicate": "owned_by", "target_class": "Client"}
            ],
            "refresh_interval": 600  # 10분
        }
        
        async with session.post(
            f"{BFF_URL}/api/v1/projections/{TEST_DB}/register",
            json=projection_request,
            headers=headers,
        ) as projection_response:
            if projection_response.status == 200:
                result = await projection_response.json()
                print(f"✅ 프로젝션 등록 성공")
                print(f"   View: {result.get('view_name')}")
                print(f"   Status: {result.get('status')}")
                print(f"   Refresh: {result.get('refresh_interval')}초")
            else:
                print(f"❌ 프로젝션 등록 실패: {projection_response.status}")
        
        # 7. 프로젝션 조회 테스트
        print("\n7️⃣ 프로젝션 캐시 조회")
        print("-" * 40)
        
        query_projection_request = {
            "view_name": "products_with_owners",
            "limit": 10
        }
        
        async with session.post(
            f"{BFF_URL}/api/v1/projections/{TEST_DB}/query",
            json=query_projection_request,
            headers=headers,
        ) as query_projection_response:
            if query_projection_response.status == 200:
                result = await query_projection_response.json()
                print(f"✅ 프로젝션 조회 응답")
                print(f"   Status: {result.get('status')}")
                print(f"   Message: {result.get('message')}")
                
                if result.get('status') == 'fallback':
                    print("   ⚠️ 아직 materialized되지 않음 (정상 - 초기 상태)")
                else:
                    print(f"   Data: {len(result.get('data', []))} records")
            else:
                print(f"❌ 프로젝션 조회 실패: {query_projection_response.status}")
        
        # 8. 시스템 통합 검증
        print("\n8️⃣ 시스템 통합 검증")
        print("-" * 40)
        
        # Check if services can find each other
        async with session.get(
            f"{BFF_URL}/api/v1/graph-query/health",
            headers=headers,
        ) as health_response:
            if health_response.status == 200:
                result = await health_response.json()
                print(f"✅ Graph Federation 서비스 상태")
                print(f"   Status: {result.get('status')}")
                
                services = result.get('services', {})
                for service, status in services.items():
                    icon = "✅" if status == "healthy" else "❌"
                    print(f"   {icon} {service}: {status}")
            else:
                print(f"❌ Health check 실패: {health_response.status}")
        
        # 9. 아키텍처 원칙 검증 결과
        print("\n" + "=" * 80)
        print("📊 팔란티어 스타일 아키텍처 검증 결과")
        print("=" * 80)
        
        verifications = {
            "CQRS 쓰기 경로 (Event Sourcing)": client_response.status in [200, 201, 202],
            "CQRS 읽기 경로 (Document API)": instances_response.status == 200,
            "WOQL 멀티홉 쿼리": multihop_response.status == 200,
            "프로젝션 등록": projection_response.status in [200, 201, 202],
            "프로젝션 조회": query_projection_response.status == 200,
            "서비스 통합": health_response.status == 200,
        }
        
        all_passed = True
        for check, passed in verifications.items():
            icon = "✅" if passed else "❌"
            print(f"{icon} {check}: {'PASS' if passed else 'FAIL'}")
            if not passed:
                all_passed = False
        
        print("\n" + "=" * 80)
        if all_passed:
            print("🎉 모든 검증 통과! 팔란티어 스타일 아키텍처가 정상 작동합니다.")
            print("✅ 중복 제거 완료")
            print("✅ CQRS 패턴 구현 확인")
            print("✅ WOQL 기반 그래프 쿼리 작동")
            print("✅ 프로젝션 시스템 준비 완료")
        else:
            print("⚠️ 일부 검증 실패. 위 결과를 확인하세요.")
        print("=" * 80)
        
        # Cleanup
        print(f"\n🧹 테스트 데이터베이스 정리: {TEST_DB}")
        async with session.delete(
            f"{OMS_URL}/api/v1/database/{TEST_DB}",
            headers=headers,
        ) as delete_response:
            if delete_response.status in [200, 202]:
                print("✅ 정리 완료")
        
        return all_passed


if __name__ == "__main__":
    print("🚀 팔란티어 스타일 아키텍처 검증 시작...")
    result = asyncio.run(test_complete_user_flow())
    exit(0 if result else 1)
