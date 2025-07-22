#!/usr/bin/env python3
"""
🔥 THINK ULTRA! TerminusDB 지원 엔드포인트 전체 탐색
"""

import asyncio
import json
import httpx
from datetime import datetime

async def explore_terminusdb_endpoints():
    """TerminusDB 지원 엔드포인트 탐색"""
    print("🔥 THINK ULTRA! TerminusDB 엔드포인트 탐색")
    print("=" * 60)
    
    base_url = "http://localhost:6363"
    account = "admin"
    auth = httpx.BasicAuth("admin", "admin123")
    
    async with httpx.AsyncClient() as client:
        
        # 1. 일반적인 API 엔드포인트들 테스트
        print("📋 1. 기본 API 엔드포인트 확인...")
        base_endpoints = [
            "/api",
            "/api/info",
            "/api/ok",
            "/api/db",
            f"/api/db/{account}",
            "/api/organizations",
            "/api/users"
        ]
        
        for endpoint in base_endpoints:
            try:
                response = await client.get(f"{base_url}{endpoint}", auth=auth)
                print(f"   {endpoint}: {response.status_code}")
                if response.status_code == 200 and len(response.text) < 500:
                    try:
                        data = response.json()
                        print(f"     {json.dumps(data, indent=2)[:200]}...")
                    except:
                        print(f"     {response.text[:100]}...")
            except Exception as e:
                print(f"   {endpoint}: ERROR - {e}")
        
        # 2. 테스트 데이터베이스로 더 많은 엔드포인트 테스트
        test_db = f"endpoint_test_{int(datetime.now().timestamp())}"
        print(f"\n🏗️ 2. 테스트 데이터베이스 생성: {test_db}")
        
        try:
            data = {
                "label": test_db,
                "comment": "Endpoint exploration database"
            }
            response = await client.post(
                f"{base_url}/api/db/{account}/{test_db}", 
                json=data,
                auth=auth
            )
            
            if response.status_code in [200, 201]:
                print("✅ 데이터베이스 생성 성공")
                
                # 3. 데이터베이스별 엔드포인트 테스트
                print(f"\n🔍 3. 데이터베이스별 엔드포인트 테스트...")
                db_endpoints = [
                    f"/api/db/{account}/{test_db}",
                    f"/api/db/{account}/{test_db}/local",
                    f"/api/db/{account}/{test_db}/local/branch",
                    f"/api/db/{account}/{test_db}/local/branch/main",
                    f"/api/db/{account}/{test_db}/local/commit",
                    f"/api/db/{account}/{test_db}/_meta",
                    f"/api/db/{account}/{test_db}/schema",
                    f"/api/db/{account}/{test_db}/instance",
                    f"/api/woql/{account}/{test_db}",
                    f"/api/document/{account}/{test_db}",
                ]
                
                for endpoint in db_endpoints:
                    try:
                        response = await client.get(f"{base_url}{endpoint}", auth=auth)
                        print(f"   {endpoint}: {response.status_code}")
                        if response.status_code == 200:
                            try:
                                data = response.json()
                                print(f"     ✅ {json.dumps(data, indent=2)[:200]}...")
                            except:
                                print(f"     ✅ {response.text[:100]}...")
                        elif response.status_code not in [404, 405]:
                            print(f"     ⚠️  {response.text[:100]}...")
                    except Exception as e:
                        print(f"   {endpoint}: ERROR - {e}")
                
                # 4. WOQL 쿼리로 브랜치 정보 확인
                print(f"\n🔍 4. WOQL 쿼리로 브랜치 정보 확인...")
                try:
                    woql_query = {
                        "@type": "woql:And",
                        "woql:and": [
                            {
                                "@type": "woql:Triple",
                                "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "Branch"},
                                "woql:predicate": {"@type": "woql:Node", "woql:node": "type"},
                                "woql:object": {"@type": "woql:Node", "woql:node": "ref:Branch"}
                            },
                            {
                                "@type": "woql:Triple", 
                                "woql:subject": {"@type": "woql:Variable", "woql:variable_name": "Branch"},
                                "woql:predicate": {"@type": "woql:Node", "woql:node": "ref:name"},
                                "woql:object": {"@type": "woql:Variable", "woql:variable_name": "Name"}
                            }
                        ]
                    }
                    
                    response = await client.post(
                        f"{base_url}/api/woql/{account}/{test_db}",
                        json={"query": woql_query},
                        auth=auth
                    )
                    print(f"   WOQL 브랜치 쿼리: {response.status_code}")
                    if response.status_code == 200:
                        data = response.json()
                        print(f"   ✅ WOQL 응답: {json.dumps(data, indent=2)[:300]}...")
                    else:
                        print(f"   ❌ WOQL 오류: {response.text[:200]}...")
                        
                except Exception as e:
                    print(f"   WOQL 쿼리 오류: {e}")
                
                # 5. 브랜치 생성 시도 (POST 메서드)
                print(f"\n🌿 5. 브랜치 생성 시도...")
                branch_create_endpoints = [
                    f"/api/db/{account}/{test_db}/local/branch/test_branch",
                    f"/api/branch/{account}/{test_db}/test_branch", 
                    f"/api/db/{account}/{test_db}/_branch"
                ]
                
                for endpoint in branch_create_endpoints:
                    try:
                        branch_data = {
                            "origin": "main",
                            "label": "test_branch",
                            "comment": "Test branch creation"
                        }
                        response = await client.post(
                            f"{base_url}{endpoint}",
                            json=branch_data,
                            auth=auth
                        )
                        print(f"   POST {endpoint}: {response.status_code}")
                        if response.status_code in [200, 201]:
                            print(f"     ✅ 브랜치 생성 성공: {response.text[:100]}...")
                        else:
                            print(f"     ❌ {response.text[:100]}...")
                    except Exception as e:
                        print(f"   POST {endpoint}: ERROR - {e}")
                
                # 6. 정리
                print(f"\n🧹 6. 테스트 데이터베이스 삭제...")
                try:
                    response = await client.delete(f"{base_url}/api/db/{account}/{test_db}", auth=auth)
                    if response.status_code in [200, 204]:
                        print(f"✅ 데이터베이스 삭제 성공")
                    else:
                        print(f"⚠️  삭제 응답: {response.text}")
                except Exception as e:
                    print(f"❌ 삭제 오류: {e}")
                    
            else:
                print(f"❌ 데이터베이스 생성 실패: {response.text}")
                
        except Exception as e:
            print(f"❌ 데이터베이스 생성 오류: {e}")

if __name__ == "__main__":
    asyncio.run(explore_terminusdb_endpoints())