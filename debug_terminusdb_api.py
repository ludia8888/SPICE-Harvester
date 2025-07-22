#!/usr/bin/env python3
"""
🔥 THINK ULTRA! TerminusDB API 엔드포인트 디버깅
실제 API 구조 확인
"""

import asyncio
import json
import httpx
from datetime import datetime

async def debug_terminusdb_api():
    """TerminusDB API 구조 디버깅"""
    print("🔥 THINK ULTRA! TerminusDB API 디버깅")
    print("=" * 60)
    
    base_url = "http://localhost:6363"
    account = "admin"
    auth = httpx.BasicAuth("admin", "admin123")
    
    async with httpx.AsyncClient() as client:
        
        # 1. 기본 정보 확인
        print("📋 1. TerminusDB 서버 정보 확인...")
        try:
            response = await client.get(f"{base_url}/api/info", auth=auth)
            if response.status_code == 200:
                info = response.json()
                print(f"✅ 서버 정보: {json.dumps(info, indent=2)}")
            else:
                print(f"❌ 서버 정보 실패: {response.status_code}")
        except Exception as e:
            print(f"❌ 서버 정보 오류: {e}")
        
        # 2. 데이터베이스 목록 확인
        print("\n📦 2. 데이터베이스 목록 확인...")
        try:
            response = await client.get(f"{base_url}/api/db/{account}", auth=auth)
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                databases = response.json()
                print(f"✅ 데이터베이스 목록: {json.dumps(databases, indent=2)}")
            else:
                print(f"❌ 데이터베이스 목록 실패: {response.text}")
        except Exception as e:
            print(f"❌ 데이터베이스 목록 오류: {e}")
        
        # 3. 테스트 데이터베이스 생성
        test_db = f"api_debug_{int(datetime.now().timestamp())}"
        print(f"\n🏗️ 3. 테스트 데이터베이스 생성: {test_db}")
        try:
            data = {
                "label": test_db,
                "comment": "API debugging database"
            }
            response = await client.post(
                f"{base_url}/api/db/{account}/{test_db}", 
                json=data,
                auth=auth
            )
            print(f"   Status: {response.status_code}")
            if response.status_code in [200, 201]:
                result = response.json()
                print(f"✅ 데이터베이스 생성 성공: {json.dumps(result, indent=2)}")
                
                # 4. 생성된 데이터베이스 확인
                print(f"\n🔍 4. 생성된 데이터베이스 확인...")
                response = await client.get(f"{base_url}/api/db/{account}/{test_db}", auth=auth)
                print(f"   Status: {response.status_code}")
                if response.status_code == 200:
                    db_info = response.json()
                    print(f"✅ 데이터베이스 정보: {json.dumps(db_info, indent=2)}")
                else:
                    print(f"❌ 데이터베이스 정보 실패: {response.text}")
                
                # 5. 브랜치 API 테스트 (여러 엔드포인트 시도)
                print(f"\n🌿 5. 브랜치 API 테스트...")
                branch_endpoints = [
                    f"/api/db/{account}/{test_db}/branch",
                    f"/api/db/{account}/{test_db}/_branch",
                    f"/api/branch/{account}/{test_db}",
                    f"/api/db/{account}/{test_db}/branches",
                    f"/api/db/{account}/{test_db}/ref"
                ]
                
                for endpoint in branch_endpoints:
                    try:
                        response = await client.get(f"{base_url}{endpoint}", auth=auth)
                        print(f"   {endpoint}: {response.status_code}")
                        if response.status_code == 200:
                            branches = response.json()
                            print(f"   ✅ 브랜치 데이터: {json.dumps(branches, indent=2)}")
                        elif response.status_code != 404:
                            print(f"   ⚠️  응답: {response.text[:200]}")
                    except Exception as e:
                        print(f"   ❌ {endpoint} 오류: {e}")
                
                # 6. 태그 API 테스트
                print(f"\n🏷️ 6. 태그 API 테스트...")
                tag_endpoints = [
                    f"/api/db/{account}/{test_db}/tag",
                    f"/api/db/{account}/{test_db}/_tag",
                    f"/api/tag/{account}/{test_db}",
                    f"/api/db/{account}/{test_db}/tags",
                    f"/api/db/{account}/{test_db}/ref"
                ]
                
                for endpoint in tag_endpoints:
                    try:
                        response = await client.get(f"{base_url}{endpoint}", auth=auth)
                        print(f"   {endpoint}: {response.status_code}")
                        if response.status_code == 200:
                            tags = response.json()
                            print(f"   ✅ 태그 데이터: {json.dumps(tags, indent=2)}")
                        elif response.status_code != 404:
                            print(f"   ⚠️  응답: {response.text[:200]}")
                    except Exception as e:
                        print(f"   ❌ {endpoint} 오류: {e}")
                
                # 7. 커밋 API 테스트
                print(f"\n💾 7. 커밋 API 테스트...")
                commit_endpoints = [
                    f"/api/db/{account}/{test_db}/_commits",
                    f"/api/commits/{account}/{test_db}",
                    f"/api/db/{account}/{test_db}/commit",
                    f"/api/db/{account}/{test_db}/history"
                ]
                
                for endpoint in commit_endpoints:
                    try:
                        response = await client.get(f"{base_url}{endpoint}", auth=auth)
                        print(f"   {endpoint}: {response.status_code}")
                        if response.status_code == 200:
                            commits = response.json()
                            print(f"   ✅ 커밋 데이터: {json.dumps(commits, indent=2)}")
                        elif response.status_code != 404:
                            print(f"   ⚠️  응답: {response.text[:200]}")
                    except Exception as e:
                        print(f"   ❌ {endpoint} 오류: {e}")
                
                # 8. 정리
                print(f"\n🧹 8. 테스트 데이터베이스 삭제...")
                try:
                    response = await client.delete(f"{base_url}/api/db/{account}/{test_db}", auth=auth)
                    print(f"   Status: {response.status_code}")
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
    asyncio.run(debug_terminusdb_api())