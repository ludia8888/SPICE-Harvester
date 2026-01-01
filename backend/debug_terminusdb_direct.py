#!/usr/bin/env python3
"""
TerminusDB 직접 조회 테스트 - 실제 저장된 데이터 확인
"""

import json
import requests
import base64
from datetime import datetime

# TerminusDB 직접 접근 설정
TERMINUSDB_URL = "http://localhost:6363"
TERMINUSDB_USER = "admin"
TERMINUSDB_PASS = "admin123"

def get_auth_header():
    """Basic 인증 헤더 생성"""
    credentials = f"{TERMINUSDB_USER}:{TERMINUSDB_PASS}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}

def test_direct_terminusdb_query():
    """TerminusDB에 직접 쿼리하여 Team 데이터 확인"""
    
    print("🔍 Direct TerminusDB Query Test")
    print("=" * 50)
    
    # 테스트 데이터베이스 이름 (최근에 생성된 것 사용)
    db_name = f"debug_team_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    headers = get_auth_header()
    headers["Content-Type"] = "application/json"
    
    try:
        # 1. 데이터베이스 생성
        print("🔨 Creating test database directly in TerminusDB...")
        create_db_data = {
            "organization": "admin",
            "database": db_name,
            "label": "Debug Direct Test",
            "comment": "Testing direct TerminusDB access"
        }
        
        response = requests.post(
            f"{TERMINUSDB_URL}/api/db/admin/{db_name}",
            json=create_db_data,
            headers=headers
        )
        response.raise_for_status()
        print(f"✅ Database created: {db_name}")
        
        # 2. BFF를 통해 Team 생성 (변환 로직 적용)
        print("🏢 Creating Team via BFF (with conversion logic)...")
        
        # Employee 먼저 생성
        employee_data = {
            "id": "Employee",
            "label": "Employee",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Name"},
                {"name": "email", "type": "string", "required": True, "label": "Email"}
            ]
        }
        
        bff_response = requests.post(
            f"http://localhost:8002/api/v1/databases/{db_name}/ontology",
            json=employee_data,
            headers={"Content-Type": "application/json"}
        )
        bff_response.raise_for_status()
        print("✅ Employee created via BFF")
        
        # Team 생성
        team_data = {
            "id": "Team",
            "label": "Team",
            "properties": [
                {"name": "name", "type": "string", "required": True, "label": "Team Name"},
                {
                    "name": "leader",
                    "type": "link",
                    "target": "Employee",
                    "required": True,
                    "label": "Team Leader"
                },
                {
                    "name": "members",
                    "type": "array",
                    "items": {"type": "link", "target": "Employee"},
                    "label": "Members"
                }
            ]
        }
        
        bff_response = requests.post(
            f"http://localhost:8002/api/v1/databases/{db_name}/ontology",
            json=team_data,
            headers={"Content-Type": "application/json"}
        )
        bff_response.raise_for_status()
        print("✅ Team created via BFF")
        
        # 3. TerminusDB에서 직접 스키마 조회
        print("🔍 Querying TerminusDB schema directly...")
        
        schema_response = requests.get(
            f"{TERMINUSDB_URL}/api/schema/admin/{db_name}",
            headers=headers
        )
        schema_response.raise_for_status()
        schema_data = schema_response.text
        
        print("📄 Raw TerminusDB Schema:")
        print(schema_data)
        
        # JSON Lines 형식으로 파싱 시도
        print("\n🔍 Parsing schema data...")
        lines = schema_data.strip().split('\n')
        team_schemas = []
        
        for line in lines:
            if line.strip():
                try:
                    doc = json.loads(line)
                    if doc.get("@id") == "Team" or "Team" in str(doc.get("@id", "")):
                        team_schemas.append(doc)
                        print(f"📋 Found Team schema: {json.dumps(doc, indent=2)}")
                except json.JSONDecodeError:
                    continue
        
        # 4. TerminusDB에서 직접 인스턴스 조회
        print("\n🔍 Querying TerminusDB instances directly...")
        
        instance_response = requests.get(
            f"{TERMINUSDB_URL}/api/document/admin/{db_name}",
            headers=headers
        )
        instance_response.raise_for_status()
        instance_data = instance_response.text
        
        print("📄 Raw TerminusDB Instances:")
        print(instance_data)
        
        # 5. BFF를 통한 조회와 비교
        print("\n🔍 Comparing with BFF retrieval...")
        
        bff_get_response = requests.get(
            f"http://localhost:8002/api/v1/databases/{db_name}/ontology/Team",
            headers={"Content-Type": "application/json"}
        )
        bff_get_response.raise_for_status()
        bff_data = bff_get_response.json()
        
        print("📄 BFF Retrieved Data:")
        print(json.dumps(bff_data, indent=2))
        
        # 6. 분석
        print("\n🎯 ANALYSIS:")
        print("=" * 30)
        
        if team_schemas:
            print(f"✅ Found {len(team_schemas)} Team schema(s) in TerminusDB")
            for i, schema in enumerate(team_schemas):
                print(f"Schema {i+1}: {json.dumps(schema, indent=2)}")
        else:
            print("❌ No Team schemas found in TerminusDB")
            
        print(f"\nBFF properties: {[p.get('name') for p in bff_data.get('properties', [])]}")
        print(f"BFF relationships: {[r.get('predicate') for r in bff_data.get('relationships', [])]}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 정리
        try:
            print(f"\n🧹 Cleaning up database: {db_name}")
            requests.delete(
                f"http://localhost:8002/api/v1/databases/{db_name}",
                headers={"Content-Type": "application/json"}
            )
            print("✅ Cleanup completed")
        except:
            print("⚠️ Cleanup failed")

if __name__ == "__main__":
    test_direct_terminusdb_query()
