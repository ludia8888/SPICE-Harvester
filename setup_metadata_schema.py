#!/usr/bin/env python3
"""
메타데이터 스키마 초기화 스크립트
ClassMetadata와 FieldMetadata 스키마를 TerminusDB에 직접 생성
"""
import requests
import json

BASE_URL = "http://localhost:8000/api/v1"
TERMINUS_URL = "http://localhost:6363"
DB_NAME = "spice_metadata_test"

def setup_metadata_schema_direct():
    """TerminusDB에 직접 메타데이터 스키마 생성"""
    print("🔥 THINK ULTRA! 메타데이터 스키마 직접 생성")
    
    # TerminusDB Schema API 사용 (Document API 대신)
    endpoint = f"{TERMINUS_URL}/api/schema/admin/{DB_NAME}"
    # Basic Auth for admin:admin123
    import base64
    auth_string = base64.b64encode(b"admin:admin123").decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_string}"
    }
    
    # 전체 스키마 업데이트 (기존 스키마와 병합)
    new_schema = {
        "@context": {
            "@base": f"terminusdb:///admin/{DB_NAME}/data/",
            "@schema": f"terminusdb:///admin/{DB_NAME}/schema#",
            "@type": "Context"
        },
        # 1. FieldMetadata 스키마 정의 (@subdocument로 설정)
        "FieldMetadata": {
            "@type": "Class",
            "@subdocument": [],
            "field_name": "xsd:string",
            "field_type": {"@type": "Optional", "@class": "xsd:string"},
            "label_ko": {"@type": "Optional", "@class": "xsd:string"},
            "label_en": {"@type": "Optional", "@class": "xsd:string"},
            "description_ko": {"@type": "Optional", "@class": "xsd:string"},
            "description_en": {"@type": "Optional", "@class": "xsd:string"},
            # Relationship fields
            "is_relationship": {"@type": "Optional", "@class": "xsd:boolean"},
            "target_class": {"@type": "Optional", "@class": "xsd:string"},
            "cardinality": {"@type": "Optional", "@class": "xsd:string"},
            "inverse_predicate": {"@type": "Optional", "@class": "xsd:string"},
            "inverse_label_ko": {"@type": "Optional", "@class": "xsd:string"},
            "inverse_label_en": {"@type": "Optional", "@class": "xsd:string"},
            # Property constraints
            "required": {"@type": "Optional", "@class": "xsd:boolean"},
            "default_value": {"@type": "Optional", "@class": "xsd:string"},
            "min_length": {"@type": "Optional", "@class": "xsd:integer"},
            "max_length": {"@type": "Optional", "@class": "xsd:integer"},
            "minimum": {"@type": "Optional", "@class": "xsd:decimal"},
            "maximum": {"@type": "Optional", "@class": "xsd:decimal"},
            "pattern": {"@type": "Optional", "@class": "xsd:string"},
            "enum_values": {"@type": "Optional", "@class": "xsd:string"}
        },
        # 2. ClassMetadata 스키마 정의
        "ClassMetadata": {
            "@type": "Class",
            "@key": {"@type": "Random"},
            "for_class": "xsd:string",
            "label_ko": {"@type": "Optional", "@class": "xsd:string"},
            "label_en": {"@type": "Optional", "@class": "xsd:string"},
            "description_ko": {"@type": "Optional", "@class": "xsd:string"},
            "description_en": {"@type": "Optional", "@class": "xsd:string"},
            "created_at": {"@type": "Optional", "@class": "xsd:dateTime"},
            "updated_at": {"@type": "Optional", "@class": "xsd:dateTime"},
            "fields": {
                "@type": "Set",
                "@class": "FieldMetadata"
            }
        }
    }
    
    # 스키마 업데이트 요청 (POST로 전체 스키마 교체)
    response = requests.post(
        endpoint,
        headers=headers,
        json=new_schema
    )
    
    if response.status_code == 200:
        print("✅ 메타데이터 스키마 생성 성공!")
        return True
    else:
        print(f"❌ 메타데이터 스키마 생성 실패: {response.status_code}")
        print(f"응답: {response.text}")
        
        # 기존 스키마와 병합 시도
        print("\n🔄 기존 스키마와 병합 시도...")
        existing_response = requests.get(endpoint, headers=headers)
        if existing_response.status_code == 200:
            existing_schema = existing_response.json()
            print("기존 스키마 구조:")
            for key in existing_schema.keys():
                if key != "@context":
                    print(f"  - {key}")
            
            # 기존 스키마에 새 클래스 추가
            merged_schema = existing_schema.copy()
            merged_schema["FieldMetadata"] = new_schema["FieldMetadata"]
            merged_schema["ClassMetadata"] = new_schema["ClassMetadata"]
            
            # 병합된 스키마로 재시도
            retry_response = requests.post(endpoint, headers=headers, json=merged_schema)
            if retry_response.status_code == 200:
                print("✅ 병합된 스키마 생성 성공!")
                return True
            else:
                print(f"❌ 병합된 스키마 생성도 실패: {retry_response.status_code}")
                print(f"응답: {retry_response.text}")
        
        return False
        
def check_schemas():
    """생성된 스키마 확인"""
    print("\n=== 스키마 확인 ===")
    
    endpoint = f"{TERMINUS_URL}/api/document/admin/{DB_NAME}"
    params = {
        "graph_type": "schema",
        "type": "Class"
    }
    
    # Basic Auth
    import base64
    auth_string = base64.b64encode(b"admin:admin123").decode()
    headers = {
        "Authorization": f"Basic {auth_string}"
    }
    
    response = requests.get(endpoint, params=params, headers=headers)
    if response.status_code == 200:
        schemas = response.text.strip().split('\n')
        for schema_line in schemas:
            if schema_line:
                try:
                    schema = json.loads(schema_line)
                    if schema.get("@id") in ["ClassMetadata", "FieldMetadata"]:
                        print(f"\n발견된 스키마: {schema.get('@id')}")
                        print(json.dumps(schema, indent=2, ensure_ascii=False))
                except:
                    pass

if __name__ == "__main__":
    print("메타데이터 스키마 설정 시작")
    print("=" * 60)
    
    # 데이터베이스는 이미 생성되었다고 가정
    print(f"📊 데이터베이스: {DB_NAME}")
    
    setup_metadata_schema_direct()
    check_schemas()
    
    print("\n완료!")