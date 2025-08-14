#!/usr/bin/env python3
"""
수정된 TerminusDB 공식 패턴 테스트
"""

import asyncio
import json
import logging
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_fixed_relationships():
    """공식 패턴으로 수정된 관계 테스트"""
    
    async with aiohttp.ClientSession() as session:
        db_name = "graph_test_db"
        
        # 1. Person 클래스 생성 (자기 참조 관계 포함)
        person_ontology = {
            "id": "Person",
            "label": "Person",
            "description": "Person with various relationship types",
            "properties": [
                {
                    "name": "name",
                    "type": "string",
                    "label": "Name",
                    "required": True  # 필수 속성
                },
                {
                    "name": "email",
                    "type": "string",
                    "label": "Email",
                    "required": False  # 옵셔널 속성
                }
            ],
            "relationships": [
                {
                    "predicate": "spouse",
                    "label": "Spouse",
                    "description": "1:1 optional relationship",
                    "target": "Person",
                    "cardinality": "1:1"
                },
                {
                    "predicate": "friends",
                    "label": "Friends",
                    "description": "1:N relationship (Set)",
                    "target": "Person",
                    "cardinality": "1:n"
                }
            ]
        }
        
        # 2. Organization 클래스 생성
        org_ontology = {
            "id": "Organization",
            "label": "Organization",
            "description": "Organization entity",
            "properties": [
                {
                    "name": "org_id",
                    "type": "string",
                    "label": "Organization ID",
                    "required": True
                },
                {
                    "name": "name",
                    "type": "string",
                    "label": "Organization Name",
                    "required": True
                }
            ]
        }
        
        # 3. Employment 클래스 생성 (관계에 속성이 있는 경우)
        employment_ontology = {
            "id": "Employment",
            "label": "Employment",
            "description": "Employment relationship with metadata",
            "properties": [
                {
                    "name": "start_date",
                    "type": "date",
                    "label": "Start Date",
                    "required": True
                },
                {
                    "name": "role",
                    "type": "string",
                    "label": "Role",
                    "required": False
                }
            ],
            "relationships": [
                {
                    "predicate": "person",
                    "label": "Person",
                    "description": "Person in employment",
                    "target": "Person",
                    "cardinality": "n:1"
                },
                {
                    "predicate": "org",
                    "label": "Organization",
                    "description": "Organization employing",
                    "target": "Organization",
                    "cardinality": "n:1"
                }
            ]
        }
        
        # 4. Roster 클래스 생성 (Set 관계 테스트)
        roster_ontology = {
            "id": "Roster",
            "label": "Roster",
            "description": "Roster with player set",
            "properties": [
                {
                    "name": "roster_id",
                    "type": "string",
                    "label": "Roster ID",
                    "required": True
                },
                {
                    "name": "season",
                    "type": "string",
                    "label": "Season",
                    "required": False
                }
            ],
            "relationships": [
                {
                    "predicate": "players",
                    "label": "Players",
                    "description": "Set of players",
                    "target": "Person",
                    "cardinality": "1:n"  # Set 타입
                }
            ]
        }
        
        # 모든 온톨로지 생성
        ontologies = [
            ("Person", person_ontology),
            ("Organization", org_ontology),
            ("Employment", employment_ontology),
            ("Roster", roster_ontology)
        ]
        
        for name, ontology in ontologies:
            url = f"http://localhost:8000/api/v1/ontology/{db_name}/create"
            
            try:
                async with session.post(url, json=ontology) as response:
                    if response.status in [200, 202]:
                        result = await response.json()
                        logger.info(f"✅ Created {name}")
                    else:
                        error = await response.text()
                        logger.error(f"❌ Failed to create {name}: {error}")
                        
            except Exception as e:
                logger.error(f"❌ Exception creating {name}: {e}")
            
            await asyncio.sleep(1)
        
        # 생성 완료 대기
        await asyncio.sleep(5)
        
        # 검증: TerminusDB에서 직접 스키마 확인
        logger.info("\n🔍 Verifying schema in TerminusDB...")
        
        for class_name, _ in ontologies:
            schema_url = f"http://localhost:6364/api/document/admin/{db_name}?graph_type=schema&id={class_name}"
            
            try:
                # TerminusDB 직접 접속 (6364 포트)
                import subprocess
                result = subprocess.run(
                    ["curl", "-s", "-u", "admin:admin", schema_url],
                    capture_output=True,
                    text=True
                )
                
                if result.stdout:
                    schema = json.loads(result.stdout)
                    logger.info(f"\n📌 {class_name} schema:")
                    
                    # 중요한 필드만 출력
                    for key, value in schema.items():
                        if key.startswith("@"):
                            continue  # 메타데이터 스킵
                        
                        if isinstance(value, dict):
                            if "@type" in value:
                                logger.info(f"   {key}: {value['@type']} -> {value.get('@class', 'N/A')}")
                            else:
                                logger.info(f"   {key}: {value}")
                        else:
                            logger.info(f"   {key}: {value}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to verify {class_name}: {e}")
        
        logger.info("\n✅ Test completed!")
        
        # 예상 결과:
        # Person:
        #   name: xsd:string (필수 -> 직접 지정)
        #   email: Optional -> xsd:string (옵셔널)
        #   spouse: Optional -> Person (1:1 옵셔널)
        #   friends: Set -> Person (1:N)
        #
        # Employment:
        #   start_date: xsd:date (필수)
        #   role: Optional -> xsd:string (옵셔널)
        #   person: Optional -> Person (n:1)
        #   org: Optional -> Organization (n:1)
        #
        # Roster:
        #   roster_id: xsd:string (필수)
        #   season: Optional -> xsd:string (옵셔널)
        #   players: Set -> Person (1:N)

if __name__ == "__main__":
    asyncio.run(test_fixed_relationships())