#!/usr/bin/env python3
"""
🔥 ULTRA! 하이브리드 온톨로지 모델 테스트

사용자는 속성을 클래스 내부에 정의하지만, 내부적으로는 관계로 관리되는
하이브리드 모델이 제대로 작동하는지 테스트합니다.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add the parent directory to sys.path to import our modules
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_property_to_relationship_conversion():
    """Property → Relationship → Property 변환 테스트"""
    
    # TerminusDB 연결 설정
    connection_config = ConnectionConfig(
        server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
        user=os.getenv("TERMINUS_USER", "admin"),
        account=os.getenv("TERMINUS_ACCOUNT", "admin"),
        key=os.getenv("TERMINUS_KEY", "admin123"),
    )
    
    terminus = AsyncTerminusService(connection_config)
    
    # 테스트용 데이터베이스
    test_db = f"test_hybrid_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        logger.info("🚀 하이브리드 온톨로지 모델 테스트 시작")
        
        # 1. 데이터베이스 생성
        logger.info(f"📦 테스트 데이터베이스 생성: {test_db}")
        await terminus.create_database(test_db, "Hybrid model test database")
        
        # 2. 먼저 Department 클래스 생성 (Employee가 참조하므로)
        logger.info("🔧 Department 클래스 생성 중...")
        
        department_class_data = {
            "id": "Department",
            "label": "Department",
            "description": "Department class",
            "properties": [
                {
                    "name": "department_id",
                    "type": "string",
                    "required": True,
                    "label": "Department ID",
                    "constraints": {"unique": True}
                },
                {
                    "name": "name",
                    "type": "string",
                    "required": True,
                    "label": "Department Name"
                }
            ],
            "relationships": []
        }
        
        await terminus.create_ontology_class(test_db, department_class_data)
        logger.info("✅ Department 클래스 생성 완료!")
        
        # 3. Project 클래스 생성 (Employee가 참조하므로)
        logger.info("🔧 Project 클래스 생성 중...")
        
        project_class_data = {
            "id": "Project",
            "label": "Project",
            "description": "Project class",
            "properties": [
                {
                    "name": "project_id",
                    "type": "string",
                    "required": True,
                    "label": "Project ID",
                    "constraints": {"unique": True}
                },
                {
                    "name": "name",
                    "type": "string",
                    "required": True,
                    "label": "Project Name"
                }
            ],
            "relationships": []
        }
        
        await terminus.create_ontology_class(test_db, project_class_data)
        logger.info("✅ Project 클래스 생성 완료!")
        
        # 4. 테스트 클래스 생성 - Property와 Relationship 혼합
        logger.info("🔧 Employee 클래스 생성 중...")
        
        test_class_data = {
            "id": "Employee",
            "label": "Employee",
            "description": "Employee class with mixed properties and relationships",
            "properties": [
                # 일반 속성들
                {
                    "name": "employee_id",
                    "type": "string",
                    "required": True,
                    "label": "Employee ID",
                    "constraints": {"unique": True}
                },
                {
                    "name": "name",
                    "type": "string",
                    "required": True,
                    "label": "Full Name"
                },
                {
                    "name": "email",
                    "type": "string",
                    "required": True,
                    "label": "Email Address",
                    "constraints": {"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"}
                },
                {
                    "name": "salary",
                    "type": "decimal",
                    "required": False,
                    "label": "Salary",
                    "constraints": {"min": 0}
                },
                # 🔥 ULTRA! 클래스 참조 속성 (linkTarget 사용)
                {
                    "name": "department",
                    "type": "link",
                    "linkTarget": "Department",
                    "required": True,
                    "label": "Department",
                    "description": "The department this employee belongs to"
                },
                {
                    "name": "manager",
                    "type": "link",
                    "linkTarget": "Employee",
                    "required": False,
                    "label": "Direct Manager",
                    "description": "The employee's direct manager"
                }
            ],
            "relationships": [
                # 명시적 관계들 (역관계 포함)
                {
                    "predicate": "manages",
                    "target": "Employee",
                    "cardinality": "1:n",
                    "label": "Manages",
                    "description": "Employees managed by this person",
                    "inverse_predicate": "managed_by",
                    "inverse_label": "Managed By"
                },
                {
                    "predicate": "works_on",
                    "target": "Project",
                    "cardinality": "n:n",
                    "label": "Works On",
                    "description": "Projects this employee works on",
                    "inverse_predicate": "has_member",
                    "inverse_label": "Has Member"
                }
            ]
        }
        
        # 클래스 생성 실행
        result = await terminus.create_ontology_class(test_db, test_class_data)
        logger.info("✅ 클래스 생성 완료!")
        logger.info(f"📊 생성 결과: {json.dumps(result, indent=2)}")
        
        # 5. 생성된 클래스 조회 (역변환 테스트)
        logger.info("🔍 생성된 클래스 조회 중...")
        retrieved_class = await terminus.get_ontology(test_db, "Employee")
        
        logger.info("📋 조회된 클래스 구조:")
        logger.info(json.dumps(retrieved_class, indent=2, ensure_ascii=False))
        
        # 6. 🔥 ULTRA! 변환 검증
        logger.info("\n🔧 하이브리드 모델 검증:")
        
        # Properties 검증
        properties = retrieved_class.get("properties", [])
        property_names = {p["name"] for p in properties}
        
        # linkTarget 속성이 property로 유지되는지 확인
        department_prop = next((p for p in properties if p["name"] == "department"), None)
        manager_prop = next((p for p in properties if p["name"] == "manager"), None)
        
        logger.info(f"  📦 총 Properties: {len(properties)}")
        logger.info(f"  📋 Property 이름들: {sorted(property_names)}")
        
        if department_prop:
            logger.info(f"  ✅ 'department' property가 올바르게 역변환됨:")
            logger.info(f"     - type: {department_prop.get('type')}")
            logger.info(f"     - linkTarget: {department_prop.get('linkTarget')}")
            logger.info(f"     - label: {department_prop.get('label')}")
        else:
            logger.error("  ❌ 'department' property가 누락됨!")
        
        if manager_prop:
            logger.info(f"  ✅ 'manager' property가 올바르게 역변환됨:")
            logger.info(f"     - type: {manager_prop.get('type')}")
            logger.info(f"     - linkTarget: {manager_prop.get('linkTarget')}")
            logger.info(f"     - label: {manager_prop.get('label')}")
        else:
            logger.error("  ❌ 'manager' property가 누락됨!")
        
        # Relationships 검증
        relationships = retrieved_class.get("relationships", [])
        relationship_predicates = {r["predicate"] for r in relationships}
        
        logger.info(f"\n  🔗 총 Relationships: {len(relationships)}")
        logger.info(f"  📋 Relationship predicates: {sorted(relationship_predicates)}")
        
        # 명시적 관계만 relationships에 있어야 함
        expected_relationships = {"manages", "works_on"}
        if relationship_predicates == expected_relationships:
            logger.info("  ✅ 명시적 relationships만 정확히 유지됨")
        else:
            logger.error(f"  ❌ Relationship 목록이 예상과 다름!")
            logger.error(f"     - 예상: {expected_relationships}")
            logger.error(f"     - 실제: {relationship_predicates}")
        
        # linkTarget 속성들이 relationships에 중복되지 않았는지 확인
        if "department" not in relationship_predicates:
            logger.info("  ✅ 'department'가 relationship에 중복되지 않음")
        else:
            logger.error("  ❌ 'department'가 relationship로 잘못 표시됨!")
        
        if "manager" not in relationship_predicates:
            logger.info("  ✅ 'manager'가 relationship에 중복되지 않음")
        else:
            logger.error("  ❌ 'manager'가 relationship로 잘못 표시됨!")
        
        # 7. 테스트 성공 여부 판단
        test_passed = (
            department_prop is not None and
            department_prop.get("type") == "link" and
            department_prop.get("linkTarget") == "Department" and
            manager_prop is not None and
            manager_prop.get("type") == "link" and
            manager_prop.get("linkTarget") == "Employee" and
            relationship_predicates == expected_relationships
        )
        
        if test_passed:
            logger.info("\n🎉 하이브리드 온톨로지 모델이 완벽하게 작동합니다!")
            logger.info("✅ Property → Relationship → Property 변환 성공!")
            return True
        else:
            logger.error("\n❌ 하이브리드 온톨로지 모델 테스트 실패!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 테스트 중 오류 발생: {e}")
        import traceback
        logger.error(f"🔍 상세 오류: {traceback.format_exc()}")
        return False
        
    finally:
        # 테스트 데이터베이스 정리
        try:
            logger.info(f"🧹 테스트 데이터베이스 정리: {test_db}")
            await terminus.delete_database(test_db)
        except Exception as e:
            logger.warning(f"⚠️ 테스트 데이터베이스 정리 실패: {e}")


async def main():
    """메인 테스트 실행"""
    logger.info("=" * 80)
    logger.info("🔥 ULTRA! 하이브리드 온톨로지 모델 테스트")
    logger.info("=" * 80)
    
    success = await test_property_to_relationship_conversion()
    
    if success:
        logger.info("🎉 모든 테스트 통과!")
        return 0
    else:
        logger.error("❌ 테스트 실패!")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)