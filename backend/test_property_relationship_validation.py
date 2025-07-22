#!/usr/bin/env python3
"""
🔥 ULTRA! Property와 Relationship 통합 검증 테스트

하이브리드 온톨로지 모델에서 Property와 Relationship의
변환, 저장, 조회가 모든 시나리오에서 정확히 작동하는지 검증합니다.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

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


async def test_scenario_1_basic_property_conversion():
    """시나리오 1: 기본 Property → Relationship 변환"""
    logger.info("🔥 시나리오 1: 기본 Property → Relationship 변환 테스트")
    
    # 기본 클래스 데이터
    class_data = {
        "id": "TestClass1",
        "label": "Test Class 1",
        "properties": [
            {"name": "name", "type": "string", "required": True, "label": "Name"},
            {"name": "owner", "type": "link", "linkTarget": "User", "required": True, "label": "Owner"},
            {"name": "category", "type": "link", "linkTarget": "Category", "required": False, "label": "Category"}
        ],
        "relationships": []
    }
    
    # 변환 테스트
    from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
    converter = PropertyToRelationshipConverter()
    processed = converter.process_class_data(class_data)
    
    # 검증
    assert len(processed["properties"]) == 1  # name만 남음
    assert len(processed["relationships"]) == 2  # owner, category가 변환됨
    
    # 변환된 관계 확인
    rel_names = {r["predicate"] for r in processed["relationships"]}
    assert rel_names == {"owner", "category"}
    
    # 메타데이터 확인
    for rel in processed["relationships"]:
        assert rel.get("_converted_from_property") == True
        assert rel.get("_original_property_name") in ["owner", "category"]
    
    logger.info("✅ 시나리오 1 통과!")
    return True


async def test_scenario_2_mixed_properties_relationships():
    """시나리오 2: Property와 Relationship 혼합"""
    logger.info("🔥 시나리오 2: Property와 Relationship 혼합 테스트")
    
    class_data = {
        "id": "TestClass2",
        "label": "Test Class 2",
        "properties": [
            {"name": "id", "type": "string", "required": True, "label": "ID"},
            {"name": "title", "type": "string", "required": True, "label": "Title"},
            {"name": "created_by", "type": "link", "linkTarget": "User", "required": True, "label": "Created By"},
            {"name": "assigned_to", "type": "link", "linkTarget": "User", "required": False, "label": "Assigned To"}
        ],
        "relationships": [
            {
                "predicate": "tagged_with",
                "target": "Tag",
                "cardinality": "n:n",
                "label": "Tagged With"
            },
            {
                "predicate": "relates_to",
                "target": "TestClass2",
                "cardinality": "n:n",
                "label": "Relates To"
            }
        ]
    }
    
    from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
    converter = PropertyToRelationshipConverter()
    processed = converter.process_class_data(class_data)
    
    # 검증
    assert len(processed["properties"]) == 2  # id, title만 남음
    assert len(processed["relationships"]) == 4  # 2개 변환 + 2개 원래
    
    # 명시적 관계 유지 확인
    explicit_rels = [r for r in processed["relationships"] 
                     if not r.get("_converted_from_property")]
    assert len(explicit_rels) == 2
    assert {r["predicate"] for r in explicit_rels} == {"tagged_with", "relates_to"}
    
    logger.info("✅ 시나리오 2 통과!")
    return True


async def test_scenario_3_complex_constraints():
    """시나리오 3: 복잡한 제약조건과 타입"""
    logger.info("🔥 시나리오 3: 복잡한 제약조건과 타입 테스트")
    
    class_data = {
        "id": "TestClass3",
        "label": "Test Class 3",
        "properties": [
            {
                "name": "code",
                "type": "string",
                "required": True,
                "label": "Code",
                "constraints": {"pattern": "^[A-Z]{3}[0-9]{4}$", "unique": True}
            },
            {
                "name": "amount",
                "type": "decimal",
                "required": True,
                "label": "Amount",
                "constraints": {"min": 0, "max": 1000000}
            },
            {
                "name": "tags",
                "type": "list<string>",
                "required": False,
                "label": "Tags",
                "constraints": {"min_items": 0, "max_items": 10}
            },
            {
                "name": "status",
                "type": "string",
                "required": True,
                "label": "Status",
                "constraints": {"enum": ["active", "inactive", "pending"]}
            },
            {
                "name": "primary_contact",
                "type": "link",
                "linkTarget": "Contact",
                "required": True,
                "label": "Primary Contact",
                "constraints": {"unique": True}
            }
        ],
        "relationships": []
    }
    
    from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
    converter = PropertyToRelationshipConverter()
    processed = converter.process_class_data(class_data)
    
    # 제약조건이 유지되는지 확인
    properties = {p["name"]: p for p in processed["properties"]}
    
    assert "pattern" in properties["code"].get("constraints", {})
    assert "min" in properties["amount"].get("constraints", {})
    assert "enum" in properties["status"].get("constraints", {})
    
    logger.info("✅ 시나리오 3 통과!")
    return True


async def test_scenario_4_circular_references():
    """시나리오 4: 순환 참조 처리"""
    logger.info("🔥 시나리오 4: 순환 참조 처리 테스트")
    
    # 서로를 참조하는 클래스들
    class_data_a = {
        "id": "ClassA",
        "label": "Class A",
        "properties": [
            {"name": "name", "type": "string", "required": True, "label": "Name"},
            {"name": "b_reference", "type": "link", "linkTarget": "ClassB", "required": False, "label": "B Reference"}
        ],
        "relationships": []
    }
    
    class_data_b = {
        "id": "ClassB",
        "label": "Class B",
        "properties": [
            {"name": "name", "type": "string", "required": True, "label": "Name"},
            {"name": "a_reference", "type": "link", "linkTarget": "ClassA", "required": False, "label": "A Reference"}
        ],
        "relationships": []
    }
    
    from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
    converter = PropertyToRelationshipConverter()
    
    processed_a = converter.process_class_data(class_data_a)
    processed_b = converter.process_class_data(class_data_b)
    
    # 순환 참조가 올바르게 변환되는지 확인
    assert any(r["predicate"] == "b_reference" for r in processed_a["relationships"])
    assert any(r["predicate"] == "a_reference" for r in processed_b["relationships"])
    
    logger.info("✅ 시나리오 4 통과!")
    return True


async def test_scenario_5_cardinality_variations():
    """시나리오 5: 다양한 카디널리티 테스트"""
    logger.info("🔥 시나리오 5: 다양한 카디널리티 테스트")
    
    test_cases = [
        ("1:1", "Optional"),
        ("1:n", "Set"),
        ("n:1", "Optional"),
        ("n:n", "Set"),
        ("n:m", "Set"),  # n:m도 지원
        ("m:n", "Set"),
        ("one", "Optional"),
        ("many", "Set")
    ]
    
    from oms.utils.terminus_schema_types import TerminusSchemaConverter
    converter = TerminusSchemaConverter()
    
    for cardinality, expected_type in test_cases:
        result = converter.convert_relationship_cardinality(cardinality)
        assert result["@type"] == expected_type, f"Failed for {cardinality}"
        logger.info(f"  ✅ {cardinality} → {expected_type}")
    
    logger.info("✅ 시나리오 5 통과!")
    return True


async def test_scenario_6_terminus_integration():
    """시나리오 6: TerminusDB 통합 테스트"""
    logger.info("🔥 시나리오 6: TerminusDB 통합 테스트")
    
    # TerminusDB 연결 설정
    connection_config = ConnectionConfig(
        server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
        user=os.getenv("TERMINUS_USER", "admin"),
        account=os.getenv("TERMINUS_ACCOUNT", "admin"),
        key=os.getenv("TERMINUS_KEY", "admin123"),
    )
    
    terminus = AsyncTerminusService(connection_config)
    test_db = f"test_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # 데이터베이스 생성
        await terminus.create_database(test_db, "Validation test database")
        
        # 참조 클래스들 먼저 생성
        reference_classes = [
            {"id": "User", "label": "User", "properties": [{"name": "username", "type": "string", "required": True, "label": "Username"}]},
            {"id": "Category", "label": "Category", "properties": [{"name": "name", "type": "string", "required": True, "label": "Name"}]},
            {"id": "Tag", "label": "Tag", "properties": [{"name": "label", "type": "string", "required": True, "label": "Label"}]}
        ]
        
        for ref_class in reference_classes:
            await terminus.create_ontology_class(test_db, ref_class)
        
        # 복잡한 클래스 생성
        complex_class = {
            "id": "Article",
            "label": "Article",
            "description": "Article with mixed properties and relationships",
            "properties": [
                {"name": "title", "type": "string", "required": True, "label": "Title"},
                {"name": "content", "type": "string", "required": True, "label": "Content"},
                {"name": "author", "type": "link", "linkTarget": "User", "required": True, "label": "Author"},
                {"name": "category", "type": "link", "linkTarget": "Category", "required": False, "label": "Category"},
                {"name": "tags", "type": "list<string>", "required": False, "label": "Tags"}
            ],
            "relationships": [
                {
                    "predicate": "references",
                    "target": "Article",
                    "cardinality": "n:n",
                    "label": "References"
                },
                {
                    "predicate": "tagged_entities",
                    "target": "Tag",
                    "cardinality": "n:n",
                    "label": "Tagged Entities"
                }
            ]
        }
        
        # 클래스 생성
        result = await terminus.create_ontology_class(test_db, complex_class)
        logger.info("✅ 복잡한 클래스 생성 성공")
        
        # 생성된 클래스 조회
        retrieved = await terminus.get_ontology(test_db, "Article")
        
        # 속성 검증
        properties = {p["name"]: p for p in retrieved.get("properties", [])}
        relationships = {r["predicate"]: r for r in retrieved.get("relationships", [])}
        
        # 일반 속성 확인
        assert "title" in properties
        assert "content" in properties
        assert "tags" in properties
        
        # linkTarget 속성 확인 (property로 표시)
        assert "author" in properties
        assert properties["author"]["type"] == "link"
        assert properties["author"]["linkTarget"] == "User"
        
        assert "category" in properties
        assert properties["category"]["type"] == "link"
        assert properties["category"]["linkTarget"] == "Category"
        
        # 명시적 관계 확인
        assert "references" in relationships
        assert "tagged_entities" in relationships
        
        logger.info("✅ 시나리오 6 통과!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 통합 테스트 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
        
    finally:
        # 정리
        try:
            await terminus.delete_database(test_db)
        except:
            pass


async def test_scenario_7_edge_cases():
    """시나리오 7: 엣지 케이스 테스트"""
    logger.info("🔥 시나리오 7: 엣지 케이스 테스트")
    
    edge_cases = [
        # 빈 클래스
        {
            "id": "EmptyClass",
            "label": "Empty Class",
            "properties": [],
            "relationships": []
        },
        # 속성만 있는 클래스
        {
            "id": "PropertiesOnly",
            "label": "Properties Only",
            "properties": [
                {"name": "prop1", "type": "string", "label": "Property 1"},
                {"name": "prop2", "type": "integer", "label": "Property 2"}
            ],
            "relationships": []
        },
        # 관계만 있는 클래스
        {
            "id": "RelationshipsOnly",
            "label": "Relationships Only",
            "properties": [],
            "relationships": [
                {"predicate": "rel1", "target": "Target1", "cardinality": "n:1"},
                {"predicate": "rel2", "target": "Target2", "cardinality": "1:n"}
            ]
        },
        # 동일한 타겟을 가진 여러 관계
        {
            "id": "MultipleToSameTarget",
            "properties": [
                {"name": "created_by", "type": "link", "linkTarget": "User", "label": "Created By"},
                {"name": "updated_by", "type": "link", "linkTarget": "User", "label": "Updated By"},
                {"name": "approved_by", "type": "link", "linkTarget": "User", "label": "Approved By"}
            ],
            "relationships": []
        }
    ]
    
    from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
    converter = PropertyToRelationshipConverter()
    
    for edge_case in edge_cases:
        try:
            processed = converter.process_class_data(edge_case)
            logger.info(f"  ✅ {edge_case['id']}: "
                       f"Properties {len(edge_case.get('properties', []))}→{len(processed['properties'])}, "
                       f"Relationships {len(edge_case.get('relationships', []))}→{len(processed['relationships'])}")
        except Exception as e:
            logger.error(f"  ❌ {edge_case['id']}: {e}")
            return False
    
    logger.info("✅ 시나리오 7 통과!")
    return True


async def main():
    """메인 테스트 실행"""
    logger.info("=" * 80)
    logger.info("🔥 ULTRA! Property와 Relationship 통합 검증 시작")
    logger.info("=" * 80)
    
    scenarios = [
        test_scenario_1_basic_property_conversion,
        test_scenario_2_mixed_properties_relationships,
        test_scenario_3_complex_constraints,
        test_scenario_4_circular_references,
        test_scenario_5_cardinality_variations,
        test_scenario_6_terminus_integration,
        test_scenario_7_edge_cases
    ]
    
    passed = 0
    failed = 0
    
    for i, scenario in enumerate(scenarios, 1):
        logger.info(f"\n📋 시나리오 {i}/{len(scenarios)}")
        try:
            result = await scenario()
            if result:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            logger.error(f"❌ 시나리오 {i} 예외 발생: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    logger.info("\n" + "=" * 80)
    logger.info(f"📊 최종 결과: {passed}/{len(scenarios)} 시나리오 통과")
    logger.info("=" * 80)
    
    if failed == 0:
        logger.info("🎉 모든 통합 검증 테스트 통과!")
        return 0
    else:
        logger.error(f"❌ {failed}개 시나리오 실패")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)