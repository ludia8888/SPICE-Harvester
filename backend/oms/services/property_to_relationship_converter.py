"""
Property to Relationship 자동 변환기
사용자가 클래스 내부에서 정의한 속성을 관계로 자동 변환
"""

import logging
from typing import Dict, List, Tuple, Optional, Any

from shared.models.ontology import Property, Relationship

logger = logging.getLogger(__name__)


class PropertyToRelationshipConverter:
    """
    Property를 Relationship으로 자동 변환하는 컨버터
    
    사용자는 클래스 정의 내부에서 간단하게 속성을 정의하지만,
    내부적으로는 ObjectProperty로 변환되어 관계로 관리됨
    """
    
    def __init__(self):
        self.logger = logger
        
    def process_class_data(self, class_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        클래스 데이터를 처리하여 property를 relationship으로 자동 변환
        
        Args:
            class_data: 원본 클래스 데이터
            
        Returns:
            처리된 클래스 데이터 (properties와 relationships가 분리됨)
        """
        logger.info(f"🔄 Processing class data for property→relationship conversion: {class_data.get('id')}")
        
        # 복사본 생성
        processed_data = class_data.copy()
        
        # properties와 relationships 초기화
        properties = class_data.get("properties", [])
        relationships = class_data.get("relationships", [])
        
        # 변환된 속성과 관계를 저장할 리스트
        final_properties = []
        converted_relationships = []
        
        # 각 property 검사 및 변환
        for prop_data in properties:
            if isinstance(prop_data, dict):
                prop = Property(**prop_data)
            else:
                prop = prop_data
                
            # 클래스 참조인지 확인
            if prop.is_class_reference():
                logger.info(f"🔗 Converting property '{prop.name}' to relationship (target: {prop.linkTarget or prop.type})")
                
                # Property를 Relationship으로 변환
                relationship_data = prop.to_relationship()
                
                # 🔥 THINK ULTRA! Property에서 변환된 relationship임을 표시
                relationship_data["_converted_from_property"] = True
                relationship_data["_original_property_name"] = prop.name
                
                converted_relationships.append(relationship_data)
                
                # 변환 정보 로깅
                logger.debug(f"✅ Converted property to relationship: {relationship_data}")
            else:
                # 일반 속성은 그대로 유지
                final_properties.append(prop_data if isinstance(prop_data, dict) else prop.model_dump())
                
        # 기존 relationships와 변환된 relationships 병합
        all_relationships = relationships + converted_relationships
        
        # 중복 제거 (같은 predicate가 있으면 하나만 유지)
        unique_relationships = {}
        for rel in all_relationships:
            predicate = rel.get("predicate")
            if predicate:
                # 나중에 추가된 것이 우선 (사용자가 명시적으로 정의한 경우)
                unique_relationships[predicate] = rel
                
        # 최종 데이터 업데이트
        processed_data["properties"] = final_properties
        processed_data["relationships"] = list(unique_relationships.values())
        
        # 변환 통계 로깅
        logger.info(f"📊 Conversion complete: {len(properties)} properties → {len(final_properties)} properties + {len(converted_relationships)} relationships")
        logger.info(f"📊 Total relationships: {len(processed_data['relationships'])} (after deduplication)")
        
        return processed_data
    
    def detect_class_references(self, properties: List[Property]) -> List[Tuple[Property, str]]:
        """
        속성 목록에서 클래스 참조를 감지
        
        Returns:
            List of (Property, target_class) tuples
        """
        class_references = []
        
        for prop in properties:
            if prop.is_class_reference():
                target = prop.linkTarget or prop.type
                class_references.append((prop, target))
                
        return class_references
    
    def validate_class_references(self, class_data: Dict[str, Any], existing_classes: List[str]) -> List[str]:
        """
        클래스 참조의 유효성 검증
        
        Args:
            class_data: 검증할 클래스 데이터
            existing_classes: 존재하는 클래스 목록
            
        Returns:
            오류 메시지 목록
        """
        errors = []
        properties = class_data.get("properties", [])
        
        for prop_data in properties:
            if isinstance(prop_data, dict):
                prop = Property(**prop_data)
            else:
                prop = prop_data
                
            if prop.is_class_reference():
                target = prop.linkTarget or prop.type
                if target not in existing_classes:
                    errors.append(f"Property '{prop.name}' references non-existent class: {target}")
                    
        return errors
    
    def generate_inverse_relationships(self, class_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        자동 변환된 관계에 대한 역관계 생성 정보
        
        Returns:
            역관계 정보 목록 (대상 클래스에 추가되어야 함)
        """
        inverse_relationships = []
        class_id = class_data.get("id")
        
        # 변환된 관계에서 역관계 생성
        for rel in class_data.get("relationships", []):
            if rel.get("inverse_predicate"):
                inverse_rel = {
                    "target_class": rel.get("target"),
                    "relationship": {
                        "predicate": rel.get("inverse_predicate"),
                        "target": class_id,
                        "label": rel.get("inverse_label", {"en": f"Inverse of {rel.get('predicate')}"}),
                        "cardinality": self._inverse_cardinality(rel.get("cardinality", "1:n"))
                    }
                }
                inverse_relationships.append(inverse_rel)
                
        return inverse_relationships
    
    def _inverse_cardinality(self, cardinality: str) -> str:
        """카디널리티 역변환"""
        mapping = {
            "1:1": "1:1",
            "1:n": "n:1",
            "n:1": "1:n",
            "n:m": "n:m"
        }
        return mapping.get(cardinality, "n:1")