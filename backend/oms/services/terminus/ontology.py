"""
Ontology Service for TerminusDB
온톨로지/스키마 CRUD 작업 서비스
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from .base import BaseTerminusService
from .database import DatabaseService
from oms.exceptions import (
    DatabaseError,
    DuplicateOntologyError,
    OntologyNotFoundError,
    OntologyValidationError
)
from shared.models.ontology import OntologyBase, OntologyResponse, Property, Relationship
from shared.models.common import DataType

logger = logging.getLogger(__name__)


class OntologyService(BaseTerminusService):
    """
    TerminusDB 온톨로지 관리 서비스
    
    온톨로지(스키마) 생성, 수정, 삭제 및 조회 기능을 제공합니다.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # DatabaseService 인스턴스 (데이터베이스 존재 확인용)
        self.db_service = DatabaseService(*args, **kwargs)
    
    async def create_ontology(
        self,
        db_name: str,
        ontology: OntologyBase
    ) -> OntologyResponse:
        """
        새 온톨로지(클래스) 생성
        
        Args:
            db_name: 데이터베이스 이름
            ontology: 온톨로지 정보
            
        Returns:
            생성된 온톨로지 응답
        """
        try:
            # 데이터베이스 존재 확인
            await self.db_service.ensure_db_exists(db_name)
            
            # 중복 확인
            if await self.ontology_exists(db_name, ontology.id):
                raise DuplicateOntologyError(f"온톨로지 '{ontology.id}'이(가) 이미 존재합니다")
            
            # 스키마 문서 생성
            schema_doc = {
                "@type": "Class",
                "@id": ontology.id,
                "@documentation": {
                    "@comment": ontology.description or f"{ontology.name} class",
                    "@label": ontology.name
                }
            }
            
            # 속성 추가
            if ontology.properties:
                for prop in ontology.properties:
                    prop_schema = self._create_property_schema(prop)
                    schema_doc[prop.name] = prop_schema
            
            # 관계 추가
            if ontology.relationships:
                for rel in ontology.relationships:
                    rel_schema = self._create_relationship_schema(rel)
                    schema_doc[rel.name] = rel_schema
            
            # 스키마 저장
            endpoint = f"/api/document/{self.connection_info.account}/{db_name}?graph_type=schema"
            
            await self._make_request("POST", endpoint, schema_doc)
            
            logger.info(f"Ontology '{ontology.id}' created in database '{db_name}'")
            
            # 응답 생성
            return OntologyResponse(
                **ontology.dict(),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
        except DuplicateOntologyError:
            raise
        except Exception as e:
            logger.error(f"Failed to create ontology: {e}")
            raise DatabaseError(f"온톨로지 생성 실패: {e}")
    
    async def update_ontology(
        self,
        db_name: str,
        ontology_id: str,
        ontology: OntologyBase
    ) -> OntologyResponse:
        """
        온톨로지 업데이트
        
        Args:
            db_name: 데이터베이스 이름
            ontology_id: 기존 온톨로지 ID
            ontology: 업데이트할 온톨로지 정보
            
        Returns:
            업데이트된 온톨로지 응답
        """
        try:
            # 존재 확인
            if not await self.ontology_exists(db_name, ontology_id):
                raise OntologyNotFoundError(f"온톨로지 '{ontology_id}'을(를) 찾을 수 없습니다")
            
            # 기존 스키마 삭제
            await self.delete_ontology(db_name, ontology_id)
            
            # 새 스키마 생성
            return await self.create_ontology(db_name, ontology)
            
        except (OntologyNotFoundError, DuplicateOntologyError):
            raise
        except Exception as e:
            logger.error(f"Failed to update ontology: {e}")
            raise DatabaseError(f"온톨로지 업데이트 실패: {e}")
    
    async def delete_ontology(
        self,
        db_name: str,
        ontology_id: str
    ) -> bool:
        """
        온톨로지 삭제
        
        Args:
            db_name: 데이터베이스 이름
            ontology_id: 온톨로지 ID
            
        Returns:
            삭제 성공 여부
        """
        try:
            # 존재 확인
            if not await self.ontology_exists(db_name, ontology_id):
                raise OntologyNotFoundError(f"온톨로지 '{ontology_id}'을(를) 찾을 수 없습니다")
            
            # 스키마 문서 삭제
            endpoint = f"/api/document/{self.connection_info.account}/{db_name}?graph_type=schema&id={ontology_id}"
            
            await self._make_request("DELETE", endpoint)
            
            logger.info(f"Ontology '{ontology_id}' deleted from database '{db_name}'")
            return True
            
        except OntologyNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Failed to delete ontology: {e}")
            raise DatabaseError(f"온톨로지 삭제 실패: {e}")
    
    async def get_ontology(
        self,
        db_name: str,
        ontology_id: str
    ) -> Optional[OntologyResponse]:
        """
        특정 온톨로지 조회
        
        Args:
            db_name: 데이터베이스 이름
            ontology_id: 온톨로지 ID
            
        Returns:
            온톨로지 정보 또는 None
        """
        try:
            # 스키마 문서 조회
            endpoint = f"/api/document/{self.connection_info.account}/{db_name}?graph_type=schema&id={ontology_id}"
            
            result = await self._make_request("GET", endpoint)
            
            if not result:
                return None
            
            # 결과 파싱
            return self._parse_ontology_document(result)
            
        except Exception as e:
            logger.error(f"Failed to get ontology: {e}")
            return None
    
    async def list_ontologies(
        self,
        db_name: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[OntologyResponse]:
        """
        데이터베이스의 모든 온톨로지 목록 조회
        
        Args:
            db_name: 데이터베이스 이름
            limit: 최대 결과 수
            offset: 시작 위치
            
        Returns:
            온톨로지 목록
        """
        try:
            # 데이터베이스 존재 확인
            await self.db_service.ensure_db_exists(db_name)
            
            # 모든 클래스 조회 SPARQL 쿼리
            sparql_query = f"""
            SELECT ?class ?label ?comment
            WHERE {{
                ?class a owl:Class .
                OPTIONAL {{ ?class rdfs:label ?label }}
                OPTIONAL {{ ?class rdfs:comment ?comment }}
            }}
            LIMIT {limit}
            OFFSET {offset}
            """
            
            endpoint = f"/api/sparql/{self.connection_info.account}/{db_name}"
            result = await self._make_request("POST", endpoint, {"query": sparql_query})
            
            ontologies = []
            
            if isinstance(result, dict) and "results" in result:
                bindings = result.get("results", {}).get("bindings", [])
                
                for binding in bindings:
                    class_uri = binding.get("class", {}).get("value", "")
                    class_id = class_uri.split("/")[-1] if "/" in class_uri else class_uri
                    
                    ontology = OntologyResponse(
                        id=class_id,
                        name=binding.get("label", {}).get("value", class_id),
                        description=binding.get("comment", {}).get("value", ""),
                        properties=[],
                        relationships=[],
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                    
                    # 각 온톨로지의 상세 정보 가져오기
                    detailed = await self.get_ontology(db_name, class_id)
                    if detailed:
                        ontology = detailed
                    
                    ontologies.append(ontology)
            
            return ontologies
            
        except Exception as e:
            logger.error(f"Failed to list ontologies: {e}")
            raise DatabaseError(f"온톨로지 목록 조회 실패: {e}")
    
    async def ontology_exists(
        self,
        db_name: str,
        ontology_id: str
    ) -> bool:
        """
        온톨로지 존재 여부 확인
        
        Args:
            db_name: 데이터베이스 이름
            ontology_id: 온톨로지 ID
            
        Returns:
            존재 여부
        """
        try:
            ontology = await self.get_ontology(db_name, ontology_id)
            return ontology is not None
        except Exception:
            return False
    
    def _create_property_schema(self, prop: Property) -> Dict[str, Any]:
        """속성 스키마 생성"""
        schema = {
            "@type": "Optional",
            "@class": self._map_datatype_to_terminus(prop.type),
            "@documentation": {
                "@comment": prop.description or f"{prop.name} property",
                "@label": prop.name
            }
        }
        
        # 필수 속성인 경우
        if prop.required:
            schema.pop("@type")
            schema["@type"] = self._map_datatype_to_terminus(prop.type)
        
        # 배열인 경우
        if prop.is_array:
            base_schema = schema.copy()
            schema = {
                "@type": "List",
                "@class": base_schema
            }
        
        return schema
    
    def _create_relationship_schema(self, rel: Relationship) -> Dict[str, Any]:
        """관계 스키마 생성"""
        schema = {
            "@type": "Optional",
            "@class": rel.target_ontology,
            "@documentation": {
                "@comment": rel.description or f"{rel.name} relationship",
                "@label": rel.name
            }
        }
        
        # 필수 관계인 경우
        if rel.required:
            schema.pop("@type")
            schema["@type"] = rel.target_ontology
        
        # 다중 관계인 경우
        if rel.is_array:
            base_schema = schema.copy()
            schema = {
                "@type": "Set",
                "@class": base_schema
            }
        
        return schema
    
    def _map_datatype_to_terminus(self, datatype: DataType) -> str:
        """DataType을 TerminusDB 타입으로 매핑"""
        mapping = {
            DataType.STRING: "xsd:string",
            DataType.INTEGER: "xsd:integer",
            DataType.NUMBER: "xsd:decimal",
            DataType.BOOLEAN: "xsd:boolean",
            DataType.DATE: "xsd:date",
            DataType.DATETIME: "xsd:dateTime",
            DataType.OBJECT: "sys:JSON",
            DataType.ARRAY: "sys:JSON"
        }
        return mapping.get(datatype, "xsd:string")
    
    def _parse_ontology_document(self, doc: Dict[str, Any]) -> OntologyResponse:
        """TerminusDB 문서를 OntologyResponse로 파싱"""
        # 기본 정보 추출
        ontology_id = doc.get("@id", "")
        documentation = doc.get("@documentation", {})
        
        ontology = OntologyResponse(
            id=ontology_id,
            name=documentation.get("@label", ontology_id),
            description=documentation.get("@comment", ""),
            properties=[],
            relationships=[],
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # 속성과 관계 파싱
        for key, value in doc.items():
            if key.startswith("@"):
                continue
            
            if isinstance(value, dict):
                # 관계인지 속성인지 판단
                if "@class" in value:
                    class_type = value.get("@class", "")
                    
                    # TerminusDB 기본 타입이면 속성
                    if class_type.startswith("xsd:") or class_type.startswith("sys:"):
                        prop = Property(
                            name=key,
                            type=self._map_terminus_to_datatype(class_type),
                            description=value.get("@documentation", {}).get("@comment", ""),
                            required=value.get("@type") != "Optional",
                            is_array=False
                        )
                        ontology.properties.append(prop)
                    else:
                        # 사용자 정의 클래스면 관계
                        rel = Relationship(
                            name=key,
                            target_ontology=class_type,
                            description=value.get("@documentation", {}).get("@comment", ""),
                            required=value.get("@type") != "Optional",
                            is_array=value.get("@type") in ["Set", "List"]
                        )
                        ontology.relationships.append(rel)
        
        return ontology
    
    def _map_terminus_to_datatype(self, terminus_type: str) -> DataType:
        """TerminusDB 타입을 DataType으로 매핑"""
        mapping = {
            "xsd:string": DataType.STRING,
            "xsd:integer": DataType.INTEGER,
            "xsd:decimal": DataType.NUMBER,
            "xsd:boolean": DataType.BOOLEAN,
            "xsd:date": DataType.DATE,
            "xsd:dateTime": DataType.DATETIME,
            "sys:JSON": DataType.OBJECT
        }
        return mapping.get(terminus_type, DataType.STRING)