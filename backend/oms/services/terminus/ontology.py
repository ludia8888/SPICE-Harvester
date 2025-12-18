"""
Ontology Service for TerminusDB
온톨로지/스키마 CRUD 작업 서비스
"""

import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone

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
from shared.utils.language import coerce_localized_text, select_localized_text

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

    async def disconnect(self) -> None:
        # Close nested db_service first to avoid leaking its httpx client in short-lived contexts/tests.
        try:
            await self.db_service.disconnect()
        finally:
            await super().disconnect()
    
    async def create_ontology(
        self,
        db_name: str,
        ontology: OntologyBase,
        branch: str = "main",
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
            if await self.ontology_exists(db_name, ontology.id, branch=branch):
                raise DuplicateOntologyError(f"온톨로지 '{ontology.id}'이(가) 이미 존재합니다")
            
            label_i18n = coerce_localized_text(ontology.label)
            description_i18n = coerce_localized_text(ontology.description) if ontology.description else {}

            label_display = select_localized_text(label_i18n, lang="en") or select_localized_text(label_i18n, lang="ko") or ontology.id
            description_display = (
                select_localized_text(description_i18n, lang="en")
                or select_localized_text(description_i18n, lang="ko")
                or f"{label_display} class"
            )

            # 스키마 문서 생성 - 🔥 CRITICAL FIX: Correct TerminusDB format
            schema_doc = {
                "@type": "Class",
                "@id": ontology.id,
                "@key": {
                    "@type": "Random"
                },
                "@documentation": {
                    "@comment": description_display,
                    "@label": label_display,
                    # Extra fields are safe to store in TerminusDB and allow EN/KR round-trip.
                    "@comment_i18n": description_i18n,
                    "@label_i18n": label_i18n,
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
                    schema_doc[rel.predicate] = rel_schema  # Relationship uses 'predicate', not 'name'
            
            # 스키마 저장
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {
                "graph_type": "schema",
                "author": self.connection_info.user or "admin",
                "message": f"Creating ontology {ontology.id}",
            }
            
            # Create schema document in TerminusDB
            response = await self._make_request("POST", endpoint, schema_doc, params=params)
            
            logger.info(f"Ontology '{ontology.id}' created in database '{db_name}'")
            
            # 응답 생성
            # ontology.model_dump() already contains created_at and updated_at from OntologyBase
            ontology_dict = ontology.model_dump()
            # Only set timestamps if they don't exist
            if 'created_at' not in ontology_dict or ontology_dict['created_at'] is None:
                ontology_dict['created_at'] = datetime.now(timezone.utc)
            if 'updated_at' not in ontology_dict or ontology_dict['updated_at'] is None:
                ontology_dict['updated_at'] = datetime.now(timezone.utc)
            
            return OntologyResponse(**ontology_dict)
            
        except DuplicateOntologyError:
            raise
        except Exception as e:
            logger.error(f"Failed to create ontology: {e}")
            raise DatabaseError(f"온톨로지 생성 실패: {e}")
    
    async def update_ontology(
        self,
        db_name: str,
        ontology_id: str,
        ontology: OntologyBase,
        branch: str = "main",
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
            if not await self.ontology_exists(db_name, ontology_id, branch=branch):
                raise OntologyNotFoundError(f"온톨로지 '{ontology_id}'을(를) 찾을 수 없습니다")
            
            # 기존 스키마 삭제
            await self.delete_ontology(db_name, ontology_id, branch=branch)
            
            # 새 스키마 생성
            return await self.create_ontology(db_name, ontology, branch=branch)
            
        except (OntologyNotFoundError, DuplicateOntologyError):
            raise
        except Exception as e:
            logger.error(f"Failed to update ontology: {e}")
            raise DatabaseError(f"온톨로지 업데이트 실패: {e}")
    
    async def delete_ontology(
        self,
        db_name: str,
        ontology_id: str,
        branch: str = "main",
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
            if not await self.ontology_exists(db_name, ontology_id, branch=branch):
                raise OntologyNotFoundError(f"온톨로지 '{ontology_id}'을(를) 찾을 수 없습니다")
            
            # 스키마 문서 삭제
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {
                "graph_type": "schema",
                "id": ontology_id,
                "author": self.connection_info.user or "admin",
                "message": f"Deleting ontology {ontology_id}",
            }
            
            await self._make_request("DELETE", endpoint, params=params)
            
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
        ontology_id: str,
        branch: str = "main",
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
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {"graph_type": "schema", "id": ontology_id}
            
            result = await self._make_request("GET", endpoint, params=params)
            
            if not result:
                return None
            
            # Handle JSONL response - if result is a list, take the first element
            if isinstance(result, list):
                if len(result) == 0:
                    return None
                result = result[0]
            
            # 결과 파싱
            return self._parse_ontology_document(result)
            
        except Exception as e:
            logger.error(f"Failed to get ontology: {e}")
            return None
    
    async def list_ontologies(
        self,
        db_name: str,
        branch: str = "main",
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
            
            # Use document API to get all schema documents
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {"graph_type": "schema"}
            
            try:
                result = await self._make_request("GET", endpoint, params=params)
            except Exception as e:
                # If no schema documents exist, return empty list
                if "404" in str(e):
                    return []
                raise
            
            ontologies = []
            
            # Handle the result - it should be a list of schema documents
            if isinstance(result, list):
                # Apply offset and limit manually
                start_idx = offset
                end_idx = offset + limit
                
                for doc in result[start_idx:end_idx]:
                    if isinstance(doc, dict) and "@type" in doc and doc["@type"] == "Class":
                        ontology = self._parse_ontology_document(doc)
                        ontologies.append(ontology)
            elif isinstance(result, dict) and "@type" in result and result["@type"] == "Class":
                # Single document returned
                if offset == 0 and limit > 0:
                    ontology = self._parse_ontology_document(result)
                    ontologies.append(ontology)
            
            return ontologies
            
        except Exception as e:
            logger.error(f"Failed to list ontologies: {e}")
            raise DatabaseError(f"온톨로지 목록 조회 실패: {e}")
    
    async def ontology_exists(
        self,
        db_name: str,
        ontology_id: str,
        branch: str = "main",
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
            ontology = await self.get_ontology(db_name, ontology_id, branch=branch)
            return ontology is not None
        except Exception:
            return False
    
    def _create_property_schema(self, prop: Property) -> Dict[str, Any]:
        """속성 스키마 생성 - TerminusDB 공식 패턴 준수"""
        # TerminusDB 공식 패턴:
        # - 스칼라 타입은 직접 지정 (예: "xsd:string")
        # - Optional이 필요한 경우만 {"@type": "Optional", "@class": "xsd:string"}
        
        mapped_type = self._map_datatype_to_terminus(prop.type)
        
        # 필수 속성: 타입 직접 반환
        if prop.required:
            return mapped_type  # "xsd:string" 형태로 직접 반환
        else:
            # 옵셔널 속성: Optional로 감싸기
            return {
                "@type": "Optional",
                "@class": mapped_type
            }
    
    def _create_relationship_schema(self, rel: Relationship) -> Dict[str, Any]:
        """관계 스키마 생성 - TerminusDB 공식 패턴 준수"""
        # TerminusDB 공식 패턴:
        # - 1:1 필수: "Person" (타겟 클래스 직접 지정)
        # - 1:1 옵셔널: {"@type": "Optional", "@class": "Person"}
        # - 1:N: {"@type": "Set", "@class": "Person"}
        # - N:M: {"@type": "Set", "@class": "Person"}
        
        # 카디널리티 분석
        cardinality = rel.cardinality or "n:1"  # 기본값: n:1
        
        # 다중 관계 (1:n, n:m)
        if cardinality.endswith(":n") or cardinality.endswith(":m"):
            return {
                "@type": "Set",  # 집합 타입 (순서/중복 없음)
                "@class": rel.target
            }
        
        # 단일 관계 (1:1, n:1)
        # Relationship 모델에 required 필드가 없으므로 기본적으로 Optional로 처리
        # 하지만 특정 패턴 검사로 필수 여부 판단 가능
        required = getattr(rel, 'required', False)
        
        if required:
            # 필수 관계: 타겟 클래스 직접 반환
            return rel.target
        else:
            # 옵셔널 관계
            return {
                "@type": "Optional",
                "@class": rel.target
            }
    
    def _map_datatype_to_terminus(self, datatype: Union[DataType, str]) -> str:
        """DataType을 TerminusDB 타입으로 매핑"""
        
        # 🔥 CRITICAL: TerminusDB doesn't support xsd: types, use sys: types instead
        # Handle both DataType enum and string inputs
        if isinstance(datatype, str):
            # Handle string inputs from tests/API calls
            string_mapping = {
                "string": "xsd:string",  # Use proper XSD types
                "integer": "xsd:integer",
                "decimal": "xsd:decimal", 
                "number": "xsd:decimal",  # 🔥 CRITICAL FIX: map number to xsd:decimal
                "float": "xsd:float",
                "double": "xsd:double",
                "boolean": "xsd:boolean",
                "date": "xsd:date",
                "datetime": "xsd:dateTime",
                "email": "xsd:string",
                "phone": "xsd:string",
                "xsd:string": "xsd:string",  # Keep XSD types as is
                "xsd:integer": "xsd:integer",
                "xsd:date": "xsd:date",
                "xsd:boolean": "xsd:boolean",
                "xsd:decimal": "xsd:decimal",
                "xsd:dateTime": "xsd:dateTime"
            }
            return string_mapping.get(datatype, "sys:JSON")
        
        # Handle DataType enum inputs (legacy)
        enum_mapping = {
            DataType.STRING: "sys:JSON",
            DataType.INTEGER: "sys:JSON", 
            DataType.DECIMAL: "sys:JSON",
            DataType.FLOAT: "sys:JSON",
            DataType.DOUBLE: "sys:JSON", 
            DataType.BOOLEAN: "sys:JSON",
            DataType.DATE: "sys:JSON",
            DataType.DATETIME: "sys:JSON",
            DataType.OBJECT: "sys:JSON",
            DataType.ARRAY: "sys:JSON"
        }
        return enum_mapping.get(datatype, "sys:JSON")
    
    def _parse_ontology_document(self, doc: Dict[str, Any]) -> OntologyResponse:
        """TerminusDB 문서를 OntologyResponse로 파싱"""
        # 기본 정보 추출
        ontology_id = doc.get("@id", "")
        documentation = doc.get("@documentation", {})

        label_i18n = documentation.get("@label_i18n")
        comment_i18n = documentation.get("@comment_i18n")
        
        ontology = OntologyResponse(
            id=ontology_id,
            label=label_i18n if isinstance(label_i18n, dict) and label_i18n else documentation.get("@label", ontology_id),
            description=comment_i18n if isinstance(comment_i18n, dict) and comment_i18n else documentation.get("@comment", ""),
            properties=[],
            relationships=[],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        
        # 속성과 관계 파싱
        for key, value in doc.items():
            if key.startswith("@"):
                continue
            
            if isinstance(value, str):
                # Required scalar properties are stored as a bare type string (e.g. "xsd:string")
                if value.startswith("xsd:") or value.startswith("sys:"):
                    ontology.properties.append(
                        Property(
                            name=key,
                            type=value,
                            label=key,
                            description="",
                            required=True,
                        )
                    )
                else:
                    # Required relationship can be stored as a bare class reference string.
                    ontology.relationships.append(
                        Relationship(
                            predicate=key,
                            target=value,
                            label=key,
                            description="",
                            cardinality="1:1",
                        )
                    )
            elif isinstance(value, dict):
                # 관계인지 속성인지 판단
                if "@class" in value:
                    class_type = value.get("@class", "")
                    
                    # TerminusDB 기본 타입이면 속성
                    if class_type.startswith("xsd:") or class_type.startswith("sys:"):
                        prop = Property(
                            name=key,
                            type=class_type,  # Keep the original type string like "xsd:string"
                            label=value.get("@documentation", {}).get("@label", key),
                            description=value.get("@documentation", {}).get("@comment", ""),
                            required=value.get("@type") != "Optional"
                        )
                        ontology.properties.append(prop)
                    else:
                        # 사용자 정의 클래스면 관계
                        rel = Relationship(
                            predicate=key,
                            target=class_type,
                            label=value.get("@documentation", {}).get("@label", key),
                            description=value.get("@documentation", {}).get("@comment", ""),
                            cardinality="n:m" if value.get("@type") == "Set" else "1:n"
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
