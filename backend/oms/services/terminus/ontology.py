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

            documentation = {
                "@comment": description_display,
                "@label": label_display,
                # Extra fields are safe to store in TerminusDB and allow EN/KR round-trip.
                "@comment_i18n": description_i18n,
                "@label_i18n": label_i18n,
            }

            metadata_payload = ontology.metadata if isinstance(ontology.metadata, dict) else {}
            if metadata_payload:
                documentation["metadata"] = metadata_payload
                if metadata_payload.get("internal"):
                    documentation["@internal"] = True

            # 스키마 문서 생성 - TerminusDB format
            schema_doc = {
                "@type": "Class",
                "@id": ontology.id,
                "@key": {
                    "@type": "Random"
                },
                "@documentation": documentation,
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

            def _is_internal(doc: Dict[str, Any]) -> bool:
                documentation = doc.get("@documentation", {}) if isinstance(doc, dict) else {}
                return bool(isinstance(documentation, dict) and documentation.get("@internal"))
            
            # Handle the result - it should be a list of schema documents
            if isinstance(result, list):
                # Apply offset and limit manually
                start_idx = offset
                end_idx = offset + limit
                
                for doc in result[start_idx:end_idx]:
                    if (
                        isinstance(doc, dict)
                        and "@type" in doc
                        and doc["@type"] == "Class"
                        and not _is_internal(doc)
                    ):
                        ontology = self._parse_ontology_document(doc)
                        ontologies.append(ontology)
            elif (
                isinstance(result, dict)
                and "@type" in result
                and result["@type"] == "Class"
                and not _is_internal(result)
            ):
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

        label_i18n = coerce_localized_text(prop.label)
        description_i18n = coerce_localized_text(prop.description) if prop.description else {}

        label_display = (
            select_localized_text(label_i18n, lang="en")
            or select_localized_text(label_i18n, lang="ko")
            or prop.name
        )
        description_display = (
            select_localized_text(description_i18n, lang="en")
            or select_localized_text(description_i18n, lang="ko")
            or ""
        )

        documentation: Dict[str, Any] = {
            "@label": label_display,
            "@comment": description_display,
            "@label_i18n": label_i18n,
            "@comment_i18n": description_i18n,
            "type": prop.type,
            "required": prop.required,
        }
        if prop.constraints:
            documentation["constraints"] = prop.constraints
        if prop.primary_key:
            documentation["primary_key"] = True
        if prop.title_key:
            documentation["title_key"] = True
        if prop.value_type_ref:
            documentation["value_type_ref"] = prop.value_type_ref
        if prop.shared_property_ref:
            documentation["shared_property_ref"] = prop.shared_property_ref
        if prop.items:
            documentation["items"] = prop.items

        schema: Dict[str, Any] = {
            "@class": mapped_type,
            "@documentation": documentation,
            # TerminusDB requires @type on object property definitions; keep required in documentation.
            "@type": "Optional",
        }
        return schema
    
    def _create_relationship_schema(self, rel: Relationship) -> Dict[str, Any]:
        """관계 스키마 생성 - TerminusDB 공식 패턴 준수"""
        # TerminusDB 공식 패턴:
        # - 1:1 필수: "Person" (타겟 클래스 직접 지정)
        # - 1:1 옵셔널: {"@type": "Optional", "@class": "Person"}
        # - 1:N: {"@type": "Set", "@class": "Person"}
        # - N:M: {"@type": "Set", "@class": "Person"}
        
        # 카디널리티 분석
        cardinality = rel.cardinality or "n:1"  # 기본값: n:1
        
        label_i18n = coerce_localized_text(rel.label)
        description_i18n = coerce_localized_text(rel.description) if rel.description else {}

        label_display = (
            select_localized_text(label_i18n, lang="en")
            or select_localized_text(label_i18n, lang="ko")
            or rel.predicate
        )
        description_display = (
            select_localized_text(description_i18n, lang="en")
            or select_localized_text(description_i18n, lang="ko")
            or ""
        )

        documentation: Dict[str, Any] = {
            "@label": label_display,
            "@comment": description_display,
            "@label_i18n": label_i18n,
            "@comment_i18n": description_i18n,
            "cardinality": cardinality,
        }

        if rel.inverse_predicate:
            documentation["inverse_predicate"] = rel.inverse_predicate
        if rel.inverse_label:
            documentation["inverse_label_i18n"] = coerce_localized_text(rel.inverse_label)

        # 다중 관계 (1:n, n:m)
        if cardinality.endswith(":n") or cardinality.endswith(":m"):
            return {
                "@type": "Set",  # 집합 타입 (순서/중복 없음)
                "@class": rel.target,
                "@documentation": documentation,
            }
        
        # 단일 관계 (1:1, n:1)
        # Relationship 모델에 required 필드가 없으므로 기본적으로 Optional로 처리
        # 하지만 특정 패턴 검사로 필수 여부 판단 가능
        required = getattr(rel, 'required', False)
        
        if required:
            # 필수 관계: TerminusDB는 bare string 형태도 허용하지만, 문서화를 위해 dict 형태를 사용.
            return {
                "@class": rel.target,
                "@documentation": documentation,
            }

        # 옵셔널 관계
        return {
            "@type": "Optional",
            "@class": rel.target,
            "@documentation": documentation,
        }
    
    def _map_datatype_to_terminus(self, datatype: Union[DataType, str]) -> str:
        """DataType을 TerminusDB 타입으로 매핑"""
        
        # Map domain type strings to TerminusDB XSD/sys types.
        # Handle both DataType enum and string inputs
        if isinstance(datatype, str):
            # Handle string inputs from tests/API calls (case-insensitive for non-prefixed types).
            string_mapping = {
                "string": "xsd:string",  # Use proper XSD types
                "integer": "xsd:integer",
                "decimal": "xsd:decimal",
                "number": "xsd:decimal",  # Map number to xsd:decimal
                "float": "xsd:float",
                "double": "xsd:double",
                "boolean": "xsd:boolean",
                "date": "xsd:date",
                "datetime": "xsd:dateTime",
                "timestamp": "xsd:dateTime",
                "email": "xsd:string",
                "phone": "xsd:string",
                "uuid": "xsd:string",
                "url": "xsd:anyURI",
                "uri": "xsd:anyURI",
                "geopoint": "xsd:string",
                "geoshape": "xsd:string",
                "marking": "xsd:string",
                "cipher": "xsd:string",
                "media": "xsd:string",
                "attachment": "xsd:string",
                "array": "sys:JSON",
                "struct": "sys:JSON",
                "object": "sys:JSON",
                "json": "sys:JSON",
                "vector": "sys:JSON",
                "xsd:string": "xsd:string",  # Keep XSD types as is
                "xsd:integer": "xsd:integer",
                "xsd:date": "xsd:date",
                "xsd:boolean": "xsd:boolean",
                "xsd:decimal": "xsd:decimal",
                "xsd:dateTime": "xsd:dateTime",
                "xsd:anyURI": "xsd:anyURI",
                "sys:JSON": "sys:JSON",
            }
            raw_type = datatype.strip()
            if not raw_type:
                return "sys:JSON"
            if raw_type.lower().startswith(("xsd:", "sys:")):
                return (
                    string_mapping.get(raw_type)
                    or string_mapping.get(raw_type.lower())
                    or raw_type
                )
            return string_mapping.get(raw_type.lower(), "sys:JSON")
        
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
        
        metadata: Dict[str, Any] = {}
        if isinstance(documentation, dict):
            doc_metadata = documentation.get("metadata")
            if isinstance(doc_metadata, dict):
                metadata.update(doc_metadata)
            if documentation.get("@internal"):
                metadata["internal"] = True

        ontology = OntologyResponse(
            id=ontology_id,
            label=label_i18n if isinstance(label_i18n, dict) and label_i18n else documentation.get("@label", ontology_id),
            description=comment_i18n if isinstance(comment_i18n, dict) and comment_i18n else documentation.get("@comment", ""),
            properties=[],
            relationships=[],
            metadata=metadata,
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
                            description=None,
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
                            description=None,
                            cardinality="1:1",
                        )
                    )
            elif isinstance(value, dict):
                # 관계인지 속성인지 판단
                if "@class" in value:
                    class_type = value.get("@class", "")
                    
                    # TerminusDB 기본 타입이면 속성
                    if class_type.startswith("xsd:") or class_type.startswith("sys:"):
                        prop_doc = value.get("@documentation", {}) if isinstance(value, dict) else {}
                        prop_label_i18n = prop_doc.get("@label_i18n")
                        prop_comment_i18n = prop_doc.get("@comment_i18n")
                        raw_comment = (
                            prop_comment_i18n
                            if isinstance(prop_comment_i18n, dict) and prop_comment_i18n
                            else prop_doc.get("@comment")
                        )
                        if isinstance(raw_comment, str) and not raw_comment.strip():
                            raw_comment = None

                        raw_type = prop_doc.get("type") or prop_doc.get("data_type") or prop_doc.get("datatype")
                        prop_type = raw_type if raw_type else class_type

                        required_flag = prop_doc.get("required")
                        prop_required = (
                            bool(required_flag)
                            if isinstance(required_flag, bool)
                            else value.get("@type") != "Optional"
                        )
                        prop = Property(
                            name=key,
                            type=prop_type,  # Prefer documented raw type (array/struct/etc) over terminus class
                            label=(
                                prop_label_i18n
                                if isinstance(prop_label_i18n, dict) and prop_label_i18n
                                else prop_doc.get("@label", key)
                            ),
                            description=raw_comment,
                            required=prop_required,
                            constraints=prop_doc.get("constraints") or {},
                            primary_key=bool(prop_doc.get("primary_key") or prop_doc.get("primaryKey")),
                            title_key=bool(prop_doc.get("title_key") or prop_doc.get("titleKey")),
                            value_type_ref=prop_doc.get("value_type_ref") or prop_doc.get("valueTypeRef"),
                            shared_property_ref=prop_doc.get("shared_property_ref") or prop_doc.get("sharedPropertyRef"),
                            items=prop_doc.get("items"),
                        )
                        ontology.properties.append(prop)
                    else:
                        # 사용자 정의 클래스면 관계
                        rel_doc = value.get("@documentation", {}) if isinstance(value, dict) else {}
                        rel_label_i18n = rel_doc.get("@label_i18n")
                        rel_comment_i18n = rel_doc.get("@comment_i18n")
                        inverse_label_i18n = rel_doc.get("inverse_label_i18n")
                        raw_rel_comment = (
                            rel_comment_i18n
                            if isinstance(rel_comment_i18n, dict) and rel_comment_i18n
                            else rel_doc.get("@comment")
                        )
                        if isinstance(raw_rel_comment, str) and not raw_rel_comment.strip():
                            raw_rel_comment = None

                        rel_cardinality = rel_doc.get("cardinality")
                        is_set = value.get("@type") == "Set"
                        if not rel_cardinality:
                            rel_cardinality = "n:m" if is_set else "n:1"
                        elif not is_set and (rel_cardinality.endswith(":n") or rel_cardinality.endswith(":m")):
                            # Schema says single (Optional/@class) but metadata claimed a multi-cardinality.
                            # Prefer schema structure to avoid accidental list coercion.
                            rel_cardinality = "n:1"
                        rel = Relationship(
                            predicate=key,
                            target=class_type,
                            label=(
                                rel_label_i18n
                                if isinstance(rel_label_i18n, dict) and rel_label_i18n
                                else rel_doc.get("@label", key)
                            ),
                            description=raw_rel_comment,
                            cardinality=rel_cardinality,
                            inverse_predicate=rel_doc.get("inverse_predicate"),
                            inverse_label=(
                                inverse_label_i18n
                                if isinstance(inverse_label_i18n, dict) and inverse_label_i18n
                                else rel_doc.get("inverse_label")
                            ),
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
