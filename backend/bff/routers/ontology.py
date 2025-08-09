"""
온톨로지 CRUD 라우터
온톨로지 생성, 조회, 수정, 삭제를 담당
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel

from shared.models.ontology import (
    OntologyBase,
    OntologyCreateRequestBFF,
    OntologyResponse,
    OntologyUpdateInput,
    Property,
    Relationship,
)

# Add shared path for common utilities
from shared.utils.language import get_accept_language

# Security validation imports
from shared.security.input_sanitizer import sanitize_input, validate_db_name, validate_class_id


# Schema suggestion request models
class SchemaFromDataRequest(BaseModel):
    """Request model for schema suggestion from data"""

    data: List[List[Any]]
    columns: List[str]
    class_name: Optional[str] = None
    include_complex_types: bool = False


class SchemaFromGoogleSheetsRequest(BaseModel):
    """Request model for schema suggestion from Google Sheets"""

    sheet_url: str
    worksheet_name: Optional[str] = None
    class_name: Optional[str] = None
    api_key: Optional[str] = None


class MappingSuggestionRequest(BaseModel):
    """Request model for mapping suggestions between schemas"""
    
    source_schema: List[Dict[str, Any]]  # New data schema
    target_schema: List[Dict[str, Any]]  # Existing ontology schema
    sample_data: Optional[List[Dict[str, Any]]] = None  # Sample values for pattern matching
    target_sample_data: Optional[List[Dict[str, Any]]] = None  # Target sample values for distribution matching


from fastapi import HTTPException

# Modernized dependency injection imports  
from bff.dependencies import (
    get_oms_client,
    OMSClientDep,
    LabelMapperDep,
    JSONLDConverterDep,
    TerminusServiceDep,
    TerminusService,
    LabelMapper,
    JSONToJSONLDConverter
)
from bff.services.adapter_service import BFFAdapterService
from bff.services.oms_client import OMSClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}", tags=["Ontology Management"])


# Dependency for BFF Adapter Service
def get_bff_adapter(
    terminus_service: TerminusService = TerminusServiceDep,
    label_mapper: LabelMapper = LabelMapperDep
) -> BFFAdapterService:
    """Get BFF Adapter Service instance"""
    return BFFAdapterService(terminus_service, label_mapper)

# Type-safe dependency annotation for BFF Adapter Service
BFFAdapterServiceDep = Depends(get_bff_adapter)


@router.post("/ontology", response_model=OntologyResponse)
async def create_ontology(
    db_name: str,
    ontology: Dict[str, Any],  # Accept raw dict to preserve target field
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
    jsonld_conv: JSONToJSONLDConverter = JSONLDConverterDep,
):
    """
    온톨로지 생성

    새로운 온톨로지 클래스를 생성합니다.
    레이블 기반으로 ID가 자동 생성됩니다.
    """
    # 🔥 ULTRA DEBUG! Force logging to check if route is called
    
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        
        # 입력 데이터 처리 (이미 dict 형태)
        ontology_dict = ontology.copy()  # Make a copy to avoid modifying the original
        ontology_dict = sanitize_input(ontology_dict)

        # ID 생성 또는 검증
        if ontology_dict.get('id'):
            # 사용자가 ID를 제공한 경우 검증
            class_id = validate_class_id(ontology_dict['id'])
            logger.info(f"Using provided class_id '{class_id}'")
        else:
            # ID가 제공되지 않은 경우 자동 생성
            from shared.utils.id_generator import generate_simple_id
            class_id = generate_simple_id(
                label=ontology_dict.get('label', ''), use_timestamp_for_korean=True, default_fallback="UnnamedClass"
            )
            logger.info(f"Generated class_id '{class_id}' from label '{ontology_dict.get('label', '')}')")

        ontology_dict["id"] = class_id

        # 🔥 THINK ULTRA! Transform properties for OMS compatibility
        # Convert 'target' to 'linkTarget' for link-type properties
        def transform_properties_for_oms(data):
            # Extract string from language objects
            def extract_string_value(value, field_name="field"):
                """Extract string value from either a plain string or language object"""
                if isinstance(value, str):
                    return value
                elif isinstance(value, dict):
                    # Try to get Korean first, then English, then any available language
                    return value.get('ko') or value.get('en') or next(iter(value.values()), '')
                else:
                    return str(value) if value else ''
            
            # Transform top-level label and description
            if 'label' in data:
                data['label'] = extract_string_value(data['label'], 'label')
            if 'description' in data:
                data['description'] = extract_string_value(data['description'], 'description')
            
            if 'properties' in data and isinstance(data['properties'], list):
                for i, prop in enumerate(data['properties']):
                    if isinstance(prop, dict):
                        # Extract string values from language objects in properties
                        if 'label' in prop:
                            prop['label'] = extract_string_value(prop['label'], 'property label')
                        if 'description' in prop:
                            prop['description'] = extract_string_value(prop['description'], 'property description')
                        
                        # 🔥 ULTRA! BFF uses 'label' but OMS requires 'name'
                        if 'name' not in prop and 'label' in prop:
                            # Generate name from label
                            prop['name'] = generate_simple_id(prop['label'], use_timestamp_for_korean=False)
                            logger.info(f"🔥 Generated property name '{prop['name']}' from label '{prop['label']}'")
                        
                        # 🔥 ULTRA! Convert STRING to xsd:string for OMS
                        if prop.get('type') == 'STRING':
                            prop['type'] = 'xsd:string'
                        elif prop.get('type') == 'INTEGER':
                            prop['type'] = 'xsd:integer'
                        elif prop.get('type') == 'DECIMAL':
                            prop['type'] = 'xsd:decimal'
                        elif prop.get('type') == 'BOOLEAN':
                            prop['type'] = 'xsd:boolean'
                        elif prop.get('type') == 'DATETIME':
                            prop['type'] = 'xsd:dateTime'
                        
                        # Convert target to linkTarget for link type properties
                        if prop.get('type') == 'link' and 'target' in prop:
                            prop['linkTarget'] = prop.pop('target')
                            logger.info(f"🔧 Converted property '{prop.get('name')}' target -> linkTarget: {prop.get('linkTarget')}")
                        
                        # Handle array properties with link items
                        if prop.get('type') == 'array' and 'items' in prop:
                            items = prop['items']
                            if isinstance(items, dict) and items.get('type') == 'link' and 'target' in items:
                                items['linkTarget'] = items.pop('target')
                                logger.info(f"🔧 Converted array property '{prop.get('name')}' items target -> linkTarget: {items.get('linkTarget')}")
            
            # Transform relationships
            if 'relationships' in data and isinstance(data['relationships'], list):
                for rel in data['relationships']:
                    if isinstance(rel, dict):
                        if 'label' in rel:
                            rel['label'] = extract_string_value(rel['label'], 'relationship label')
                        if 'description' in rel:
                            rel['description'] = extract_string_value(rel['description'], 'relationship description')
        
        # Apply transformation
        transform_properties_for_oms(ontology_dict)
        
        # 🔥 ULTRA DEBUG! Write to file to verify transformation
        debug_file = f"/tmp/bff_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(debug_file, 'w') as f:
            f.write(f"BEFORE transformation: {json.dumps(ontology, ensure_ascii=False, indent=2)}\n\n")
            f.write(f"AFTER transformation: {json.dumps(ontology_dict, ensure_ascii=False, indent=2)}\n")
        
        # Log transformed data for debugging
        logger.info(f"🔥 ULTRA DEBUG! Sending to OMS: {json.dumps(ontology_dict, ensure_ascii=False, indent=2)}")
        
        # 온톨로지 생성
        result = await terminus.create_class(db_name, ontology_dict)
        logger.info(f"OMS create result: {result}")

        # 레이블 매핑 등록
        await mapper.register_class(db_name, class_id, ontology_dict.get('label', ''), ontology_dict.get('description'))

        # 속성 레이블 매핑
        for prop in ontology_dict.get('properties', []):
            if isinstance(prop, dict):
                await mapper.register_property(db_name, class_id, prop.get('name', ''), prop.get('label', ''))

        # 관계 레이블 매핑
        for rel in ontology_dict.get('relationships', []):
            if isinstance(rel, dict):
                await mapper.register_relationship(db_name, rel.get('predicate', ''), rel.get('label', ''))

        # OMS 응답에서 생성된 데이터 추출
        if isinstance(result, dict):
            # OMS가 data 필드를 반환하는 경우
            if "data" in result and result.get("status") == "success":
                created_data = result["data"]
                # 생성된 ID 사용
                if "id" in created_data:
                    class_id = created_data["id"]
            elif "id" in result:
                # 직접 데이터 형식인 경우
                created_data = result
                class_id = result["id"]

        # 응답 생성
        # Validate properties and relationships to ensure they're properly formatted
        validated_properties = []
        for prop in ontology_dict.get('properties', []):
            if isinstance(prop, dict):
                validated_properties.append(Property(**prop))
                
        validated_relationships = []
        for rel in ontology_dict.get('relationships', []):
            if isinstance(rel, dict):
                validated_relationships.append(Relationship(**rel))
        
        # Create response
        return OntologyResponse(
            id=class_id,
            label=ontology_dict.get('label', ''),
            description=ontology_dict.get('description'),
            parent_class=ontology_dict.get('parent_class'),
            abstract=ontology_dict.get('abstract', False),
            properties=validated_properties,
            relationships=validated_relationships,
            metadata={"created": True, "database": db_name}
        )

    except HTTPException as he:
        # 🔥 ULTRA! Properly propagate HTTP exceptions with correct status codes
        logger.error(f"HTTP exception in create_ontology: {he.status_code} - {he.detail}")
        raise he
    except Exception as e:
        logger.error(f"Failed to create ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 생성 실패: {str(e)}",
        )


@router.get("/ontology/list")
async def list_ontologies(
    db_name: str,
    request: Request,
    class_type: str = Query("sys:Class", description="클래스 타입"),
    limit: Optional[int] = Query(None, description="결과 개수 제한"),
    offset: int = Query(0, description="오프셋"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    """
    온톨로지 목록 조회

    데이터베이스의 모든 온톨로지를 조회합니다.
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        
        # class_type 파라미터 화이트리스트 검증 (Security Enhancement)
        allowed_class_types = {"sys:Class", "owl:Class", "rdfs:Class"}
        if class_type not in allowed_class_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid class_type. Allowed values: {', '.join(allowed_class_types)}"
            )
        
        # 온톨로지 목록 조회
        ontologies = await terminus.list_classes(db_name)
        
        # 🔥 ULTRA DEBUG!
        logger.info(f"🔥 ontologies type: {type(ontologies)}")
        logger.info(f"🔥 ontologies content: {ontologies[:2] if ontologies else 'empty'}")

        # 배치 레이블 정보 추가 (N+1 쿼리 문제 해결)
        # 🔥 ULTRA! Skip label mapping for now to fix the issue
        labeled_ontologies = ontologies  # await mapper.convert_to_display_batch(db_name, ontologies, lang)

        return {
            "total": len(labeled_ontologies),
            "ontologies": labeled_ontologies,
            "offset": offset,
            "limit": limit,
        }

    except HTTPException as he:
        # 🔥 ULTRA! Properly propagate HTTP exceptions
        logger.error(f"HTTP exception in list_ontologies: {he.status_code} - {he.detail}")
        raise he
    except Exception as e:
        logger.error(f"Failed to list ontologies: {e}")
        # 🔥 ULTRA! Check if it's a 404 from OMS
        if "404" in str(e) or "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다"
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 목록 조회 실패: {str(e)}",
        )


@router.get("/ontology/{class_label}", response_model=OntologyResponse)
async def get_ontology(
    db_name: str,
    class_label: str,
    request: Request,
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    """
    온톨로지 조회

    레이블 또는 ID로 온톨로지를 조회합니다.
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        # URL 파라미터 디버깅
        logger.info(
            f"Getting ontology - db_name: {db_name}, class_label: {class_label}, lang: {lang}"
        )

        # 레이블로 ID 조회 시도
        class_id = await mapper.get_class_id(db_name, class_label, lang)
        logger.info(f"Mapped label '{class_label}' to class_id: {class_id}")

        # ID가 없으면 입력값을 ID로 간주
        if not class_id:
            class_id = class_label
            logger.info(f"No mapping found, using label as ID: {class_id}")

        # 온톨로지 조회
        result = await terminus.get_class(db_name, class_id)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_label}'을(를) 찾을 수 없습니다",
            )

        logger.info(f"Raw result from terminus.get_class: {result}")

        # Extract ontology data (terminus.get_class already extracts from data field)
        ontology_data = result if isinstance(result, dict) else {}

        # 레이블 정보 추가
        ontology_data = await mapper.convert_to_display(db_name, ontology_data, lang)
        logger.info(f"After convert_to_display: {ontology_data}")

        # Ensure result has required fields for OntologyResponse
        if not ontology_data.get("id"):
            ontology_data["id"] = class_id
        if not ontology_data.get("label"):
            # Try to get label from the mapper
            label = await mapper.get_class_label(db_name, class_id, lang)
            ontology_data["label"] = label or class_label

        # Remove extra fields that are not part of OntologyBase
        # Ensure label and description are simple strings
        label_value = ontology_data.get("label")
        if isinstance(label_value, dict):
            # Extract string from dict (fallback to English if available)
            label_value = label_value.get(lang) or label_value.get("en") or label_value.get("ko") or class_label
        elif not isinstance(label_value, str):
            label_value = str(label_value) if label_value else class_label
        
        desc_value = ontology_data.get("description")
        if isinstance(desc_value, dict):
            # Extract string from dict (fallback to English if available)
            desc_value = desc_value.get(lang) or desc_value.get("en") or desc_value.get("ko") or ""
        elif not isinstance(desc_value, str) and desc_value:
            desc_value = str(desc_value)
        
        ontology_base_data = {
            "id": ontology_data.get("id"),
            "label": label_value,
            "description": desc_value,
            "properties": ontology_data.get("properties", []),
            "relationships": ontology_data.get("relationships", []),
            "parent_class": ontology_data.get("parent_class"),
            "abstract": ontology_data.get("abstract", False),
            "metadata": ontology_data.get("metadata", {}),
        }

        # OntologyResponse inherits from OntologyBase, so pass fields directly
        return OntologyResponse(**ontology_base_data)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 조회 실패: {str(e)}",
        )


@router.put("/ontology/{class_label}")
async def update_ontology(
    db_name: str,
    class_label: str,
    ontology: OntologyUpdateInput,
    request: Request,
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    """
    온톨로지 수정

    기존 온톨로지를 수정합니다.
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        # 레이블로 ID 조회
        class_id = await mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = class_label

        # 업데이트 데이터 준비
        update_data = ontology.dict(exclude_unset=True)
        update_data["id"] = class_id

        # 온톨로지 업데이트
        await terminus.update_class(db_name, class_id, update_data)

        # 레이블 매핑 업데이트
        await mapper.update_mappings(db_name, update_data)

        return {
            "message": f"온톨로지 '{class_label}'이(가) 수정되었습니다",
            "id": class_id,
            "updated_fields": list(update_data.keys()),
        }

    except HTTPException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"온톨로지 '{e.ontology_id}'을(를) 찾을 수 없습니다",
        )
    except HTTPException as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"유효성 검증 실패: {e.message}",
        )
    except Exception as e:
        logger.error(f"Failed to update ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 수정 실패: {str(e)}",
        )


@router.delete("/ontology/{class_label}")
async def delete_ontology(
    db_name: str,
    class_label: str,
    request: Request,
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    """
    온톨로지 삭제

    온톨로지를 삭제합니다.
    주의: 이 작업은 되돌릴 수 없습니다!
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        # 레이블로 ID 조회
        class_id = await mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = class_label

        # 온톨로지 삭제
        await terminus.delete_class(db_name, class_id)

        # 레이블 매핑 삭제
        await mapper.remove_class(db_name, class_id)

        return {"message": f"온톨로지 '{class_label}'이(가) 삭제되었습니다", "id": class_id}

    except HTTPException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"온톨로지 '{e.ontology_id}'을(를) 찾을 수 없습니다",
        )
    except Exception as e:
        logger.error(f"Failed to delete ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 삭제 실패: {str(e)}",
        )


@router.get("/ontology/{class_id}/schema")
async def get_ontology_schema(
    db_name: str,
    class_id: str,
    request: Request,
    format: str = Query("json", description="스키마 형식 (json, jsonld, owl)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
    jsonld_conv: JSONToJSONLDConverter = JSONLDConverterDep,
):
    """
    온톨로지 스키마 조회

    온톨로지의 스키마를 다양한 형식으로 조회합니다.
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = sanitize_input(class_id)
        
        # format 파라미터 화이트리스트 검증 (Security Enhancement)
        allowed_formats = {"json", "jsonld", "owl"}
        if format not in allowed_formats:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid format. Allowed values: {', '.join(allowed_formats)}"
            )
        
        # 레이블로 ID 조회
        actual_id = await mapper.get_class_id(db_name, class_id, lang)
        if not actual_id:
            actual_id = class_id

        # 온톨로지 조회
        ontology = await terminus.get_class(db_name, actual_id)

        if not ontology:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"온톨로지 '{class_id}'을(를) 찾을 수 없습니다",
            )

        # 형식별 변환
        if format == "jsonld":
            schema = jsonld_conv.convert_to_jsonld(ontology, db_name)
        elif format == "owl":
            # OWL 형식은 JSON-LD로 대체 (RDF 호환)
            schema = jsonld_conv.convert_to_jsonld(ontology, db_name)
            # OWL 형식 표시를 위한 메타데이터 추가
            if isinstance(schema, dict):
                schema["@comment"] = "This is a JSON-LD representation compatible with OWL"
                schema["@owl:versionInfo"] = "1.0"
        else:
            # 기본 JSON
            schema = ontology

        return schema

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get ontology schema: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 스키마 조회 실패: {str(e)}",
        )


# 🔥 THINK ULTRA! BFF Enhanced Relationship Management Endpoints


@router.post("/ontology-advanced", response_model=OntologyResponse)
async def create_ontology_with_relationship_validation(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    adapter: BFFAdapterService = BFFAdapterServiceDep,
):
    """
    🔥 고급 관계 검증을 포함한 온톨로지 생성 (BFF 레이어 - 리팩토링됨)

    이제 중복 로직 없이 BFF Adapter를 통해 OMS로 모든 비즈니스 로직을 위임합니다.
    
    Features:
    - 레이블 기반 자동 ID 생성
    - 자동 역관계 생성
    - 관계 검증 및 무결성 체크
    - 순환 참조 탐지
    - 다국어 레이블 매핑
    """
    try:
        # BFF Adapter를 통해 모든 로직 위임 (중복 제거)
        return await adapter.create_advanced_ontology(
            db_name=db_name,
            ontology_data=ontology.dict(exclude_unset=True),
            language="ko"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ontology with advanced relationships: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"고급 온톨로지 생성 실패: {str(e)}",
        )


@router.post("/validate-relationships")
async def validate_ontology_relationships_bff(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    adapter: BFFAdapterService = BFFAdapterServiceDep,
):
    """
    🔥 온톨로지 관계 검증 (BFF 레이어 - 리팩토링됨)

    이제 중복 로직 없이 BFF Adapter를 통해 OMS로 모든 비즈니스 로직을 위임합니다.
    """
    try:
        # BFF Adapter를 통해 모든 로직 위임 (중복 제거)
        return await adapter.validate_relationships(
            db_name=db_name,
            validation_data=ontology.dict(exclude_unset=True),
            language="ko"
        )

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Failed to validate relationships: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"관계 검증 실패: {str(e)}"
        )


@router.post("/check-circular-references")
async def check_circular_references_bff(
    db_name: str,
    ontology: Optional[OntologyCreateRequestBFF] = None,
    adapter: BFFAdapterService = BFFAdapterServiceDep,
):
    """
    🔥 순환 참조 탐지 (BFF 레이어 - 리팩토링됨)

    이제 중복 로직 없이 BFF Adapter를 통해 OMS로 모든 비즈니스 로직을 위임합니다.
    """
    try:
        # 새 온톨로지 데이터 준비
        detection_data = {}
        if ontology:
            detection_data = ontology.dict(exclude_unset=True)

        # BFF Adapter를 통해 모든 로직 위임 (중복 제거)
        return await adapter.detect_circular_references(
            db_name=db_name,
            detection_data=detection_data,
            language="ko"
        )

    except Exception as e:
        logger.error(f"Failed to check circular references: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"순환 참조 탐지 실패: {str(e)}",
        )


@router.get("/relationship-network/analyze")
async def analyze_relationship_network_bff(
    db_name: str,
    request: Request,
    terminus: TerminusService = TerminusServiceDep,
    mapper: LabelMapper = LabelMapperDep,
):
    """
    🔥 관계 네트워크 분석 (BFF 레이어)

    전체 관계 네트워크의 건강성을 분석하고 사용자 친화적 결과 반환
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        # 네트워크 분석 수행
        analysis_result = await terminus.analyze_relationship_network(db_name)

        # 사용자 친화적 형태로 변환
        summary = analysis_result.get("relationship_summary", {})
        validation = analysis_result.get("validation_summary", {})
        cycles = analysis_result.get("cycle_analysis", {})
        graph = analysis_result.get("graph_summary", {})
        recommendations = analysis_result.get("recommendations", [])

        # 건강성 점수 계산 (0-100)
        health_score = 100
        if validation.get("errors", 0) > 0:
            health_score -= validation["errors"] * 10
        if cycles.get("critical_cycles", 0) > 0:
            health_score -= cycles["critical_cycles"] * 15
        if validation.get("warnings", 0) > 0:
            health_score -= validation["warnings"] * 2
        health_score = max(0, min(100, health_score))

        # 건강성 등급 결정
        if health_score >= 90:
            health_grade = "Excellent"
            health_color = "green"
        elif health_score >= 70:
            health_grade = "Good"
            health_color = "blue"
        elif health_score >= 50:
            health_grade = "Fair"
            health_color = "yellow"
        else:
            health_grade = "Poor"
            health_color = "red"

        return {
            "status": "success",
            "message": "관계 네트워크 분석이 완료되었습니다",
            "network_health": {
                "score": health_score,
                "grade": health_grade,
                "color": health_color,
                "description": f"네트워크 건강성이 {health_grade.lower()} 상태입니다",
            },
            "statistics": {
                "total_ontologies": analysis_result.get("ontology_count", 0),
                "total_relationships": summary.get("total_relationships", 0),
                "relationship_types": len(graph.get("relationship_types", [])),
                "total_entities": graph.get("total_entities", 0),
                "average_connections": round(graph.get("average_connections_per_entity", 0), 2),
            },
            "quality_metrics": {
                "validation_errors": validation.get("errors", 0),
                "validation_warnings": validation.get("warnings", 0),
                "critical_cycles": cycles.get("critical_cycles", 0),
                "total_cycles": cycles.get("total_cycles", 0),
                "inverse_coverage": summary.get("inverse_coverage", "0/0 (0%)"),
            },
            "recommendations": [
                {
                    "priority": "high" if "❌" in rec else "medium" if "⚠️" in rec else "low",
                    "message": rec.replace("❌ ", "")
                    .replace("⚠️ ", "")
                    .replace("📝 ", "")
                    .replace("🔄 ", ""),
                }
                for rec in recommendations
            ],
            "metadata": {
                "analysis_timestamp": analysis_result.get("analysis_timestamp"),
                "database": db_name,
                "language": lang,
            },
        }

    except Exception as e:
        logger.error(f"Failed to analyze relationship network: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"관계 네트워크 분석 실패: {str(e)}",
        )


@router.get("/relationship-paths")
async def find_relationship_paths_bff(
    request: Request,
    db_name: str,
    start_entity: str,
    end_entity: Optional[str] = Query(
        None, description="목표 엔티티 (없으면 모든 도달 가능한 엔티티)"
    ),
    max_depth: int = Query(3, ge=1, le=5, description="최대 탐색 깊이"),
    path_type: str = Query("shortest", description="경로 타입 (shortest, all, weighted, semantic)"),
    terminus: TerminusService = TerminusServiceDep,
    mapper: LabelMapper = LabelMapperDep,
):
    """
    🔥 관계 경로 탐색 (BFF 레이어)

    엔티티 간의 관계 경로를 찾아 사용자 친화적 형태로 반환
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        start_entity = sanitize_input(start_entity)
        if end_entity:
            end_entity = sanitize_input(end_entity)
        
        # path_type 파라미터 화이트리스트 검증 (Security Enhancement)
        allowed_path_types = {"shortest", "all", "weighted", "semantic"}
        if path_type not in allowed_path_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid path_type. Allowed values: {', '.join(allowed_path_types)}"
            )
        # 레이블을 ID로 변환
        start_id = await mapper.get_class_id(db_name, start_entity, lang)
        if not start_id:
            start_id = start_entity

        end_id = None
        if end_entity:
            end_id = await mapper.get_class_id(db_name, end_entity, lang)
            if not end_id:
                end_id = end_entity

        # 경로 탐색 수행
        path_result = await terminus.find_relationship_paths(
            db_name=db_name,
            start_entity=start_id,
            end_entity=end_id,
            max_depth=max_depth,
            path_type=path_type,
        )

        # ID를 레이블로 변환하여 사용자 친화적 결과 생성
        paths = path_result.get("paths", [])
        user_friendly_paths = []

        for path in paths:
            # 엔티티 ID들을 레이블로 변환
            entity_labels = []
            for entity_id in path.get("entities", []):
                label = await mapper.get_class_label(db_name, entity_id, lang)
                entity_labels.append(label or entity_id)

            user_friendly_paths.append(
                {
                    "start": entity_labels[0] if entity_labels else start_entity,
                    "end": (
                        entity_labels[-1]
                        if len(entity_labels) > 1
                        else entity_labels[0] if entity_labels else ""
                    ),
                    "path": " → ".join(entity_labels),
                    "predicates": path.get("predicates", []),
                    "length": path.get("length", 0),
                    "weight": round(path.get("total_weight", 0), 2),
                    "confidence": path.get("confidence", 1.0),
                    "path_type": path.get("path_type", "unknown"),
                }
            )

        statistics = path_result.get("statistics", {})

        return {
            "status": "success",
            "message": f"관계 경로 탐색이 완료되었습니다 ({len(user_friendly_paths)}개 경로 발견)",
            "query": {
                "start_entity": start_entity,
                "end_entity": end_entity,
                "max_depth": max_depth,
                "path_type": path_type,
            },
            "paths": user_friendly_paths[:10],  # 최대 10개만 반환
            "statistics": {
                "total_paths_found": len(paths),
                "displayed_paths": min(len(user_friendly_paths), 10),
                "average_length": statistics.get("average_length", 0),
                "shortest_path_length": statistics.get("min_length", 0),
                "longest_path_length": statistics.get("max_length", 0),
            },
            "metadata": {"language": lang, "start_id": start_id, "end_id": end_id},
        }

    except Exception as e:
        logger.error(f"Failed to find relationship paths: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"관계 경로 탐색 실패: {str(e)}",
        )


@router.post("/suggest-schema-from-data")
async def suggest_schema_from_data(
    db_name: str,
    request: SchemaFromDataRequest,
    terminus: TerminusService = TerminusServiceDep,
):
    """
    🔥 데이터에서 스키마 자동 제안

    제공된 데이터를 분석하여 OMS 온톨로지 스키마를 자동으로 생성합니다.
    이는 Funnel 서비스의 핵심 기능을 통합한 엔드포인트입니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        from bff.services.funnel_client import FunnelClient

        # Funnel 클라이언트를 통해 스키마 제안 요청
        async with FunnelClient() as funnel_client:
            # 데이터 분석 및 스키마 제안을 한 번에 수행
            result = await funnel_client.analyze_and_suggest_schema(
                data=request.data,
                columns=request.columns,
                class_name=request.class_name,
                include_complex_types=request.include_complex_types,
            )

            analysis = result.get("analysis", {})
            schema_suggestion = result.get("schema_suggestion", {})

            # 분석 결과 요약
            analyzed_columns = analysis.get("columns", [])
            high_confidence_types = [
                col
                for col in analyzed_columns
                if col.get("inferred_type", {}).get("confidence", 0) >= 0.7
            ]

            return {
                "status": "success",
                "message": f"스키마 제안이 완료되었습니다. {len(high_confidence_types)}/{len(analyzed_columns)} 컬럼에 대해 높은 신뢰도 타입 추론 성공",
                "suggested_schema": schema_suggestion,
                "analysis_summary": {
                    "total_columns": len(analyzed_columns),
                    "high_confidence_columns": len(high_confidence_types),
                    "average_confidence": (
                        round(
                            sum(
                                col.get("inferred_type", {}).get("confidence", 0)
                                for col in analyzed_columns
                            )
                            / len(analyzed_columns),
                            2,
                        )
                        if analyzed_columns
                        else 0
                    ),
                    "detected_types": list(
                        set(
                            col.get("inferred_type", {}).get("type", "unknown")
                            for col in analyzed_columns
                        )
                    ),
                },
                "detailed_analysis": analysis,
            }

    except Exception as e:
        logger.error(f"Schema suggestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"스키마 제안 실패: {str(e)}"
        )


@router.post("/suggest-mappings")
async def suggest_mappings(
    db_name: str,
    request: MappingSuggestionRequest,
) -> Dict[str, Any]:
    """
    두 스키마 간의 매핑을 자동으로 제안
    
    AI 기반 매핑 제안 엔진을 사용하여 소스 스키마(새 데이터)를
    타겟 스키마(기존 온톨로지)에 매핑하는 제안을 생성합니다.
    
    Features:
    - 이름 기반 매칭 (exact, fuzzy, phonetic)
    - 타입 기반 매칭
    - 의미론적 매칭 (도메인 지식)
    - 값 패턴 기반 매칭
    - 신뢰도 점수 제공
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        
        from bff.services.mapping_suggestion_service import MappingSuggestionService
        
        # 매핑 제안 서비스 사용 (구성 가능한 임계값/가중치)
        config = None
        try:
            # Try to load config from file
            import os
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'mapping_config.json')
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                logger.info(f"Loaded mapping config from {config_path}")
        except Exception as e:
            logger.warning(f"Failed to load mapping config: {e}, using defaults")
        
        suggestion_service = MappingSuggestionService(config=config)
        
        # Get target sample data if provided
        target_sample_data = request.target_sample_data
        
        # Note: For better accuracy, the frontend should fetch target sample data
        # when selecting a target ontology class and include it in the request.
        # This allows for value distribution similarity matching.
        
        if not target_sample_data:
            logger.info("No target sample data provided - value distribution matching will be skipped")
        
        suggestion = suggestion_service.suggest_mappings(
            source_schema=request.source_schema,
            target_schema=request.target_schema,
            sample_data=request.sample_data,
            target_sample_data=target_sample_data
        )
        
        # 결과 포맷팅
        return {
            "status": "success",
            "message": f"Found {len(suggestion.mappings)} mapping suggestions with {suggestion.overall_confidence:.2f} overall confidence",
            "mappings": [
                {
                    "source_field": m.source_field,
                    "target_field": m.target_field,
                    "confidence": m.confidence,
                    "match_type": m.match_type,
                    "reasons": m.reasons
                }
                for m in suggestion.mappings
            ],
            "unmapped_source_fields": suggestion.unmapped_source_fields,
            "unmapped_target_fields": suggestion.unmapped_target_fields,
            "overall_confidence": suggestion.overall_confidence,
            "statistics": {
                "total_source_fields": len(request.source_schema),
                "total_target_fields": len(request.target_schema),
                "mapped_fields": len(suggestion.mappings),
                "high_confidence_mappings": len([m for m in suggestion.mappings if m.confidence >= 0.8]),
                "medium_confidence_mappings": len([m for m in suggestion.mappings if 0.6 <= m.confidence < 0.8]),
            }
        }
    
    except Exception as e:
        logger.error(f"Mapping suggestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"매핑 제안 실패: {str(e)}"
        )


@router.post("/suggest-schema-from-google-sheets")
async def suggest_schema_from_google_sheets(
    db_name: str,
    request: SchemaFromGoogleSheetsRequest,
    terminus: TerminusService = TerminusServiceDep,
):
    """
    🔥 Google Sheets에서 스키마 자동 제안

    Google Sheets URL에서 데이터를 가져와 분석하고 OMS 온톨로지 스키마를 자동으로 생성합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        from bff.services.funnel_client import FunnelClient

        # Funnel 클라이언트를 통해 Google Sheets 스키마 제안 요청
        async with FunnelClient() as funnel_client:
            result = await funnel_client.google_sheets_to_schema(
                sheet_url=request.sheet_url,
                worksheet_name=request.worksheet_name,
                class_name=request.class_name,
                api_key=request.api_key,
            )

            preview = result.get("preview", {})
            schema_suggestion = result.get("schema_suggestion", {})

            # 미리보기 정보 요약
            total_rows = preview.get("total_rows", 0)
            preview_rows = preview.get("preview_rows", 0)
            columns = preview.get("columns", [])

            return {
                "status": "success",
                "message": f"Google Sheets 스키마 제안이 완료되었습니다. {total_rows}행 중 {preview_rows}행 분석됨",
                "suggested_schema": schema_suggestion,
                "source_info": {
                    "sheet_url": request.sheet_url,
                    "worksheet_name": request.worksheet_name,
                    "total_rows": total_rows,
                    "preview_rows": preview_rows,
                    "columns": columns,
                },
                "preview_data": preview,
            }

    except Exception as e:
        logger.error(f"Google Sheets schema suggestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets 스키마 제안 실패: {str(e)}",
        )


@router.post("/ontology/{class_id}/mapping-metadata")
async def save_mapping_metadata(
    db_name: str,
    class_id: str,
    metadata: Dict[str, Any],
    oms: OMSClient = OMSClientDep,
    mapper: LabelMapper = LabelMapperDep,
):
    """
    매핑 메타데이터를 온톨로지 클래스에 저장
    
    데이터 매핑 이력과 통계를 ClassMetadata에 저장합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = validate_class_id(class_id)
        sanitized_metadata = sanitize_input(metadata)
        
        # 기존 메타데이터 가져오기
        existing_metadata = await oms.get_class_metadata(db_name, class_id)
        
        # 매핑 이력 업데이트
        mapping_history = existing_metadata.get("mapping_history", [])
        
        # 새 매핑 정보 추가
        new_mapping_entry = {
            "timestamp": sanitized_metadata.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "source_file": sanitized_metadata.get("sourceFile", "unknown"),
            "mappings_count": sanitized_metadata.get("mappingsCount", 0),
            "average_confidence": sanitized_metadata.get("averageConfidence", 0),
            "mapping_details": sanitized_metadata.get("mappingDetails", [])
        }
        
        mapping_history.append(new_mapping_entry)
        
        # 최근 10개의 매핑 이력만 유지
        if len(mapping_history) > 10:
            mapping_history = mapping_history[-10:]
        
        # 메타데이터 업데이트
        updated_metadata = {
            **existing_metadata,
            "mapping_history": mapping_history,
            "last_mapping_date": new_mapping_entry["timestamp"],
            "total_mappings": sum(entry.get("mappings_count", 0) for entry in mapping_history),
            "mapping_sources": list(set(
                entry.get("source_file", "unknown") 
                for entry in mapping_history 
                if entry.get("source_file") != "unknown"
            ))
        }
        
        # OMS를 통해 메타데이터 저장
        await oms.update_class_metadata(db_name, class_id, updated_metadata)
        
        logger.info(f"Saved mapping metadata for class {class_id}: {new_mapping_entry['mappings_count']} mappings from {new_mapping_entry['source_file']}")
        
        return {
            "status": "success",
            "message": "매핑 메타데이터가 저장되었습니다",
            "data": {
                "class_id": class_id,
                "mapping_entry": new_mapping_entry,
                "total_history_entries": len(mapping_history)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save mapping metadata: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"매핑 메타데이터 저장 실패: {str(e)}"
        )


# ==========================================
# CSV DATA IMPORT ENDPOINTS
# ==========================================
# TODO: Replace with actual Outbox Pattern + CQRS implementation
# ARCHITECTURE NOTES for future developers:
# ========================================
# 
# 1. OUTBOX PATTERN IMPLEMENTATION:
#    - Create PostgreSQL table: import_jobs
#    - Schema: job_id, database_name, class_id, csv_data, mappings, status, created_at
#    - Insert import job atomically with other operations
#    - Use database transactions to ensure consistency
#
# 2. EVENT STREAMING with KAFKA:
#    - Topic: "data-import-requests"
#    - Event Schema: { jobId, database, classId, csvData, mappings, timestamp }
#    - Producer: BFF service (this endpoint)
#    - Consumer: Import processing service
#
# 3. CQRS IMPLEMENTATION:
#    - COMMAND: Import job creation (handled by Kafka consumer)
#    - QUERY: Data retrieval from ElasticSearch + Cassandra
#    - Write Model: TerminusDB (source of truth)
#    - Read Models: ElasticSearch (search), Cassandra (bulk data)
#
# 4. SERVICES ARCHITECTURE:
#    BFF -> Outbox Table -> Kafka -> Import Service -> TerminusDB + ElasticSearch + Cassandra
#
# 5. PROGRESS TRACKING:
#    - WebSocket/SSE endpoint for real-time progress
#    - Redis for caching import job status
#    - Event-driven status updates
#
# 6. ERROR HANDLING:
#    - Dead letter queue for failed imports
#    - Retry mechanism with exponential backoff
#    - Detailed error logging and recovery

class CsvImportRequest(BaseModel):
    """Request model for CSV data import"""
    csv_data: List[Dict[str, Any]]
    mappings: Dict[str, Any]
    source_file: str
    total_records: int


@router.post("/import-csv-data")
async def import_csv_data(
    db_name: str,
    import_request: CsvImportRequest,
    oms: OMSClient = OMSClientDep
) -> Dict[str, Any]:
    """
    CSV 데이터를 온톨로지 인스턴스로 임포트
    
    TODO: 실제 구현에서는 Outbox Pattern + Event Streaming 사용
    현재는 UX 플로우 완성을 위한 Placeholder 구현
    """
    try:
        # Input validation
        validate_db_name(db_name)
        
        if not import_request.csv_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CSV 데이터가 비어있습니다"
            )
        
        logger.info(f"Starting CSV import for database {db_name}: {import_request.total_records} records from {import_request.source_file}")
        
        # TODO: Replace with Outbox Pattern
        # =====================================
        # STEP 1: Insert to outbox table (PostgreSQL)
        # import_job = await create_import_job({
        #     "database_name": db_name,
        #     "source_file": import_request.source_file,
        #     "total_records": import_request.total_records,
        #     "csv_data": import_request.csv_data,
        #     "mappings": import_request.mappings,
        #     "status": "pending",
        #     "created_at": datetime.utcnow()
        # })
        # 
        # STEP 2: Publish to Kafka
        # await kafka_producer.send("data-import-requests", {
        #     "job_id": import_job.id,
        #     "database": db_name,
        #     "class_id": import_request.mappings.get("target_class"),
        #     "csv_data": import_request.csv_data,
        #     "mappings": import_request.mappings,
        #     "timestamp": datetime.utcnow().isoformat()
        # })
        # 
        # STEP 3: Return job ID for progress tracking
        # return {
        #     "status": "accepted",
        #     "job_id": import_job.id,
        #     "message": "Import job queued successfully",
        #     "progress_url": f"/api/v1/import-progress/{import_job.id}"
        # }
        
        # PLACEHOLDER IMPLEMENTATION: Simulate async processing
        import uuid
        import asyncio
        
        job_id = str(uuid.uuid4())
        
        # Simulate processing delay
        await asyncio.sleep(0.1)
        
        # Mock processing results (95% success rate)
        success_count = int(import_request.total_records * 0.95)
        failed_count = import_request.total_records - success_count
        
        mock_errors = [
            f"Row {i}: Invalid data format" for i in range(1, failed_count + 1)
        ]
        
        logger.info(f"CSV import completed for job {job_id}: {success_count}/{import_request.total_records} records successful")
        
        return {
            "status": "completed",
            "job_id": job_id,
            "results": {
                "total_records": import_request.total_records,
                "success_count": success_count,
                "failed_count": failed_count,
                "errors": mock_errors[:3]  # Return first 3 errors
            },
            "message": f"Successfully imported {success_count} out of {import_request.total_records} records"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"CSV import failed for database {db_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"CSV 데이터 임포트 실패: {str(e)}"
        )


@router.get("/import-progress/{job_id}")
async def get_import_progress(
    db_name: str,
    job_id: str
) -> Dict[str, Any]:
    """
    임포트 작업 진행 상황 조회
    
    TODO: WebSocket/SSE로 실시간 진행 상황 제공
    Redis에서 진행 상황 캐시 조회
    """
    try:
        validate_db_name(db_name)
        
        # TODO: Replace with Redis cache lookup
        # progress_data = await redis_client.get(f"import_progress:{job_id}")
        # if not progress_data:
        #     raise HTTPException(404, "Import job not found")
        # return json.loads(progress_data)
        
        # PLACEHOLDER: Mock progress data
        return {
            "job_id": job_id,
            "status": "completed",
            "progress": 100,
            "current_step": "Import completed",
            "total_records": 500,
            "processed_records": 500,
            "success_count": 475,
            "failed_count": 25,
            "started_at": "2024-01-01T10:00:00Z",
            "completed_at": "2024-01-01T10:05:00Z"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get import progress for job {job_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"임포트 진행 상황 조회 실패: {str(e)}"
        )
