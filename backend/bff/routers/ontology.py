"""
온톨로지 CRUD 라우터
온톨로지 생성, 조회, 수정, 삭제를 담당
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from shared.models.ontology import (
    OntologyBase,
    OntologyCreateRequestBFF,
    OntologyResponse,
    OntologyUpdateInput,
    Property,
    Relationship,
)
from shared.models.responses import ApiResponse
from shared.models.structure_analysis import BoundingBox

# Add shared path for common utilities
from shared.utils.language import get_accept_language

# Security validation imports
from shared.security.input_sanitizer import (
    sanitize_input,
    validate_branch_name,
    validate_db_name,
    validate_class_id,
)


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
    table_id: Optional[str] = None
    table_bbox: Optional[BoundingBox] = None


class MappingSuggestionRequest(BaseModel):
    """Request model for mapping suggestions between schemas"""
    
    source_schema: List[Dict[str, Any]]  # New data schema
    target_schema: List[Dict[str, Any]]  # Existing ontology schema
    sample_data: Optional[List[Dict[str, Any]]] = None  # Sample values for pattern matching
    target_sample_data: Optional[List[Dict[str, Any]]] = None  # Target sample values for distribution matching


class MappingFromGoogleSheetsRequest(BaseModel):
    """Request model for mapping suggestions from Google Sheets → existing ontology class"""

    sheet_url: str
    worksheet_name: Optional[str] = None
    api_key: Optional[str] = None

    target_class_id: str
    # OMS integration is optional; when disabled, clients should supply the target schema directly.
    target_schema: List[Dict[str, Any]] = Field(default_factory=list)

    table_id: Optional[str] = None
    table_bbox: Optional[BoundingBox] = None

    include_relationships: bool = False
    enable_semantic_hints: bool = False


class ImportFieldMapping(BaseModel):
    """Field mapping for import (source column → target property)"""

    source_field: str
    target_field: str


class ImportTargetField(BaseModel):
    """Target field definition for import (name + type)."""

    name: str
    type: str = "xsd:string"


class ImportFromGoogleSheetsRequest(BaseModel):
    """Request model for dry-run/commit import from Google Sheets"""

    sheet_url: str
    worksheet_name: Optional[str] = None
    api_key: Optional[str] = None

    target_class_id: str
    target_schema: List[ImportTargetField] = Field(default_factory=list)
    mappings: List[ImportFieldMapping] = Field(default_factory=list)

    table_id: Optional[str] = None
    table_bbox: Optional[BoundingBox] = None

    # Extraction controls
    max_tables: int = 5
    max_rows: Optional[int] = None
    max_cols: Optional[int] = None
    trim_trailing_empty: bool = True

    # Import behavior
    allow_partial: bool = False
    dry_run_rows: int = 100
    max_import_rows: Optional[int] = None
    batch_size: int = 500
    return_instances: bool = False
    max_return_instances: int = 1000
    options: Dict[str, Any] = Field(default_factory=dict)

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
from bff.services.oms_client import OMSClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}", tags=["Ontology Management"])


@router.post(
    "/ontology",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "Conflict (already exists / OCC)"},
    },
)
async def create_ontology(
    db_name: str,
    ontology: OntologyCreateRequestBFF,  # 🔥 FIXED: Use proper Pydantic validation
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
    jsonld_conv: JSONToJSONLDConverter = JSONLDConverterDep,
):
    """
    온톨로지 생성

    새로운 온톨로지 클래스를 생성합니다.
    레이블 기반으로 ID가 자동 생성됩니다.
    """    
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)

        from shared.utils.id_generator import generate_simple_id

        def _localized_to_string(value: Any) -> str:
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                # Prefer English for IDs (domain-neutral), then Korean, then any translation.
                return (
                    str(value.get("en") or "").strip()
                    or str(value.get("ko") or "").strip()
                    or next((str(v).strip() for v in value.values() if v and str(v).strip()), "")
                )
            return str(value).strip() if value is not None else ""
        
        # 입력 데이터 처리 (Pydantic model에서 dict로 변환)
        ontology_dict = ontology.model_dump(exclude_unset=True)  # Convert Pydantic model to dict
        ontology_dict = sanitize_input(ontology_dict)

        # ID 생성 또는 검증
        if ontology_dict.get('id'):
            # 사용자가 ID를 제공한 경우 검증
            class_id = validate_class_id(ontology_dict['id'])
            logger.info(f"Using provided class_id '{class_id}'")
        else:
            # ID가 제공되지 않은 경우 자동 생성
            class_id = generate_simple_id(
                label=_localized_to_string(ontology_dict.get("label", "")),
                use_timestamp_for_korean=True,
                default_fallback="UnnamedClass",
            )
            logger.info(f"Generated class_id '{class_id}' from label '{ontology_dict.get('label', '')}')")

        ontology_dict["id"] = class_id

        # 🔥 THINK ULTRA! Transform properties for OMS compatibility
        # Convert 'target' to 'linkTarget' for link-type properties
        def transform_properties_for_oms(data):
            if 'properties' in data and isinstance(data['properties'], list):
                for i, prop in enumerate(data['properties']):
                    if isinstance(prop, dict):
                        # Generate property name from label if missing
                        if 'name' not in prop and 'label' in prop:
                            prop['name'] = generate_simple_id(
                                _localized_to_string(prop.get("label")),
                                use_timestamp_for_korean=False,
                            )
                        
                        # Convert generic types to XSD types for OMS compatibility
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
            
            # Relationships: keep localized labels as-is; only structural transformations belong here.
        
        # Apply transformation
        transform_properties_for_oms(ontology_dict)
        
        # Log ontology creation request
        logger.info(f"Creating ontology '{ontology_dict.get('id')}' in database '{db_name}'")
        
        # 온톨로지 생성 - OMS API를 통해 생성
        import aiohttp
        from shared.config.service_config import ServiceConfig
        
        oms_url = ServiceConfig.get_oms_url()
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{oms_url}/api/v1/database/{db_name}/ontology",
                json=ontology_dict,
                params={"branch": branch},
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status in [200, 202]:  # Accept both 200 (direct) and 202 (Event Sourcing)
                    result = await response.json()
                    logger.info(f"OMS create result (status {response.status}): {result}")
                else:
                    error_text = await response.text()
                    logger.error(f"OMS API failed with status {response.status}: {error_text}")
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"OMS API failed: {error_text}"
                    )

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

        # Event-sourcing 202 response (pass-through ApiResponse.accepted)
        if isinstance(result, dict) and result.get("status") == "accepted" and "data" in result:
            event_data = result["data"]
            class_id = event_data.get("ontology_id", class_id)
            logger.info(
                f"Event Sourcing: ontology {class_id} creation accepted with command_id {event_data.get('command_id')}"
            )
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        # Some deployments may return ApiResponse even in direct mode. In that case, pass through.
        if isinstance(result, dict) and "status" in result and "message" in result:
            return JSONResponse(status_code=status.HTTP_200_OK, content=result)

        # Direct mode (legacy): wrap raw ontology payload into ApiResponse for FE consistency
        if isinstance(result, dict) and "id" in result:
            class_id = result.get("id") or class_id

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"온톨로지 '{class_id}'이(가) 생성되었습니다",
                data={"ontology_id": class_id, "ontology": result, "mode": "direct"},
            ).to_dict(),
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
    branch: str = Query("main", description="Target branch (default: main)"),
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
        branch = validate_branch_name(branch)
        
        # class_type 파라미터 화이트리스트 검증 (Security Enhancement)
        allowed_class_types = {"sys:Class", "owl:Class", "rdfs:Class"}
        if class_type not in allowed_class_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid class_type. Allowed values: {', '.join(allowed_class_types)}"
            )
        
        # 온톨로지 목록 조회
        ontologies = await terminus.list_classes(db_name, branch=branch)
        
        # Log ontology retrieval result
        logger.info(f"Retrieved {len(ontologies) if ontologies else 0} ontologies from database '{db_name}'")

        # 배치 레이블 정보 추가 (N+1 쿼리 문제 해결)
        # Apply label mapping for better display
        labeled_ontologies = await mapper.convert_to_display_batch(db_name, ontologies, lang)

        return {
            "total": len(labeled_ontologies),
            "ontologies": labeled_ontologies,
            "offset": offset,
            "limit": limit,
            "branch": branch,
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
    branch: str = Query("main", description="Target branch (default: main)"),
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
        branch = validate_branch_name(branch)
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
        result = await terminus.get_class(db_name, class_id, branch=branch)

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
            "metadata": {**(ontology_data.get("metadata", {}) or {}), "branch": branch},
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


@router.post("/ontology/validate", response_model=ApiResponse)
async def validate_ontology_create_bff(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    oms_client: OMSClient = OMSClientDep,
):
    """온톨로지 생성 검증 (no write) - OMS proxy."""
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        payload = sanitize_input(ontology.model_dump(exclude_unset=True))
        return await oms_client.validate_ontology_create(db_name, payload, branch=branch)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate ontology create: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 생성 검증 실패: {str(e)}",
        )


@router.post("/ontology/{class_label}/validate", response_model=ApiResponse)
async def validate_ontology_update_bff(
    db_name: str,
    class_label: str,
    ontology: OntologyUpdateInput,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    oms_client: OMSClient = OMSClientDep,
):
    """온톨로지 업데이트 검증 (no write) - OMS proxy."""
    lang = get_accept_language(request)
    try:
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        branch = validate_branch_name(branch)

        class_id = await mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = class_label

        payload = sanitize_input(ontology.model_dump(exclude_unset=True))
        return await oms_client.validate_ontology_update(db_name, class_id, payload, branch=branch)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate ontology update: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 업데이트 검증 실패: {str(e)}",
        )


@router.put(
    "/ontology/{class_label}",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "OCC conflict"},
    },
)
async def update_ontology(
    db_name: str,
    class_label: str,
    ontology: OntologyUpdateInput,
    request: Request,
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    branch: str = Query("main", description="Target branch (default: main)"),
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
        branch = validate_branch_name(branch)
        # 레이블로 ID 조회
        class_id = await mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = class_label

        # 업데이트 데이터 준비
        update_data = ontology.model_dump(exclude_unset=True)
        update_data["id"] = class_id

        forward_headers: Dict[str, str] = {}
        for key in ("X-Admin-Token", "X-Change-Reason", "X-Admin-Actor", "X-Actor", "Authorization"):
            value = request.headers.get(key)
            if value:
                forward_headers[key] = value

        # 온톨로지 업데이트
        result = await terminus.update_class(
            db_name,
            class_id,
            update_data,
            expected_seq=expected_seq,
            branch=branch,
            headers=forward_headers or None,
        )

        # 레이블 매핑 업데이트 (BFF-local read model; update immediately on accept as well)
        await mapper.update_mappings(db_name, update_data)

        if isinstance(result, dict) and result.get("status") == "accepted":
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"온톨로지 '{class_label}'이(가) 수정되었습니다",
                data={"ontology_id": class_id, "updated_fields": list(update_data.keys()), "mode": "direct"},
            ).to_dict(),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update ontology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"온톨로지 수정 실패: {str(e)}",
        )


@router.delete(
    "/ontology/{class_label}",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "OCC conflict"},
    },
)
async def delete_ontology(
    db_name: str,
    class_label: str,
    request: Request,
    expected_seq: int = Query(..., ge=0, description="Expected current aggregate sequence (OCC)"),
    branch: str = Query("main", description="Target branch (default: main)"),
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
        branch = validate_branch_name(branch)
        # 레이블로 ID 조회
        class_id = await mapper.get_class_id(db_name, class_label, lang)
        if not class_id:
            class_id = class_label

        forward_headers: Dict[str, str] = {}
        for key in ("X-Admin-Token", "X-Change-Reason", "X-Admin-Actor", "X-Actor", "Authorization"):
            value = request.headers.get(key)
            if value:
                forward_headers[key] = value

        result = await terminus.delete_class(
            db_name, class_id, expected_seq=expected_seq, branch=branch, headers=forward_headers or None
        )

        # 레이블 매핑 삭제 (BFF-local read model; delete immediately on accept as well)
        await mapper.remove_class(db_name, class_id)

        if isinstance(result, dict) and result.get("status") == "accepted":
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"온톨로지 '{class_label}'이(가) 삭제되었습니다",
                data={"ontology_id": class_id, "mode": "direct"},
            ).to_dict(),
        )

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
    branch: str = Query("main", description="Target branch (default: main)"),
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
        branch = validate_branch_name(branch)
        
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
        ontology = await terminus.get_class(db_name, actual_id, branch=branch)

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


@router.post(
    "/ontology-advanced",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "Conflict (already exists / OCC)"},
    },
)
async def create_ontology_with_relationship_validation(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    auto_generate_inverse: bool = Query(False, description="(Not implemented) Auto-generate inverse metadata"),
    validate_relationships: bool = Query(True, description="Validate relationships against current schema"),
    check_circular_references: bool = Query(True, description="Reject introducing critical schema cycles"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
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
    lang = get_accept_language(request)

    try:
        if auto_generate_inverse:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail=(
                    "auto_generate_inverse is not implemented yet. TerminusDB schema documents discard "
                    "per-property custom metadata, so inverse metadata needs a dedicated projection store."
                ),
            )
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)

        from shared.utils.id_generator import generate_simple_id

        def _localized_to_string(value: Any) -> str:
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                return (
                    str(value.get("en") or "").strip()
                    or str(value.get("ko") or "").strip()
                    or next((str(v).strip() for v in value.values() if v and str(v).strip()), "")
                )
            return str(value).strip() if value is not None else ""

        ontology_dict = ontology.model_dump(exclude_unset=True)
        ontology_dict = sanitize_input(ontology_dict)

        if ontology_dict.get("id"):
            class_id = validate_class_id(ontology_dict["id"])
        else:
            class_id = generate_simple_id(
                label=_localized_to_string(ontology_dict.get("label", "")),
                use_timestamp_for_korean=True,
                default_fallback="UnnamedClass",
            )
        ontology_dict["id"] = class_id

        # Convert relationship targets from labels -> ids when mappings exist.
        for rel in ontology_dict.get("relationships", []) or []:
            if not isinstance(rel, dict):
                continue
            target = rel.get("target")
            if isinstance(target, str) and target.strip():
                mapped = await mapper.get_class_id(db_name, target, lang)
                if mapped:
                    rel["target"] = mapped

        # Reuse the same OMS compatibility transformations as `/ontology`.
        def transform_properties_for_oms(data: Dict[str, Any]) -> None:
            if 'properties' in data and isinstance(data['properties'], list):
                for prop in data['properties']:
                    if not isinstance(prop, dict):
                        continue

                    if 'name' not in prop and 'label' in prop:
                        prop['name'] = generate_simple_id(
                            _localized_to_string(prop.get("label")),
                            use_timestamp_for_korean=False,
                        )

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

                    if prop.get('type') == 'link' and 'target' in prop:
                        prop['linkTarget'] = prop.pop('target')

                    if prop.get('type') == 'array' and 'items' in prop:
                        items = prop['items']
                        if isinstance(items, dict) and items.get('type') == 'link' and 'target' in items:
                            items['linkTarget'] = items.pop('target')

        transform_properties_for_oms(ontology_dict)

        forward_headers: Dict[str, str] = {}
        for key in ("X-Admin-Token", "X-Change-Reason", "X-Admin-Actor", "X-Actor", "Authorization"):
            value = request.headers.get(key)
            if value:
                forward_headers[key] = value

        result = await terminus.create_ontology_with_advanced_relationships(
            db_name,
            ontology_dict,
            branch=branch,
            auto_generate_inverse=auto_generate_inverse,
            validate_relationships=validate_relationships,
            check_circular_references=check_circular_references,
            headers=forward_headers or None,
        )

        await mapper.register_class(db_name, class_id, ontology_dict.get('label', ''), ontology_dict.get('description'))
        for prop in ontology_dict.get('properties', []):
            if isinstance(prop, dict):
                await mapper.register_property(db_name, class_id, prop.get('name', ''), prop.get('label', ''))
        for rel in ontology_dict.get('relationships', []):
            if isinstance(rel, dict):
                await mapper.register_relationship(db_name, rel.get('predicate', ''), rel.get('label', ''))

        if isinstance(result, dict) and result.get("status") == "accepted":
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)
        if isinstance(result, dict) and "status" in result and "message" in result:
            return JSONResponse(status_code=status.HTTP_200_OK, content=result)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"온톨로지 '{class_id}'이(가) 생성되었습니다",
                data={"ontology_id": class_id, "ontology": result, "mode": "direct"},
            ).to_dict(),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create ontology with advanced relationships: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"고급 온톨로지 생성 실패: {str(e)}")


@router.post("/validate-relationships")
async def validate_ontology_relationships_bff(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    """
    🔥 온톨로지 관계 검증 (BFF 레이어 - 리팩토링됨)

    이제 중복 로직 없이 BFF Adapter를 통해 OMS로 모든 비즈니스 로직을 위임합니다.
    """
    lang = get_accept_language(request)

    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        payload = sanitize_input(ontology.model_dump(exclude_unset=True))

        if not payload.get("id"):
            from shared.utils.id_generator import generate_simple_id

            raw_label = payload.get("label") or ""
            label_str = raw_label if isinstance(raw_label, str) else (raw_label.get("en") or raw_label.get("ko") or "")
            payload["id"] = generate_simple_id(
                label=str(label_str),
                use_timestamp_for_korean=True,
                default_fallback="UnnamedClass",
            )

        for rel in payload.get("relationships", []) or []:
            if not isinstance(rel, dict):
                continue
            target = rel.get("target")
            if isinstance(target, str) and target.strip():
                mapped = await mapper.get_class_id(db_name, target, lang)
                if mapped:
                    rel["target"] = mapped

        result = await terminus.validate_relationships(db_name, payload, branch=branch)
        return ApiResponse.success(message="관계 검증이 완료되었습니다", data=result).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate relationships: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"관계 검증 실패: {str(e)}")


@router.post("/check-circular-references")
async def check_circular_references_bff(
    db_name: str,
    request: Request,
    ontology: Optional[OntologyCreateRequestBFF] = None,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    terminus: TerminusService = TerminusServiceDep,
):
    """
    🔥 순환 참조 탐지 (BFF 레이어 - 리팩토링됨)

    이제 중복 로직 없이 BFF Adapter를 통해 OMS로 모든 비즈니스 로직을 위임합니다.
    """
    lang = get_accept_language(request)

    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)

        new_ontology: Optional[Dict[str, Any]] = None
        if ontology is not None:
            payload = sanitize_input(ontology.model_dump(exclude_unset=True))

            for rel in payload.get("relationships", []) or []:
                if not isinstance(rel, dict):
                    continue
                target = rel.get("target")
                if isinstance(target, str) and target.strip():
                    mapped = await mapper.get_class_id(db_name, target, lang)
                    if mapped:
                        rel["target"] = mapped

            new_ontology = payload

        result = await terminus.detect_circular_references(db_name, branch=branch, new_ontology=new_ontology)
        return ApiResponse.success(message="순환 참조 탐지가 완료되었습니다", data=result).to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to check circular references: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"순환 참조 탐지 실패: {str(e)}")


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
        # OMS returns canonical keys: summary/graph_structure/validation/cycle_analysis.
        # Keep backwards compatibility if older shapes are returned.
        summary = analysis_result.get("summary") or analysis_result.get("relationship_summary") or {}
        validation = analysis_result.get("validation") or analysis_result.get("validation_summary") or {}
        cycles = analysis_result.get("cycle_analysis") or {}
        graph = analysis_result.get("graph_structure") or analysis_result.get("graph_summary") or {}
        recommendations = analysis_result.get("recommendations") or []

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


def _normalize_mapping_type(type_value: Any) -> str:
    if not type_value:
        return "xsd:string"
    t = str(type_value).strip()
    if not t:
        return "xsd:string"
    if t.startswith("xsd:"):
        return t

    t_lower = t.lower()

    # Funnel complex types → base XSD types (domain-neutral)
    if t_lower == "money":
        return "xsd:decimal"
    if t_lower in {"integer", "int"}:
        return "xsd:integer"
    if t_lower in {"decimal", "number", "float", "double"}:
        return "xsd:decimal"
    if t_lower in {"boolean", "bool"}:
        return "xsd:boolean"
    if t_lower in {"datetime", "timestamp"}:
        return "xsd:dateTime"
    if t_lower == "date":
        # Note: "date" may be a complex type representing date/time in upstream.
        return "xsd:dateTime"
    return "xsd:string"


def _build_source_schema_from_preview(preview: Dict[str, Any]) -> List[Dict[str, Any]]:
    inferred = preview.get("inferred_schema") or []
    out: List[Dict[str, Any]] = []

    for col in inferred:
        if not isinstance(col, dict):
            continue
        col_name = col.get("column_name") or col.get("name")
        if not col_name:
            continue
        inferred_type = col.get("inferred_type") or {}
        type_id = None
        conf = None
        if isinstance(inferred_type, dict):
            type_id = inferred_type.get("type")
            conf = inferred_type.get("confidence")
        if not type_id:
            type_id = col.get("type")

        field: Dict[str, Any] = {
            "name": str(col_name),
            "type": _normalize_mapping_type(type_id),
        }
        if conf is not None:
            field["confidence"] = conf
        if type_id is not None:
            field["original_type"] = type_id
        out.append(field)

    # Fallback: no inferred_schema → columns only
    if not out:
        for name in preview.get("columns") or []:
            out.append({"name": str(name), "type": "xsd:string"})

    return out


def _build_sample_data_from_preview(preview: Dict[str, Any]) -> List[Dict[str, Any]]:
    columns = [str(c) for c in (preview.get("columns") or [])]
    rows = preview.get("sample_data") or []
    sample: List[Dict[str, Any]] = []

    for row in rows:
        if not isinstance(row, list):
            continue
        sample.append({columns[i]: row[i] if i < len(row) else None for i in range(len(columns))})

    return sample


def _build_target_schema_from_ontology(
    ontology: Dict[str, Any],
    *,
    include_relationships: bool,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    raw_props = ontology.get("properties") or []
    for prop in raw_props:
        if isinstance(prop, dict):
            name = prop.get("name") or prop.get("id")
            if not name:
                continue
            out.append(
                {
                    "name": str(name),
                    "type": _normalize_mapping_type(prop.get("type")),
                    "label": prop.get("label"),
                }
            )
        elif isinstance(prop, str):
            out.append({"name": prop, "type": "xsd:string"})

    if include_relationships:
        raw_rels = ontology.get("relationships") or []
        for rel in raw_rels:
            if not isinstance(rel, dict):
                continue
            predicate = rel.get("predicate") or rel.get("name")
            if not predicate:
                continue
            out.append(
                {
                    "name": str(predicate),
                    "type": "link",
                    "label": rel.get("label"),
                    "target": rel.get("target"),
                }
            )

    # Keep stable order but remove duplicates by name
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for f in out:
        name = f.get("name")
        if not name or name in seen:
            continue
        seen.add(name)
        deduped.append(f)
    return deduped


def _normalize_target_schema_for_mapping(
    target_schema: List[Dict[str, Any]],
    *,
    include_relationships: bool,
) -> List[Dict[str, Any]]:
    """
    Normalize a client-provided target schema to the shape expected by MappingSuggestionService.

    This keeps behavior domain-neutral and does not require OMS.
    """
    out: List[Dict[str, Any]] = []
    for f in target_schema or []:
        if not isinstance(f, dict):
            continue
        name = f.get("name") or f.get("id") or f.get("predicate")
        if not name:
            continue
        t = f.get("type") or "xsd:string"
        norm = {**f, "name": str(name), "type": _normalize_mapping_type(t)}
        out.append(norm)

    if not include_relationships:
        out = [f for f in out if str(f.get("type") or "") != "link"]

    # Keep stable order but remove duplicates by name
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for f in out:
        name = f.get("name")
        if not name or name in seen:
            continue
        seen.add(name)
        deduped.append(f)
    return deduped


@router.post("/suggest-mappings-from-google-sheets")
async def suggest_mappings_from_google_sheets(
    db_name: str,
    request: MappingFromGoogleSheetsRequest,
):
    """
    🔥 Google Sheets → 온톨로지 클래스 매핑 자동 제안

    - Funnel의 구조 분석(멀티테이블/전치/폼)을 먼저 수행한 뒤,
    - 선택된 테이블의 source_schema를 만들고,
    - 타겟 온톨로지 클래스 schema를 불러와,
    - 매핑 후보를 반환합니다.
    """
    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(request.target_class_id)
        target_schema = _normalize_target_schema_for_mapping(
            request.target_schema,
            include_relationships=bool(request.include_relationships),
        )
        if not target_schema:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="target_schema is required for import (field types)",
            )

        from bff.services.funnel_client import FunnelClient
        from bff.services.mapping_suggestion_service import MappingSuggestionService

        async with FunnelClient() as funnel_client:
            result = await funnel_client.google_sheets_to_structure_preview(
                sheet_url=request.sheet_url,
                worksheet_name=request.worksheet_name,
                api_key=request.api_key,
                table_id=request.table_id,
                table_bbox=request.table_bbox.model_dump() if request.table_bbox is not None else None,
                include_complex_types=True,
            )

        preview = result.get("preview") or {}
        structure = result.get("structure")

        source_schema = _build_source_schema_from_preview(preview)
        sample_data = _build_sample_data_from_preview(preview)

        config = None
        try:
            import os

            config_path = os.path.join(os.path.dirname(__file__), "..", "config", "mapping_config.json")
            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    config = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load mapping config: {e}, using defaults")

        # Domain-neutral by default; semantic hints are opt-in per request.
        if request.enable_semantic_hints:
            config = {**(config or {}), "features": {**((config or {}).get("features") or {}), "semantic_match": True}}
        else:
            config = {**(config or {}), "features": {**((config or {}).get("features") or {}), "semantic_match": False}}

        suggestion_service = MappingSuggestionService(config=config)
        suggestion = suggestion_service.suggest_mappings(
            source_schema=source_schema,
            target_schema=target_schema,
            sample_data=sample_data,
            target_sample_data=None,
        )

        return {
            "status": "success",
            "message": f"Found {len(suggestion.mappings)} mapping suggestions with {suggestion.overall_confidence:.2f} overall confidence",
            "source_info": {
                "sheet_url": request.sheet_url,
                "worksheet_name": request.worksheet_name,
                "table_id": (result.get("table") or {}).get("id"),
                "table_mode": (result.get("table") or {}).get("mode"),
                "table_bbox": (result.get("table") or {}).get("bbox"),
            },
            "target_info": {"class_id": target_class_id},
            "source_schema": source_schema,
            "target_schema": target_schema,
            "mappings": [
                {
                    "source_field": m.source_field,
                    "target_field": m.target_field,
                    "confidence": m.confidence,
                    "match_type": m.match_type,
                    "reasons": m.reasons,
                }
                for m in suggestion.mappings
            ],
            "unmapped_source_fields": suggestion.unmapped_source_fields,
            "unmapped_target_fields": suggestion.unmapped_target_fields,
            "overall_confidence": suggestion.overall_confidence,
            "statistics": {
                "total_source_fields": len(source_schema),
                "total_target_fields": len(target_schema),
                "mapped_fields": len(suggestion.mappings),
                "high_confidence_mappings": len([m for m in suggestion.mappings if m.confidence >= 0.8]),
                "medium_confidence_mappings": len([m for m in suggestion.mappings if 0.6 <= m.confidence < 0.8]),
            },
            "preview_data": preview,
            "structure": structure,
        }

    except httpx.HTTPStatusError as e:
        detail = e.response.text
        try:
            detail_json = e.response.json()
            if isinstance(detail_json, dict):
                detail = detail_json.get("detail") or detail_json
        except Exception:
            pass
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Google Sheets mapping suggestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets 매핑 제안 실패: {str(e)}",
        )


@router.post("/suggest-mappings-from-excel")
async def suggest_mappings_from_excel(
    db_name: str,
    target_class_id: str = Query(..., description="Target ontology class id"),
    file: UploadFile = File(...),
    target_schema_json: Optional[str] = Form(None, description="JSON array of target schema fields"),
    sheet_name: Optional[str] = None,
    table_id: Optional[str] = None,
    table_top: Optional[int] = None,
    table_left: Optional[int] = None,
    table_bottom: Optional[int] = None,
    table_right: Optional[int] = None,
    include_relationships: bool = False,
    enable_semantic_hints: bool = False,
    max_tables: int = 5,
    max_rows: Optional[int] = None,
    max_cols: Optional[int] = None,
):
    """
    🔥 Excel 업로드 → 온톨로지 클래스 매핑 자동 제안

    - Funnel의 구조 분석(멀티테이블/전치/폼)을 먼저 수행한 뒤,
    - 선택된 테이블의 source_schema를 만들고,
    - 타겟 온톨로지 클래스 schema를 불러와,
    - 매핑 후보를 반환합니다.
    """
    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(target_class_id)

        filename = file.filename or "upload.xlsx"
        if not filename.lower().endswith((".xlsx", ".xlsm")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only .xlsx/.xlsm files are supported",
            )

        content = await file.read()
        if not content:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

        if not target_schema_json:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="target_schema_json is required for import (field types)",
            )
        try:
            target_schema_raw = json.loads(target_schema_json)
            if not isinstance(target_schema_raw, list):
                raise ValueError("target_schema_json must be a JSON array")
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid target_schema_json: {e}")

        table_bbox: Optional[Dict[str, int]] = None
        bbox_parts = [table_top, table_left, table_bottom, table_right]
        if any(v is not None for v in bbox_parts):
            if any(v is None for v in bbox_parts):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="table_top/table_left/table_bottom/table_right must be provided together",
                )
            table_bbox = {
                "top": int(table_top),
                "left": int(table_left),
                "bottom": int(table_bottom),
                "right": int(table_right),
            }

        from bff.services.funnel_client import FunnelClient
        from bff.services.mapping_suggestion_service import MappingSuggestionService

        async with FunnelClient() as funnel_client:
            result = await funnel_client.excel_to_structure_preview(
                xlsx_bytes=content,
                filename=filename,
                sheet_name=sheet_name,
                table_id=table_id,
                table_bbox=table_bbox,
                include_complex_types=True,
                max_tables=max_tables,
                max_rows=max_rows,
                max_cols=max_cols,
            )

        preview = result.get("preview") or {}
        structure = result.get("structure")

        source_schema = _build_source_schema_from_preview(preview)
        sample_data = _build_sample_data_from_preview(preview)
        target_schema = _normalize_target_schema_for_mapping(
            target_schema_raw,
            include_relationships=bool(include_relationships),
        )
        if not target_schema:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="target_schema_json produced an empty schema",
            )

        config = None
        try:
            import os

            config_path = os.path.join(os.path.dirname(__file__), "..", "config", "mapping_config.json")
            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    config = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load mapping config: {e}, using defaults")

        # Domain-neutral by default; semantic hints are opt-in per request.
        if enable_semantic_hints:
            config = {**(config or {}), "features": {**((config or {}).get("features") or {}), "semantic_match": True}}
        else:
            config = {**(config or {}), "features": {**((config or {}).get("features") or {}), "semantic_match": False}}

        suggestion_service = MappingSuggestionService(config=config)
        suggestion = suggestion_service.suggest_mappings(
            source_schema=source_schema,
            target_schema=target_schema,
            sample_data=sample_data,
            target_sample_data=None,
        )

        return {
            "status": "success",
            "message": f"Found {len(suggestion.mappings)} mapping suggestions with {suggestion.overall_confidence:.2f} overall confidence",
            "source_info": {
                "file_name": filename,
                "sheet_name": sheet_name,
                "table_id": (result.get("table") or {}).get("id"),
                "table_mode": (result.get("table") or {}).get("mode"),
                "table_bbox": (result.get("table") or {}).get("bbox"),
            },
            "target_info": {"class_id": target_class_id},
            "source_schema": source_schema,
            "target_schema": target_schema,
            "mappings": [
                {
                    "source_field": m.source_field,
                    "target_field": m.target_field,
                    "confidence": m.confidence,
                    "match_type": m.match_type,
                    "reasons": m.reasons,
                }
                for m in suggestion.mappings
            ],
            "unmapped_source_fields": suggestion.unmapped_source_fields,
            "unmapped_target_fields": suggestion.unmapped_target_fields,
            "overall_confidence": suggestion.overall_confidence,
            "statistics": {
                "total_source_fields": len(source_schema),
                "total_target_fields": len(target_schema),
                "mapped_fields": len(suggestion.mappings),
                "high_confidence_mappings": len([m for m in suggestion.mappings if m.confidence >= 0.8]),
                "medium_confidence_mappings": len([m for m in suggestion.mappings if 0.6 <= m.confidence < 0.8]),
            },
            "preview_data": preview,
            "structure": structure,
        }

    except httpx.HTTPStatusError as e:
        detail = e.response.text
        try:
            detail_json = e.response.json()
            if isinstance(detail_json, dict):
                detail = detail_json.get("detail") or detail_json
        except Exception:
            pass
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Excel mapping suggestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excel 매핑 제안 실패: {str(e)}",
        )


def _extract_target_field_types(ontology: Dict[str, Any]) -> Dict[str, str]:
    types: Dict[str, str] = {}

    for prop in ontology.get("properties") or []:
        if not isinstance(prop, dict):
            continue
        name = prop.get("name") or prop.get("id")
        if not name:
            continue
        types[str(name)] = str(prop.get("type") or "xsd:string")

    for rel in ontology.get("relationships") or []:
        if not isinstance(rel, dict):
            continue
        predicate = rel.get("predicate") or rel.get("name")
        if not predicate:
            continue
        types[str(predicate)] = "link"

    return types


def _normalize_import_target_type(type_value: Any) -> str:
    """
    Normalize user-provided target field type for import.

    This is intentionally conservative and domain-neutral.
    """
    if not type_value:
        return "xsd:string"
    t = str(type_value).strip()
    if not t:
        return "xsd:string"
    if t.startswith("xsd:") or t == "link":
        return t

    t_lower = t.lower()
    if t_lower in {"string", "text"}:
        return "xsd:string"
    if t_lower in {"int", "integer", "long"}:
        return "xsd:integer"
    if t_lower in {"decimal", "number", "float", "double"}:
        return "xsd:decimal"
    if t_lower in {"bool", "boolean"}:
        return "xsd:boolean"
    if t_lower in {"date"}:
        return "xsd:date"
    if t_lower in {"datetime", "timestamp"}:
        return "xsd:dateTime"
    if t_lower in {"money", "currency"}:
        # Stored as numeric in instances; currency metadata is not imported yet.
        return "xsd:decimal"

    return "xsd:string"


def _extract_target_field_types_from_import_schema(
    target_schema: List[ImportTargetField],
) -> Dict[str, str]:
    types: Dict[str, str] = {}
    for f in target_schema:
        name = (f.name or "").strip()
        if not name:
            continue
        types[name] = _normalize_import_target_type(f.type)
    return types


@router.post("/import-from-google-sheets/dry-run")
async def dry_run_import_from_google_sheets(
    db_name: str,
    request: ImportFromGoogleSheetsRequest,
):
    """
    Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
    """
    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(request.target_class_id)
        if not request.target_schema:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="target_schema is required for import (field types)",
            )

        dry_run_rows = max(1, min(int(request.dry_run_rows or 100), 5000))
        sample_row_limit = dry_run_rows
        if request.max_import_rows is not None:
            sample_row_limit = min(sample_row_limit, int(request.max_import_rows))

        from bff.services.funnel_client import FunnelClient
        from bff.services.sheet_import_service import FieldMapping, SheetImportService

        options = {**(request.options or {}), "sample_row_limit": sample_row_limit}
        async with FunnelClient() as funnel_client:
            result = await funnel_client.google_sheets_to_structure_preview(
                sheet_url=request.sheet_url,
                worksheet_name=request.worksheet_name,
                api_key=request.api_key,
                table_id=request.table_id,
                table_bbox=request.table_bbox.model_dump() if request.table_bbox is not None else None,
                include_complex_types=True,
                max_tables=int(request.max_tables or 5),
                max_rows=request.max_rows,
                max_cols=request.max_cols,
                trim_trailing_empty=bool(request.trim_trailing_empty),
                options=options,
            )

        table = result.get("table") or {}
        if table.get("mode") == "property":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Property-form tables are not supported for record import yet",
            )

        preview = result.get("preview") or {}
        structure = result.get("structure")
        target_field_types = _extract_target_field_types_from_import_schema(request.target_schema)
        mappings = [
            FieldMapping(source_field=m.source_field, target_field=m.target_field)
            for m in (request.mappings or [])
        ]

        build = SheetImportService.build_instances(
            columns=preview.get("columns") or [],
            rows=preview.get("sample_data") or [],
            mappings=mappings,
            target_field_types=target_field_types,
        )

        instances = build.get("instances") or []
        errors = build.get("errors") or []

        return {
            "status": "success",
            "message": "Dry-run import completed",
            "source_info": {
                "sheet_url": request.sheet_url,
                "worksheet_name": request.worksheet_name,
                "table_id": table.get("id"),
                "table_mode": table.get("mode"),
                "table_bbox": table.get("bbox"),
            },
            "target_info": {"class_id": target_class_id},
            "mapping_summary": {
                "mappings": [m.model_dump() for m in request.mappings],
                "mapping_count": len(request.mappings),
            },
            "stats": build.get("stats") or {},
            "warnings": build.get("warnings") or [],
            "errors": errors[:200],
            "sample_instances": instances[:5],
            "preview_data": preview,
            "structure": structure,
        }

    except httpx.HTTPStatusError as e:
        detail = e.response.text
        try:
            detail_json = e.response.json()
            if isinstance(detail_json, dict):
                detail = detail_json.get("detail") or detail_json
        except Exception:
            pass
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Google Sheets dry-run import failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets dry-run import 실패: {str(e)}",
        )


@router.post("/import-from-google-sheets/commit")
async def commit_import_from_google_sheets(
    db_name: str,
    request: ImportFromGoogleSheetsRequest,
    oms_client: OMSClient = OMSClientDep,
):
    """
    Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
    """
    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(request.target_class_id)
        if not request.target_schema:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="target_schema is required for import (field types)",
            )

        from bff.services.funnel_client import FunnelClient
        from bff.services.sheet_import_service import FieldMapping, SheetImportService

        sample_row_limit: int | None
        if request.max_import_rows is None:
            sample_row_limit = -1  # full
        else:
            sample_row_limit = max(0, int(request.max_import_rows))

        options = {**(request.options or {}), "sample_row_limit": sample_row_limit}
        async with FunnelClient() as funnel_client:
            result = await funnel_client.google_sheets_to_structure_preview(
                sheet_url=request.sheet_url,
                worksheet_name=request.worksheet_name,
                api_key=request.api_key,
                table_id=request.table_id,
                table_bbox=request.table_bbox.model_dump() if request.table_bbox is not None else None,
                include_complex_types=True,
                max_tables=int(request.max_tables or 5),
                max_rows=request.max_rows,
                max_cols=request.max_cols,
                trim_trailing_empty=bool(request.trim_trailing_empty),
                options=options,
            )

        table = result.get("table") or {}
        if table.get("mode") == "property":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Property-form tables are not supported for record import yet",
            )

        preview = result.get("preview") or {}
        structure = result.get("structure")
        target_field_types = _extract_target_field_types_from_import_schema(request.target_schema)
        mappings = [
            FieldMapping(source_field=m.source_field, target_field=m.target_field)
            for m in (request.mappings or [])
        ]

        build = SheetImportService.build_instances(
            columns=preview.get("columns") or [],
            rows=preview.get("sample_data") or [],
            mappings=mappings,
            target_field_types=target_field_types,
        )

        errors = build.get("errors") or []
        if errors and not request.allow_partial:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": "Import validation failed. Fix errors or set allow_partial=true to skip bad rows.",
                    "stats": build.get("stats") or {},
                    "errors": errors[:200],
                },
            )

        instances = build.get("instances") or []
        instance_row_indices = build.get("instance_row_indices") or []
        error_row_indices = set(build.get("error_row_indices") or [])

        if error_row_indices:
            filtered_instances = []
            for inst, row_idx in zip(instances, instance_row_indices):
                if row_idx in error_row_indices:
                    continue
                filtered_instances.append(inst)
            instances = filtered_instances

        if not instances:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid instances to import",
            )

        batch_size = max(1, min(int(request.batch_size or 500), 5000))
        prepared_batches = [
            {"batch_index": i // batch_size, "count": len(instances[i : i + batch_size])}
            for i in range(0, len(instances), batch_size)
        ]

        base_metadata = {
            "source": "google_sheets",
            "sheet_url": request.sheet_url,
            "worksheet_name": request.worksheet_name,
            "table_id": table.get("id"),
            "table_mode": table.get("mode"),
            "table_bbox": table.get("bbox"),
            "mappings": [m.model_dump() for m in request.mappings],
        }

        returned_instances: List[Dict[str, Any]] = []
        has_more_instances = False
        if request.return_instances:
            max_return = max(0, min(int(request.max_return_instances or 1000), 50000))
            returned_instances = instances[:max_return]
            has_more_instances = len(instances) > max_return

        submitted_commands: List[Dict[str, Any]] = []
        for batch in prepared_batches:
            idx = int(batch["batch_index"])
            start = idx * batch_size
            end = start + batch_size
            batch_instances = instances[start:end]

            resp = await oms_client.post(
                f"/api/v1/instances/{db_name}/async/{target_class_id}/bulk-create",
                params={"branch": "main"},
                json={
                    "instances": batch_instances,
                    "metadata": {
                        "import": base_metadata,
                        "import_batch": {"index": idx, "count": len(batch_instances)},
                    },
                },
            )

            command_id = (
                resp.get("command_id")
                if isinstance(resp, dict)
                else None
            )
            submitted_commands.append(
                {
                    "batch_index": idx,
                    "count": len(batch_instances),
                    "command": resp,
                    "status_url": f"/api/v1/commands/{command_id}/status" if command_id else None,
                }
            )

        return {
            "status": "success",
            "message": "Import accepted and submitted to OMS (async write pipeline started)",
            "source_info": base_metadata,
            "target_info": {"class_id": target_class_id},
            "stats": {
                **(build.get("stats") or {}),
                "prepared_instances": len(instances),
                "skipped_error_rows": len(error_row_indices),
                "batches": len(prepared_batches),
                "submitted_commands": len(submitted_commands),
            },
            "warnings": build.get("warnings") or [],
            "errors": errors[:200],
            "prepared": {
                "batch_size": batch_size,
                "batches": prepared_batches,
                "instances": returned_instances,
                "has_more_instances": has_more_instances,
            },
            "write": {
                "branch": "main",
                "commands": submitted_commands,
            },
            "structure": structure,
        }

    except httpx.HTTPStatusError as e:
        detail = e.response.text
        try:
            detail_json = e.response.json()
            if isinstance(detail_json, dict):
                detail = detail_json.get("detail") or detail_json
        except Exception:
            pass
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Google Sheets import commit failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets import 실패: {str(e)}",
        )


@router.post("/import-from-excel/dry-run")
async def dry_run_import_from_excel(
    db_name: str,
    file: UploadFile = File(...),
    target_class_id: str = Form(...),
    target_schema_json: Optional[str] = Form(None),
    mappings_json: str = Form(...),
    sheet_name: Optional[str] = Form(None),
    table_id: Optional[str] = Form(None),
    table_top: Optional[int] = Form(None),
    table_left: Optional[int] = Form(None),
    table_bottom: Optional[int] = Form(None),
    table_right: Optional[int] = Form(None),
    max_tables: int = Form(5),
    max_rows: Optional[int] = Form(None),
    max_cols: Optional[int] = Form(None),
    dry_run_rows: int = Form(100),
    max_import_rows: Optional[int] = Form(None),
    options_json: Optional[str] = Form(None),
):
    """
    Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
    """
    import json

    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(target_class_id)

        filename = file.filename or "upload.xlsx"
        if not filename.lower().endswith((".xlsx", ".xlsm")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only .xlsx/.xlsm files are supported",
            )

        content = await file.read()
        if not content:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

        try:
            mappings_raw = json.loads(mappings_json)
            if not isinstance(mappings_raw, list):
                raise ValueError("mappings_json must be a JSON array")
            mappings = [ImportFieldMapping(**m) for m in mappings_raw]
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid mappings_json: {e}")

        try:
            options = json.loads(options_json) if options_json else {}
            if not isinstance(options, dict):
                raise ValueError("options_json must be an object")
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid options_json: {e}")

        if not target_schema_json:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="target_schema_json is required for import (field types)",
            )
        try:
            target_schema_raw = json.loads(target_schema_json)
            if not isinstance(target_schema_raw, list):
                raise ValueError("target_schema_json must be a JSON array")
            target_schema = [ImportTargetField(**f) for f in target_schema_raw]
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid target_schema_json: {e}")

        target_field_types = _extract_target_field_types_from_import_schema(target_schema)

        dry_run_rows = max(1, min(int(dry_run_rows or 100), 5000))
        sample_row_limit = dry_run_rows
        if max_import_rows is not None:
            sample_row_limit = min(sample_row_limit, int(max_import_rows))

        from bff.services.funnel_client import FunnelClient
        from bff.services.sheet_import_service import FieldMapping, SheetImportService

        table_bbox: Optional[Dict[str, int]] = None
        bbox_parts = [table_top, table_left, table_bottom, table_right]
        if any(v is not None for v in bbox_parts):
            if any(v is None for v in bbox_parts):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="table_top/table_left/table_bottom/table_right must be provided together",
                )
            table_bbox = {
                "top": int(table_top),
                "left": int(table_left),
                "bottom": int(table_bottom),
                "right": int(table_right),
            }

        options = {**options, "sample_row_limit": sample_row_limit}
        async with FunnelClient() as funnel_client:
            result = await funnel_client.excel_to_structure_preview(
                xlsx_bytes=content,
                filename=filename,
                sheet_name=sheet_name,
                table_id=table_id,
                table_bbox=table_bbox,
                include_complex_types=True,
                max_tables=int(max_tables or 5),
                max_rows=max_rows,
                max_cols=max_cols,
                options=options,
            )

        table = result.get("table") or {}
        if table.get("mode") == "property":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Property-form tables are not supported for record import yet",
            )

        preview = result.get("preview") or {}
        structure = result.get("structure")
        build = SheetImportService.build_instances(
            columns=preview.get("columns") or [],
            rows=preview.get("sample_data") or [],
            mappings=[FieldMapping(source_field=m.source_field, target_field=m.target_field) for m in mappings],
            target_field_types=target_field_types,
        )

        instances = build.get("instances") or []
        errors = build.get("errors") or []

        return {
            "status": "success",
            "message": "Dry-run import completed",
            "source_info": {
                "file_name": filename,
                "sheet_name": sheet_name,
                "table_id": table.get("id"),
                "table_mode": table.get("mode"),
                "table_bbox": table.get("bbox"),
            },
            "target_info": {"class_id": target_class_id},
            "mapping_summary": {
                "mappings": [m.model_dump() for m in mappings],
                "mapping_count": len(mappings),
            },
            "stats": build.get("stats") or {},
            "warnings": build.get("warnings") or [],
            "errors": errors[:200],
            "sample_instances": instances[:5],
            "preview_data": preview,
            "structure": structure,
        }

    except httpx.HTTPStatusError as e:
        detail = e.response.text
        try:
            detail_json = e.response.json()
            if isinstance(detail_json, dict):
                detail = detail_json.get("detail") or detail_json
        except Exception:
            pass
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Excel dry-run import failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excel dry-run import 실패: {str(e)}",
        )


@router.post("/import-from-excel/commit")
async def commit_import_from_excel(
    db_name: str,
    file: UploadFile = File(...),
    target_class_id: str = Form(...),
    target_schema_json: Optional[str] = Form(None),
    mappings_json: str = Form(...),
    sheet_name: Optional[str] = Form(None),
    table_id: Optional[str] = Form(None),
    table_top: Optional[int] = Form(None),
    table_left: Optional[int] = Form(None),
    table_bottom: Optional[int] = Form(None),
    table_right: Optional[int] = Form(None),
    max_tables: int = Form(5),
    max_rows: Optional[int] = Form(None),
    max_cols: Optional[int] = Form(None),
    allow_partial: bool = Form(False),
    max_import_rows: Optional[int] = Form(None),
    batch_size: int = Form(500),
    return_instances: bool = Form(False),
    max_return_instances: int = Form(1000),
    options_json: Optional[str] = Form(None),
    oms_client: OMSClient = OMSClientDep,
):
    """
    Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
    """
    import json

    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(target_class_id)

        filename = file.filename or "upload.xlsx"
        if not filename.lower().endswith((".xlsx", ".xlsm")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only .xlsx/.xlsm files are supported",
            )

        content = await file.read()
        if not content:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

        try:
            mappings_raw = json.loads(mappings_json)
            if not isinstance(mappings_raw, list):
                raise ValueError("mappings_json must be a JSON array")
            mappings = [ImportFieldMapping(**m) for m in mappings_raw]
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid mappings_json: {e}")

        try:
            options = json.loads(options_json) if options_json else {}
            if not isinstance(options, dict):
                raise ValueError("options_json must be an object")
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid options_json: {e}")

        if not target_schema_json:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="target_schema_json is required for import (field types)",
            )
        try:
            target_schema_raw = json.loads(target_schema_json)
            if not isinstance(target_schema_raw, list):
                raise ValueError("target_schema_json must be a JSON array")
            target_schema = [ImportTargetField(**f) for f in target_schema_raw]
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid target_schema_json: {e}")

        target_field_types = _extract_target_field_types_from_import_schema(target_schema)

        sample_row_limit: int | None
        if max_import_rows is None:
            sample_row_limit = -1  # full
        else:
            sample_row_limit = max(0, int(max_import_rows))

        from bff.services.funnel_client import FunnelClient
        from bff.services.sheet_import_service import FieldMapping, SheetImportService

        table_bbox: Optional[Dict[str, int]] = None
        bbox_parts = [table_top, table_left, table_bottom, table_right]
        if any(v is not None for v in bbox_parts):
            if any(v is None for v in bbox_parts):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="table_top/table_left/table_bottom/table_right must be provided together",
                )
            table_bbox = {
                "top": int(table_top),
                "left": int(table_left),
                "bottom": int(table_bottom),
                "right": int(table_right),
            }

        options = {**options, "sample_row_limit": sample_row_limit}
        async with FunnelClient() as funnel_client:
            result = await funnel_client.excel_to_structure_preview(
                xlsx_bytes=content,
                filename=filename,
                sheet_name=sheet_name,
                table_id=table_id,
                table_bbox=table_bbox,
                include_complex_types=True,
                max_tables=int(max_tables or 5),
                max_rows=max_rows,
                max_cols=max_cols,
                options=options,
            )

        table = result.get("table") or {}
        if table.get("mode") == "property":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Property-form tables are not supported for record import yet",
            )

        preview = result.get("preview") or {}
        structure = result.get("structure")
        build = SheetImportService.build_instances(
            columns=preview.get("columns") or [],
            rows=preview.get("sample_data") or [],
            mappings=[FieldMapping(source_field=m.source_field, target_field=m.target_field) for m in mappings],
            target_field_types=target_field_types,
        )

        errors = build.get("errors") or []
        if errors and not allow_partial:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": "Import validation failed. Fix errors or set allow_partial=true to skip bad rows.",
                    "stats": build.get("stats") or {},
                    "errors": errors[:200],
                },
            )

        instances = build.get("instances") or []
        instance_row_indices = build.get("instance_row_indices") or []
        error_row_indices = set(build.get("error_row_indices") or [])

        if error_row_indices:
            filtered_instances = []
            for inst, row_idx in zip(instances, instance_row_indices):
                if row_idx in error_row_indices:
                    continue
                filtered_instances.append(inst)
            instances = filtered_instances

        if not instances:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid instances to import")

        batch_size = max(1, min(int(batch_size or 500), 5000))
        prepared_batches = [
            {"batch_index": i // batch_size, "count": len(instances[i : i + batch_size])}
            for i in range(0, len(instances), batch_size)
        ]

        base_metadata = {
            "source": "excel",
            "file_name": filename,
            "sheet_name": sheet_name,
            "table_id": table.get("id"),
            "table_mode": table.get("mode"),
            "table_bbox": table.get("bbox"),
            "mappings": [m.model_dump() for m in mappings],
        }

        returned_instances: List[Dict[str, Any]] = []
        has_more_instances = False
        if return_instances:
            max_return = max(0, min(int(max_return_instances or 1000), 50000))
            returned_instances = instances[:max_return]
            has_more_instances = len(instances) > max_return

        submitted_commands: List[Dict[str, Any]] = []
        for batch in prepared_batches:
            idx = int(batch["batch_index"])
            start = idx * batch_size
            end = start + batch_size
            batch_instances = instances[start:end]

            resp = await oms_client.post(
                f"/api/v1/instances/{db_name}/async/{target_class_id}/bulk-create",
                params={"branch": "main"},
                json={
                    "instances": batch_instances,
                    "metadata": {
                        "import": base_metadata,
                        "import_batch": {"index": idx, "count": len(batch_instances)},
                    },
                },
            )

            command_id = (
                resp.get("command_id")
                if isinstance(resp, dict)
                else None
            )
            submitted_commands.append(
                {
                    "batch_index": idx,
                    "count": len(batch_instances),
                    "command": resp,
                    "status_url": f"/api/v1/commands/{command_id}/status" if command_id else None,
                }
            )

        return {
            "status": "success",
            "message": "Import accepted and submitted to OMS (async write pipeline started)",
            "source_info": base_metadata,
            "target_info": {"class_id": target_class_id},
            "stats": {
                **(build.get("stats") or {}),
                "prepared_instances": len(instances),
                "skipped_error_rows": len(error_row_indices),
                "batches": len(prepared_batches),
                "submitted_commands": len(submitted_commands),
            },
            "warnings": build.get("warnings") or [],
            "errors": errors[:200],
            "prepared": {
                "batch_size": batch_size,
                "batches": prepared_batches,
                "instances": returned_instances,
                "has_more_instances": has_more_instances,
            },
            "write": {
                "branch": "main",
                "commands": submitted_commands,
            },
            "structure": structure,
        }

    except httpx.HTTPStatusError as e:
        detail = e.response.text
        try:
            detail_json = e.response.json()
            if isinstance(detail_json, dict):
                detail = detail_json.get("detail") or detail_json
        except Exception:
            pass
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Excel import commit failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excel import 실패: {str(e)}",
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
                table_id=request.table_id,
                table_bbox=request.table_bbox.model_dump() if request.table_bbox is not None else None,
            )

            preview = result.get("preview", {})
            schema_suggestion = result.get("schema_suggestion", {})
            structure = result.get("structure")

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
                "structure": structure,
            }

    except httpx.HTTPStatusError as e:
        detail = e.response.text
        try:
            detail_json = e.response.json()
            if isinstance(detail_json, dict):
                detail = detail_json.get("detail") or detail_json
        except Exception:
            pass
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Google Sheets schema suggestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets 스키마 제안 실패: {str(e)}",
        )


@router.post("/suggest-schema-from-excel")
async def suggest_schema_from_excel(
    db_name: str,
    file: UploadFile = File(...),
    sheet_name: Optional[str] = None,
    class_name: Optional[str] = None,
    table_id: Optional[str] = None,
    table_top: Optional[int] = None,
    table_left: Optional[int] = None,
    table_bottom: Optional[int] = None,
    table_right: Optional[int] = None,
    include_complex_types: bool = True,
    max_tables: int = 5,
    max_rows: Optional[int] = None,
    max_cols: Optional[int] = None,
):
    """
    🔥 Excel 업로드에서 스키마 자동 제안

    Excel(.xlsx/.xlsm) 파일을 업로드 받아 분석하고 OMS 온톨로지 스키마를 자동으로 생성합니다.
    """
    try:
        db_name = validate_db_name(db_name)

        filename = file.filename or "upload.xlsx"
        if not filename.lower().endswith((".xlsx", ".xlsm")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only .xlsx/.xlsm files are supported",
            )

        content = await file.read()
        if not content:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Empty file")

        from bff.services.funnel_client import FunnelClient

        table_bbox: Optional[Dict[str, int]] = None
        bbox_parts = [table_top, table_left, table_bottom, table_right]
        if any(v is not None for v in bbox_parts):
            if any(v is None for v in bbox_parts):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="table_top/table_left/table_bottom/table_right must be provided together",
                )
            table_bbox = {
                "top": int(table_top),
                "left": int(table_left),
                "bottom": int(table_bottom),
                "right": int(table_right),
            }

        async with FunnelClient() as funnel_client:
            result = await funnel_client.excel_to_schema(
                xlsx_bytes=content,
                filename=filename,
                sheet_name=sheet_name,
                class_name=class_name,
                table_id=table_id,
                table_bbox=table_bbox,
                include_complex_types=include_complex_types,
                max_tables=max_tables,
                max_rows=max_rows,
                max_cols=max_cols,
            )

        preview = result.get("preview", {})
        schema_suggestion = result.get("schema_suggestion", {})
        structure = result.get("structure")

        total_rows = preview.get("total_rows", 0)
        preview_rows = preview.get("preview_rows", 0)
        columns = preview.get("columns", [])

        return {
            "status": "success",
            "message": f"Excel 스키마 제안이 완료되었습니다. (추정) {total_rows}행 중 {preview_rows}행 샘플 분석됨",
            "suggested_schema": schema_suggestion,
            "source_info": {
                "file_name": filename,
                "sheet_name": sheet_name,
                "total_rows": total_rows,
                "preview_rows": preview_rows,
                "columns": columns,
            },
            "preview_data": preview,
            "structure": structure,
        }

    except httpx.HTTPStatusError as e:
        # Propagate Funnel's status codes (e.g., 501 when openpyxl is missing)
        detail = e.response.text
        try:
            detail_json = e.response.json()
            if isinstance(detail_json, dict):
                detail = detail_json.get("detail") or detail_json
        except Exception:
            pass
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Excel schema suggestion failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excel 스키마 제안 실패: {str(e)}",
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
