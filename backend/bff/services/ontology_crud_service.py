"""Ontology CRUD service (BFF).

Extracted from `bff.routers.ontology_crud` to keep routers thin and to
deduplicate common CRUD/control-flow (Facade pattern).
"""

from __future__ import annotations

import logging

from fastapi import status
from fastapi.responses import JSONResponse

from bff.routers.ontology_ops import _transform_properties_for_oms
from bff.services.ontology_class_id_service import resolve_or_generate_class_id
from bff.services.ontology_label_mapper_service import register_ontology_label_mappings
from bff.services.oms_error_policy import raise_oms_boundary_exception
from bff.services.oms_client import OMSClient
from shared.models.ontology import OntologyCreateRequestBFF
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import (
    sanitize_input,
    validate_branch_name,
    validate_db_name,
)
from shared.utils.label_mapper import LabelMapper
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


@trace_external_call("bff.ontology_crud.create_ontology")
async def create_ontology(
    *,
    db_name: str,
    body: OntologyCreateRequestBFF,
    branch: str,
    mapper: LabelMapper,
    oms_client: OMSClient,
) -> JSONResponse:
    """
    온톨로지 생성

    새로운 온톨로지 클래스를 생성합니다.
    레이블 기반으로 ID가 자동 생성됩니다.
    """
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)

        ontology_dict = sanitize_input(body.model_dump(exclude_unset=True))

        class_id = resolve_or_generate_class_id(ontology_dict)

        ontology_dict["id"] = class_id
        _transform_properties_for_oms(ontology_dict, log_conversions=True)

        result = await oms_client.create_ontology(db_name, ontology_dict, branch=branch)
        await register_ontology_label_mappings(
            mapper=mapper,
            db_name=db_name,
            class_id=class_id,
            ontology_dict=ontology_dict,
        )

        if isinstance(result, dict) and result.get("status") == "accepted" and "data" in result:
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        if isinstance(result, dict) and "status" in result and "message" in result:
            return JSONResponse(status_code=status.HTTP_200_OK, content=result)

        if isinstance(result, dict) and "id" in result:
            class_id = result.get("id") or class_id

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"온톨로지 '{class_id}'이(가) 생성되었습니다",
                data={"ontology_id": class_id, "ontology": result, "mode": "direct"},
            ).to_dict(),
        )

    except Exception as exc:
        logging.getLogger(__name__).warning("Exception fallback at bff/services/ontology_crud_service.py:105", exc_info=True)
        raise_oms_boundary_exception(exc=exc, action="온톨로지 생성", logger=logger)
