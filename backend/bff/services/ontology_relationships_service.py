"""Ontology relationship service (BFF).

Extracted from `bff.routers.ontology_relationships` to keep routers thin,
deduplicate label→id conversion flows, and centralize error mapping
(Facade pattern).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse

from bff.dependencies import LabelMapper, TerminusService
from bff.routers.ontology_ops import _transform_properties_for_oms
from bff.services.ontology_class_id_service import resolve_or_generate_class_id
from bff.services.input_validation_service import (
    sanitized_payload,
    validated_branch_name,
    validated_db_name,
)
from bff.services.ontology_label_mapper_service import map_relationship_targets, register_ontology_label_mappings
from bff.utils.request_headers import extract_forward_headers
from shared.models.ontology import OntologyCreateRequestBFF
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_class_id,
)
from shared.utils.language import get_accept_language

logger = logging.getLogger(__name__)

_ALLOWED_PATH_TYPES = {"shortest", "all", "weighted", "semantic"}

async def create_ontology_with_relationship_validation(
    *,
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    request: Request,
    branch: str,
    auto_generate_inverse: bool,
    validate_relationships: bool,
    check_circular_references: bool,
    mapper: LabelMapper,
    terminus: TerminusService,
) -> JSONResponse:
    """
    Create ontology with advanced relationship validation (BFF).

    Delegates to OMS via the TerminusService adapter while handling:
    - id generation
    - label→id mapping for relationship targets
    - label-mapping registry updates
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

        db_name = validated_db_name(db_name)
        branch = validated_branch_name(branch)

        ontology_dict = sanitized_payload(ontology.model_dump(exclude_unset=True))
        class_id = resolve_or_generate_class_id(ontology_dict)
        ontology_dict["id"] = class_id

        await map_relationship_targets(mapper=mapper, db_name=db_name, ontology_dict=ontology_dict, lang=lang)
        _transform_properties_for_oms(ontology_dict)

        forward_headers = extract_forward_headers(request)
        result = await terminus.create_ontology_with_advanced_relationships(
            db_name,
            ontology_dict,
            branch=branch,
            auto_generate_inverse=auto_generate_inverse,
            validate_relationships=validate_relationships,
            check_circular_references=check_circular_references,
            headers=forward_headers or None,
        )

        await register_ontology_label_mappings(
            mapper=mapper,
            db_name=db_name,
            class_id=class_id,
            ontology_dict=ontology_dict,
        )

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
    except Exception as exc:
        logger.error("Failed to create ontology with advanced relationships: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"고급 온톨로지 생성 실패: {str(exc)}",
        ) from exc


async def validate_ontology_relationships(
    *,
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    request: Request,
    branch: str,
    mapper: LabelMapper,
    terminus: TerminusService,
) -> Dict[str, Any]:
    """Validate ontology relationships (no write)."""
    lang = get_accept_language(request)
    try:
        db_name = validated_db_name(db_name)
        branch = validated_branch_name(branch)
        payload = sanitized_payload(ontology.model_dump(exclude_unset=True))

        if not payload.get("id"):
            payload["id"] = resolve_or_generate_class_id(payload)
        else:
            payload["id"] = validate_class_id(str(payload["id"]))

        await map_relationship_targets(mapper=mapper, db_name=db_name, ontology_dict=payload, lang=lang)

        result = await terminus.validate_relationships(db_name, payload, branch=branch)
        return ApiResponse.success(message="관계 검증이 완료되었습니다", data=result).to_dict()
    except (SecurityViolationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to validate relationships: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"관계 검증 실패: {str(exc)}",
        ) from exc


async def check_circular_references(
    *,
    db_name: str,
    request: Request,
    ontology: Optional[OntologyCreateRequestBFF],
    branch: str,
    mapper: LabelMapper,
    terminus: TerminusService,
) -> Dict[str, Any]:
    """Detect circular references in ontology graph (no write)."""
    lang = get_accept_language(request)
    try:
        db_name = validated_db_name(db_name)
        branch = validated_branch_name(branch)

        new_ontology: Optional[Dict[str, Any]] = None
        if ontology is not None:
            payload = sanitized_payload(ontology.model_dump(exclude_unset=True))
            await map_relationship_targets(mapper=mapper, db_name=db_name, ontology_dict=payload, lang=lang)
            new_ontology = payload

        result = await terminus.detect_circular_references(db_name, branch=branch, new_ontology=new_ontology)
        return ApiResponse.success(message="순환 참조 탐지가 완료되었습니다", data=result).to_dict()
    except (SecurityViolationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to check circular references: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"순환 참조 탐지 실패: {str(exc)}",
        ) from exc


async def analyze_relationship_network(
    *,
    db_name: str,
    request: Request,
    terminus: TerminusService,
) -> Dict[str, Any]:
    """Analyze ontology relationship network and return user-friendly metrics."""
    lang = get_accept_language(request)
    try:
        db_name = validated_db_name(db_name)
        analysis_result = await terminus.analyze_relationship_network(db_name)

        summary = analysis_result.get("summary") or analysis_result.get("relationship_summary") or {}
        validation = analysis_result.get("validation") or analysis_result.get("validation_summary") or {}
        cycles = analysis_result.get("cycle_analysis") or {}
        graph = analysis_result.get("graph_structure") or analysis_result.get("graph_summary") or {}
        recommendations = analysis_result.get("recommendations") or []

        health_score = 100
        if validation.get("errors", 0) > 0:
            health_score -= validation["errors"] * 10
        if cycles.get("critical_cycles", 0) > 0:
            health_score -= cycles["critical_cycles"] * 15
        if validation.get("warnings", 0) > 0:
            health_score -= validation["warnings"] * 2
        health_score = max(0, min(100, health_score))

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
                    "message": (
                        rec.replace("❌ ", "")
                        .replace("⚠️ ", "")
                        .replace("📝 ", "")
                        .replace("🔄 ", "")
                    ),
                }
                for rec in recommendations
            ],
            "metadata": {
                "analysis_timestamp": analysis_result.get("analysis_timestamp"),
                "database": db_name,
                "language": lang,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to analyze relationship network: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"관계 네트워크 분석 실패: {str(exc)}",
        ) from exc


async def find_relationship_paths(
    *,
    request: Request,
    db_name: str,
    start_entity: str,
    end_entity: Optional[str],
    max_depth: int,
    path_type: str,
    terminus: TerminusService,
    mapper: LabelMapper,
) -> Dict[str, Any]:
    """Find relationship paths between ontology entities."""
    lang = get_accept_language(request)
    try:
        db_name = validated_db_name(db_name)
        start_entity = sanitize_input(start_entity)
        if end_entity:
            end_entity = sanitize_input(end_entity)

        if path_type not in _ALLOWED_PATH_TYPES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid path_type. Allowed values: {', '.join(_ALLOWED_PATH_TYPES)}",
            )

        start_id = await mapper.get_class_id(db_name, start_entity, lang)
        if not start_id:
            start_id = start_entity

        end_id = None
        if end_entity:
            end_id = await mapper.get_class_id(db_name, end_entity, lang)
            if not end_id:
                end_id = end_entity

        path_result = await terminus.find_relationship_paths(
            db_name=db_name,
            start_entity=start_id,
            end_entity=end_id,
            max_depth=max_depth,
            path_type=path_type,
        )

        paths = path_result.get("paths", [])
        user_friendly_paths = []
        for path in paths:
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
            "paths": user_friendly_paths[:10],
            "statistics": {
                "total_paths_found": len(paths),
                "displayed_paths": min(len(user_friendly_paths), 10),
                "average_length": statistics.get("average_length", 0),
                "shortest_path_length": statistics.get("min_length", 0),
                "longest_path_length": statistics.get("max_length", 0),
            },
            "metadata": {"language": lang, "start_id": start_id, "end_id": end_id},
        }
    except (SecurityViolationError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to find relationship paths: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"관계 경로 탐색 실패: {str(exc)}",
        ) from exc
