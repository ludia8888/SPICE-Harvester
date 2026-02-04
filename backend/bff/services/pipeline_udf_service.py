"""Pipeline UDF domain logic (BFF).

Extracted from `bff.routers.pipeline_udfs` to keep routers thin.
"""

import logging
from typing import Any, Optional
from uuid import UUID

from fastapi import HTTPException, status

from shared.models.requests import ApiResponse
from shared.security.input_sanitizer import validate_db_name

logger = logging.getLogger(__name__)


def _parse_uuid_or_404(value: str, *, detail: str) -> str:
    try:
        return str(UUID(str(value)))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail) from exc


async def create_udf(
    *,
    db_name: str,
    name: str,
    code: str,
    description: Optional[str],
    pipeline_registry: Any,
) -> dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        udf = await pipeline_registry.create_udf(
            db_name=db_name,
            name=name.strip(),
            code=code,
            description=description,
        )
        return ApiResponse.success(
            message="UDF created",
            data={
                "udf_id": udf.udf_id,
                "db_name": udf.db_name,
                "name": udf.name,
                "description": udf.description,
                "latest_version": udf.latest_version,
                "created_at": udf.created_at.isoformat() if udf.created_at else None,
                "updated_at": udf.updated_at.isoformat() if udf.updated_at else None,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create UDF: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def list_udfs(
    *,
    db_name: str,
    pipeline_registry: Any,
) -> dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        udfs = await pipeline_registry.list_udfs(db_name=db_name)
        return ApiResponse.success(
            message="UDFs retrieved",
            data={
                "udfs": [
                    {
                        "udf_id": udf.udf_id,
                        "db_name": udf.db_name,
                        "name": udf.name,
                        "description": udf.description,
                        "latest_version": udf.latest_version,
                        "created_at": udf.created_at.isoformat() if udf.created_at else None,
                        "updated_at": udf.updated_at.isoformat() if udf.updated_at else None,
                    }
                    for udf in udfs
                ]
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list UDFs: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def get_udf(
    *,
    udf_id: str,
    pipeline_registry: Any,
) -> dict[str, Any]:
    try:
        udf_id = _parse_uuid_or_404(udf_id, detail="UDF not found")
        udf = await pipeline_registry.get_udf(udf_id=udf_id)
        if not udf:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="UDF not found")

        latest_version = await pipeline_registry.get_udf_latest_version(udf_id=udf_id)
        return ApiResponse.success(
            message="UDF retrieved",
            data={
                "udf_id": udf.udf_id,
                "db_name": udf.db_name,
                "name": udf.name,
                "description": udf.description,
                "latest_version": udf.latest_version,
                "created_at": udf.created_at.isoformat() if udf.created_at else None,
                "updated_at": udf.updated_at.isoformat() if udf.updated_at else None,
                "code": latest_version.code if latest_version else None,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get UDF: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def create_udf_version(
    *,
    udf_id: str,
    code: str,
    pipeline_registry: Any,
) -> dict[str, Any]:
    try:
        udf_id = _parse_uuid_or_404(udf_id, detail="UDF not found")
        udf = await pipeline_registry.get_udf(udf_id=udf_id)
        if not udf:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="UDF not found")

        version = await pipeline_registry.create_udf_version(
            udf_id=udf_id,
            code=code,
        )
        return ApiResponse.success(
            message="UDF version created",
            data={
                "version_id": version.version_id,
                "udf_id": version.udf_id,
                "version": version.version,
                "code": version.code,
                "created_at": version.created_at.isoformat() if version.created_at else None,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create UDF version: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def get_udf_version(
    *,
    udf_id: str,
    version: int,
    pipeline_registry: Any,
) -> dict[str, Any]:
    try:
        udf_id = _parse_uuid_or_404(udf_id, detail="UDF version not found")
        udf_version = await pipeline_registry.get_udf_version(udf_id=udf_id, version=version)
        if not udf_version:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="UDF version not found")

        return ApiResponse.success(
            message="UDF version retrieved",
            data={
                "version_id": udf_version.version_id,
                "udf_id": udf_version.udf_id,
                "version": udf_version.version,
                "code": udf_version.code,
                "created_at": udf_version.created_at.isoformat() if udf_version.created_at else None,
            },
        ).to_dict()
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get UDF version: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

