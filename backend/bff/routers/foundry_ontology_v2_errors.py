from __future__ import annotations

import logging
from typing import Any, Dict

import httpx
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse

from shared.foundry.errors import foundry_error
from shared.errors.infra_errors import RegistryUnavailableError
from shared.security.database_access import DatabaseAccessRegistryUnavailableError
from shared.security.input_sanitizer import SecurityViolationError
from shared.utils.action_permission_profile import ActionPermissionProfileError

logger = logging.getLogger(__name__)


class OntologyNotFoundError(Exception):
    pass


class PermissionDeniedError(Exception):
    pass


class ApiFeaturePreviewUsageOnlyError(ValueError):
    pass


class ObjectSetNotFoundError(ValueError):
    pass


_ONTOLOGY_HANDLED_EXCEPTIONS = (
    OntologyNotFoundError,
    PermissionDeniedError,
    ApiFeaturePreviewUsageOnlyError,
    ObjectSetNotFoundError,
    SecurityViolationError,
    ActionPermissionProfileError,
    httpx.HTTPError,
    HTTPException,
    RuntimeError,
    LookupError,
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    OSError,
    AssertionError,
)


def _foundry_error(
    status_code: int,
    *,
    error_code: str,
    error_name: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return foundry_error(
        int(status_code),
        error_code=error_code,
        error_name=error_name,
        parameters=parameters or {},
    )


def _passthrough_upstream_error_payload(exc: httpx.HTTPStatusError) -> JSONResponse | None:
    response = exc.response
    if response is None:
        return None
    try:
        payload = response.json()
    except ValueError:
        logger.warning("Failed to decode upstream error payload as JSON", exc_info=True)
        return None
    if not isinstance(payload, dict):
        return None
    return JSONResponse(status_code=response.status_code, content=payload)


def _named_not_found(error_name: str, *, parameters: Dict[str, Any]) -> JSONResponse:
    return _foundry_error(
        status.HTTP_404_NOT_FOUND,
        error_code="NOT_FOUND",
        error_name=error_name,
        parameters=parameters,
    )


def _error_parameters(
    *,
    ontology: str,
    object_type: str | None = None,
    link_type: str | None = None,
    primary_key: str | None = None,
    linked_primary_key: str | None = None,
    parameters: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"ontology": ontology}
    if object_type is not None:
        payload["objectType"] = object_type
    if link_type is not None:
        payload["linkType"] = link_type
    if primary_key is not None:
        payload["primaryKey"] = primary_key
    if linked_primary_key is not None:
        payload["linkedObjectPrimaryKey"] = linked_primary_key
    if parameters:
        payload.update({str(key): value for key, value in parameters.items() if value is not None})
    return payload


def _not_found_error(
    error_name: str,
    *,
    ontology: str,
    object_type: str | None = None,
    link_type: str | None = None,
    primary_key: str | None = None,
    linked_primary_key: str | None = None,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return _named_not_found(
        error_name,
        parameters=_error_parameters(
            ontology=ontology,
            object_type=object_type,
            link_type=link_type,
            primary_key=primary_key,
            linked_primary_key=linked_primary_key,
            parameters=parameters,
        ),
    )


def _permission_denied(
    *,
    ontology: str,
    message: str = "Permission denied",
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    payload = {"ontology": ontology, "message": message}
    if parameters:
        payload.update(parameters)
    return _foundry_error(
        status.HTTP_403_FORBIDDEN,
        error_code="PERMISSION_DENIED",
        error_name="PermissionDenied",
        parameters=payload,
    )


def _service_http_error_response(
    exc: HTTPException,
    *,
    ontology: str,
    object_type: str | None = None,
) -> JSONResponse:
    status_code = int(getattr(exc, "status_code", status.HTTP_500_INTERNAL_SERVER_ERROR))
    detail = getattr(exc, "detail", None)
    message = str(exc)
    if isinstance(detail, dict):
        message = str(detail.get("message") or detail.get("detail") or detail.get("error") or message)
    elif detail:
        message = str(detail)

    if status_code == status.HTTP_404_NOT_FOUND:
        if object_type:
            return _not_found_error("ObjectTypeNotFound", ontology=ontology, object_type=object_type)
        return _not_found_error("OntologyNotFound", ontology=ontology)
    if status_code == status.HTTP_403_FORBIDDEN:
        return _permission_denied(ontology=ontology, message=message)

    error_code = "INVALID_ARGUMENT" if 400 <= status_code < 500 else "INTERNAL"
    error_name = "InvalidArgument" if 400 <= status_code < 500 else "Internal"
    parameters = _error_parameters(
        ontology=ontology,
        object_type=object_type,
        parameters={"message": message},
    )
    return _foundry_error(status_code, error_code=error_code, error_name=error_name, parameters=parameters)


def _preflight_error_response(
    exc: Exception,
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    scoped = {str(key): value for key, value in (parameters or {}).items() if value is not None}
    if isinstance(exc, OntologyNotFoundError):
        return _not_found_error("OntologyNotFound", ontology=ontology, parameters=scoped or None)
    if isinstance(exc, ObjectSetNotFoundError):
        return _not_found_error("ObjectSetNotFound", ontology=ontology, parameters=scoped or None)
    if isinstance(exc, PermissionDeniedError):
        return _permission_denied(ontology=ontology, message=str(exc), parameters=scoped or None)
    if isinstance(exc, DatabaseAccessRegistryUnavailableError):
        return _foundry_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code="UPSTREAM_UNAVAILABLE",
            error_name="UpstreamUnavailable",
            parameters={"ontology": ontology, **scoped, "message": "Database access registry unavailable"},
        )
    if isinstance(exc, RegistryUnavailableError):
        return _foundry_error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code="UPSTREAM_UNAVAILABLE",
            error_name="UpstreamUnavailable",
            parameters={"ontology": ontology, **scoped, "message": str(exc)},
        )
    if isinstance(exc, ApiFeaturePreviewUsageOnlyError):
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="ApiFeaturePreviewUsageOnly",
            parameters={"ontology": ontology, **scoped, "message": str(exc)},
        )
    if isinstance(exc, (ValueError, SecurityViolationError)):
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters={"ontology": ontology, **scoped, "message": str(exc)},
        )
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
        return _foundry_error(
            status_code,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": ontology, **scoped},
        )
    if isinstance(exc, httpx.HTTPError):
        return _foundry_error(
            status.HTTP_502_BAD_GATEWAY,
            error_code="UPSTREAM_ERROR",
            error_name="UpstreamError",
            parameters={"ontology": ontology, **scoped},
        )
    logger.error("Foundry v2 preflight failed (%s): %s", ontology, exc)
    return _foundry_error(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_code="INTERNAL",
        error_name="Internal",
        parameters={"ontology": ontology, **scoped},
    )


def _scoped_error_parameters(
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"ontology": ontology}
    if parameters:
        payload.update({str(key): value for key, value in parameters.items() if value is not None})
    return payload


def _normalize_non_foundry_upstream_error(
    exc: httpx.HTTPStatusError,
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
    action_surface: bool = False,
) -> JSONResponse | None:
    response = exc.response
    if response is None:
        return None

    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    if "errorCode" in payload and "errorName" in payload:
        return None

    status_code = int(response.status_code)
    error_code = str(payload.get("code") or "").strip().upper()
    message = str(payload.get("message") or "").strip()
    merged_parameters = _scoped_error_parameters(ontology=ontology, parameters=parameters)
    if message:
        merged_parameters["message"] = message

    if status_code in {status.HTTP_400_BAD_REQUEST, status.HTTP_422_UNPROCESSABLE_ENTITY} or error_code == "REQUEST_VALIDATION_FAILED":
        if action_surface:
            return _foundry_error(
                status.HTTP_400_BAD_REQUEST,
                error_code="ACTION_VALIDATION_FAILED",
                error_name="ActionValidationFailed",
                parameters=merged_parameters,
            )
        return _foundry_error(
            status.HTTP_400_BAD_REQUEST,
            error_code="INVALID_ARGUMENT",
            error_name="InvalidArgument",
            parameters=merged_parameters,
        )
    if status_code == status.HTTP_403_FORBIDDEN:
        if action_surface:
            return _foundry_error(
                status.HTTP_403_FORBIDDEN,
                error_code="PERMISSION_DENIED",
                error_name="EditObjectPermissionDenied",
                parameters=merged_parameters,
            )
        return _foundry_error(
            status.HTTP_403_FORBIDDEN,
            error_code="PERMISSION_DENIED",
            error_name="PermissionDenied",
            parameters=merged_parameters,
        )
    if status_code == status.HTTP_404_NOT_FOUND:
        if action_surface:
            return _foundry_error(
                status.HTTP_404_NOT_FOUND,
                error_code="NOT_FOUND",
                error_name="ActionTypeNotFound",
                parameters=merged_parameters,
            )
        return _foundry_error(
            status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            error_name="NotFound",
            parameters=merged_parameters,
        )
    if status_code == status.HTTP_409_CONFLICT:
        return _foundry_error(
            status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            error_name="Conflict",
            parameters=merged_parameters,
        )
    return None


def _upstream_status_error_response(
    exc: httpx.HTTPStatusError,
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
    not_found_response: JSONResponse | None = None,
    passthrough_payload: bool = False,
    normalize_non_foundry_payload: bool = False,
    action_surface: bool = False,
) -> JSONResponse:
    status_code = exc.response.status_code if exc.response is not None else status.HTTP_502_BAD_GATEWAY
    if normalize_non_foundry_payload:
        normalized = _normalize_non_foundry_upstream_error(
            exc,
            ontology=ontology,
            parameters=parameters,
            action_surface=action_surface,
        )
        if normalized is not None:
            return normalized
    if passthrough_payload:
        passthrough = _passthrough_upstream_error_payload(exc)
        if passthrough is not None:
            return passthrough
    if status_code == status.HTTP_404_NOT_FOUND and not_found_response is not None:
        return not_found_response
    return _foundry_error(
        status_code,
        error_code="UPSTREAM_ERROR",
        error_name="UpstreamError",
        parameters=_scoped_error_parameters(ontology=ontology, parameters=parameters),
    )


def _upstream_transport_error_response(
    *,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    return _foundry_error(
        status.HTTP_502_BAD_GATEWAY,
        error_code="UPSTREAM_ERROR",
        error_name="UpstreamError",
        parameters=_scoped_error_parameters(ontology=ontology, parameters=parameters),
    )


def _internal_error_response(
    *,
    log_message: str,
    exc: Exception,
    ontology: str,
    parameters: Dict[str, Any] | None = None,
) -> JSONResponse:
    logger.error("%s: %s", log_message, exc)
    return _foundry_error(
        status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_code="INTERNAL",
        error_name="Internal",
        parameters=_scoped_error_parameters(ontology=ontology, parameters=parameters),
    )
