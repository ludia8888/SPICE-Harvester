import pytest
from fastapi import FastAPI

from bff.routers import command_status as bff_command_status
from bff.routers import foundry_ontology_v2
from oms.routers import attachments as oms_attachments
from oms.routers import command_status as oms_command_status
from shared.routers import config_monitoring


def _response_codes(schema: dict, *, path: str, method: str) -> set[str]:
    operation = schema.get("paths", {}).get(path, {}).get(method, {})
    responses = operation.get("responses", {})
    return {str(code) for code in responses.keys()}


@pytest.mark.unit
def test_bff_command_status_openapi_includes_404_and_503():
    app = FastAPI()
    app.include_router(bff_command_status.router, prefix="/api/v1")
    schema = app.openapi()

    codes = _response_codes(schema, path="/api/v1/commands/{command_id}/status", method="get")
    assert {"200", "400", "404", "503"} <= codes


@pytest.mark.unit
def test_oms_command_status_openapi_includes_404_and_503():
    app = FastAPI()
    app.include_router(oms_command_status.router, prefix="/api/v1")
    schema = app.openapi()

    codes = _response_codes(schema, path="/api/v1/commands/{command_id}/status", method="get")
    assert {"200", "400", "404", "503"} <= codes


@pytest.mark.unit
def test_v2_attachments_upload_openapi_includes_400_for_bff_and_oms():
    bff_app = FastAPI()
    bff_app.include_router(foundry_ontology_v2.router, prefix="/api")
    bff_schema = bff_app.openapi()
    bff_codes = _response_codes(bff_schema, path="/api/v2/ontologies/attachments/upload", method="post")
    assert {"200", "400"} <= bff_codes

    oms_app = FastAPI()
    oms_app.include_router(oms_attachments.attachments_upload_router, prefix="/api")
    oms_schema = oms_app.openapi()
    oms_codes = _response_codes(oms_schema, path="/api/v2/ontologies/attachments/upload", method="post")
    assert {"200", "400"} <= oms_codes


@pytest.mark.unit
def test_config_drift_analysis_openapi_includes_400():
    app = FastAPI()
    app.include_router(config_monitoring.router, prefix="/api/v1/config")
    schema = app.openapi()

    codes = _response_codes(schema, path="/api/v1/config/config/drift-analysis", method="get")
    assert {"200", "400"} <= codes
