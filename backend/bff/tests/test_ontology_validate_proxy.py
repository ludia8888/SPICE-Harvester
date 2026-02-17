from fastapi.testclient import TestClient

from bff.main import app


def test_ontology_validate_endpoints_removed():
    client = TestClient(app)
    create_validate = client.post(
        "/api/v1/databases/demo_db/ontology/validate",
        params={"branch": "main"},
        json={
            "id": "Product",
            "label": "Product",
            "properties": [{"name": "product_id", "type": "xsd:string", "label": "Product ID"}],
        },
    )
    update_validate = client.post(
        "/api/v1/databases/demo_db/ontology/Product/validate",
        params={"branch": "main"},
        json={"label": "Product v2"},
    )

    assert create_validate.status_code == 404
    assert update_validate.status_code == 404
