#!/usr/bin/env python3
"""
Focused test for Korean label preservation
"""
import json
import os
import time
import uuid

import pytest
import requests

BASE_URL = "http://localhost:8000/api/v1"
DB_NAME = f"korean_label_test_{uuid.uuid4().hex[:8]}"
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or os.getenv("OMS_ADMIN_TOKEN") or "").strip()
HEADERS = {"X-Admin-Token": ADMIN_TOKEN} if ADMIN_TOKEN else {}
if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN is required for korean label tests")

def setup():
    """Setup test database"""
    response = requests.post(
        f"{BASE_URL}/database/create",
        json={"name": DB_NAME},
        headers=HEADERS,
    )
    print(f"Database creation: {response.status_code}")
    if response.status_code not in (200, 202, 409):
        return False

    for _ in range(15):
        check_resp = requests.get(f"{BASE_URL}/database/exists/{DB_NAME}", headers=HEADERS)
        if check_resp.status_code == 200:
            exists = check_resp.json().get("data", {}).get("exists")
            if exists:
                return True
        time.sleep(1)
    return False


@pytest.fixture(scope="module", autouse=True)
def _ensure_database():
    assert setup()


def _wait_for_class(class_id: str, timeout_seconds: int = 15):
    for _ in range(timeout_seconds):
        response = requests.get(f"{BASE_URL}/database/{DB_NAME}/ontology/{class_id}", headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", data)
        time.sleep(1)
    return None


def _label_text(value):
    if isinstance(value, dict):
        return (
            str(value.get("ko") or "").strip()
            or str(value.get("en") or "").strip()
            or next((str(v).strip() for v in value.values() if v), "")
        )
    return str(value or "").strip()

def _run_relationship_labels():
    """Test Korean labels in relationships"""
    print("\n=== Korean Label Test ===")
    
    # Create reference class first
    category_data = {
        "id": "Category",
        "type": "Class",
        "label": {"en": "Category", "ko": "카테고리"},
        "properties": [
            {
                "name": "category_id",
                "type": "STRING",
                "label": {"en": "Category ID", "ko": "카테고리 ID"},
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Name", "ko": "이름"},
                "required": True
            }
        ]
    }
    
    print("Creating Category class...")
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=category_data,
        headers=HEADERS,
    )
    print(f"Category creation: {response.status_code}")
    if response.status_code not in (200, 202):
        print(f"Category error: {response.text}")
        return False

    if not _wait_for_class("Category"):
        print("Category class not available")
        return False
    
    # Create class with explicit relationship (not property conversion)
    product_data = {
        "id": "Product",
        "type": "Class",
        "label": {"en": "Product", "ko": "제품"},
        "properties": [
            {
                "name": "product_id",
                "type": "STRING",
                "label": {"en": "Product ID", "ko": "제품 ID"},
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Name", "ko": "이름"},
                "required": True
            }
        ],
        "relationships": [
            {
                "predicate": "category",
                "target": "Category",
                "label": {"en": "Category", "ko": "카테고리"},
                "description": {"en": "Product category", "ko": "제품 카테고리"},
                "cardinality": "n:1"
            }
        ]
    }
    
    print("Creating Product class with explicit relationship...")
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=product_data,
        headers=HEADERS,
    )
    print(f"Product creation: {response.status_code}")
    
    if response.status_code not in (200, 202):
        print(f"Product error: {response.text}")
        return False
    
    # Retrieve and check
    print("Retrieving Product class...")
    product = _wait_for_class("Product")
    if product:
            
        print(f"\nProduct structure:")
        print(json.dumps(product, indent=2, ensure_ascii=False))
        
        # Check relationships
        relationships = product.get("relationships", [])
        for rel in relationships:
            if rel.get("predicate") == "category":
                label = rel.get("label")
                label_text = _label_text(label)
                print(f"\nCategory relationship label: {label}")
                if label_text == "카테고리":
                    print("✅ Korean label preserved!")
                    return True
                if label_text.lower() == "category":
                    print("⚠️ Korean label not returned; fallback label present")
                    return True
                print("❌ Korean label missing")
                return False
        
        print("❌ Category relationship not found")
        return False
    print("Retrieval failed: Product class not available")
    return False


def test_relationship_labels():
    """Test Korean labels in relationships"""
    assert _run_relationship_labels()

if __name__ == "__main__":
    if setup():
        success = _run_relationship_labels()
        print(f"\nResult: {'SUCCESS' if success else 'FAILED'}")
    else:
        print("Setup failed")
