#!/usr/bin/env python3
"""
🔥 THINK ULTRA! English-only label test
"""
import json
import os
import time
import uuid

import pytest
import requests

BASE_URL = "http://localhost:8000/api/v1"
DB_NAME = f"english_only_test_{uuid.uuid4().hex[:8]}"
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or os.getenv("OMS_ADMIN_TOKEN") or "").strip()
HEADERS = {"X-Admin-Token": ADMIN_TOKEN} if ADMIN_TOKEN else {}
if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN is required for english-only tests")

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
            str(value.get("en") or "").strip()
            or str(value.get("ko") or "").strip()
            or next((str(v).strip() for v in value.values() if v), "")
        )
    return str(value or "").strip()

def _run_english_labels():
    """Test English-only labels"""
    print("\n=== English-Only Label Test ===")
    
    # Create reference class first
    category_data = {
        "id": "Category",
        "type": "Class", 
        "label": "Category",  # Simple string
        "properties": [
            {
                "name": "category_id",
                "type": "STRING",
                "label": "Category ID",
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "name",
                "type": "STRING",
                "label": "Name",  # Simple string
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
    
    # Create class with Property → Relationship conversion
    product_data = {
        "id": "Product",
        "type": "Class",
        "label": "Product",  # Simple string
        "description": "A product in the catalog",  # Simple string
        "properties": [
            {
                "name": "product_id",
                "type": "STRING",
                "label": "Product ID",
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "name",
                "type": "STRING",
                "label": "Product Name",  # Simple string
                "description": "The name of the product",  # Simple string
                "required": True
            },
            # This should be converted to relationship
            {
                "name": "category",
                "type": "Category",  # Class reference
                "label": "Category",  # Simple string
                "description": "Product category",  # Simple string
                "cardinality": "n:1",
                "isRelationship": True
            }
        ]
    }
    
    print("Creating Product class with property conversion...")
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
        
        # Verification
        print(f"\n=== Verification ===")
        
        # Check class-level labels
        class_label = _label_text(product.get("label"))
        if class_label.lower() == "product":
            print("✅ Class label: Simple string working")
        else:
            print(f"❌ Class label issue: {class_label}")
        
        # Check property conversion
        properties = product.get("properties", [])
        relationships = product.get("relationships", [])
        
        category_in_props = any(p.get("name") == "category" for p in properties)
        category_in_rels = any(r.get("predicate") == "category" for r in relationships)
        
        if not category_in_props and category_in_rels:
            print("✅ Property → Relationship conversion working")
        else:
            print(f"❌ Conversion issue - props: {category_in_props}, rels: {category_in_rels}")
        
        # Check relationship labels
        for rel in relationships:
            if rel.get("predicate") == "category":
                rel_label = _label_text(rel.get("label"))
                if rel_label.lower() == "category":
                    print("✅ Relationship label: Simple string working!")
                    return True
                else:
                    print(f"❌ Relationship label issue: {rel_label}")
                    return False
        
        print("❌ Category relationship not found")
        return False
    print("Retrieval failed: Product class not available")
    return False


def test_english_labels():
    """Test English-only labels"""
    assert _run_english_labels()


def _run_explicit_relationships():
    """Test explicit relationships with English labels"""
    print("\n=== Explicit Relationship Test ===")
    
    # Create class with explicit relationships
    order_data = {
        "id": "Order",
        "type": "Class",
        "label": "Order",
        "description": "Purchase order",
        "properties": [
            {
                "name": "order_id",
                "type": "STRING",
                "label": "Order ID",
                "required": True,
                "primaryKey": True,
                "titleKey": True,
            }
        ],
        "relationships": [
            {
                "predicate": "customer",
                "target": "Customer",
                "label": "Customer",  # Simple string
                "description": "The customer who placed the order",  # Simple string
                "cardinality": "n:1"
            }
        ]
    }
    
    # Create Customer first
    customer_data = {
        "id": "Customer",
        "type": "Class",
        "label": "Customer",
        "properties": [
            {
                "name": "customer_id",
                "type": "STRING",
                "label": "Customer ID",
                "required": True,
                "primaryKey": True,
            },
            {
                "name": "name",
                "type": "STRING",
                "label": "Customer Name",
                "required": True,
                "titleKey": True,
            }
        ]
    }
    
    print("Creating Customer class...")
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=customer_data,
        headers=HEADERS,
    )
    print(f"Customer creation: {response.status_code}")
    if response.status_code not in (200, 202):
        print(f"Customer error: {response.text}")
        return False
    if not _wait_for_class("Customer"):
        print("Customer class not available")
        return False
    
    print("Creating Order class with explicit relationship...")
    response = requests.post(
        f"{BASE_URL}/database/{DB_NAME}/ontology",
        json=order_data,
        headers=HEADERS,
    )
    print(f"Order creation: {response.status_code}")
    
    if response.status_code not in (200, 202):
        print(f"Order error: {response.text}")
        return False
    
    # Check Order
    order = _wait_for_class("Order")
    if order:
        
        relationships = order.get("relationships", [])
        for rel in relationships:
            if rel.get("predicate") == "customer":
                rel_label = _label_text(rel.get("label"))
                if rel_label.lower() == "customer":
                    print("✅ Explicit relationship English label working!")
                    return True
                else:
                    print(f"❌ Explicit relationship label issue: {rel_label}")
        
        print("❌ Customer relationship not found")
        return False
    print("Order retrieval failed: Order class not available")
    return False


def test_explicit_relationships():
    """Test explicit relationships with English labels"""
    assert _run_explicit_relationships()

if __name__ == "__main__":
    print("🔥 THINK ULTRA! English-Only System Test")
    print("=" * 60)
    
    if setup():
        print("✅ Database setup complete")
        
        # Test property conversion
        success1 = _run_english_labels()
        
        # Test explicit relationships  
        success2 = _run_explicit_relationships()
        
        if success1 and success2:
            print("\n🎉 ALL TESTS PASSED! English-only system working perfectly!")
        else:
            print(f"\n❌ Tests failed - conversion: {success1}, explicit: {success2}")
            
    else:
        print("❌ Database setup failed")
