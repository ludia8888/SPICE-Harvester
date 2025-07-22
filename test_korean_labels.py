#!/usr/bin/env python3
"""
Focused test for Korean label preservation
"""
import requests
import json

BASE_URL = "http://localhost:8000/api/v1"
DB_NAME = "korean_label_test"

def setup():
    """Setup test database"""
    try:
        requests.delete(f"{BASE_URL}/database/{DB_NAME}")
    except:
        pass
    
    response = requests.post(f"{BASE_URL}/database/create", json={"name": DB_NAME})
    print(f"Database creation: {response.status_code}")
    return response.status_code == 200

def test_relationship_labels():
    """Test Korean labels in relationships"""
    print("\n=== Korean Label Test ===")
    
    # Create reference class first
    category_data = {
        "id": "Category",
        "type": "Class",
        "label": {"en": "Category", "ko": "카테고리"},
        "properties": [
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Name", "ko": "이름"},
                "required": True
            }
        ]
    }
    
    print("Creating Category class...")
    response = requests.post(f"{BASE_URL}/ontology/{DB_NAME}/create", json=category_data)
    print(f"Category creation: {response.status_code}")
    if response.status_code != 200:
        print(f"Category error: {response.text}")
        return False
    
    # Create class with explicit relationship (not property conversion)
    product_data = {
        "id": "Product",
        "type": "Class",
        "label": {"en": "Product", "ko": "제품"},
        "properties": [
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
    response = requests.post(f"{BASE_URL}/ontology/{DB_NAME}/create", json=product_data)
    print(f"Product creation: {response.status_code}")
    
    if response.status_code != 200:
        print(f"Product error: {response.text}")
        return False
    
    # Retrieve and check
    print("Retrieving Product class...")
    response = requests.get(f"{BASE_URL}/ontology/{DB_NAME}/Product")
    
    if response.status_code == 200:
        data = response.json()
        if "data" in data:
            product = data["data"]
        else:
            product = data
            
        print(f"\nProduct structure:")
        print(json.dumps(product, indent=2, ensure_ascii=False))
        
        # Check relationships
        relationships = product.get("relationships", [])
        for rel in relationships:
            if rel.get("predicate") == "category":
                label = rel.get("label", {})
                print(f"\nCategory relationship label: {label}")
                if label.get("ko") == "카테고리":
                    print("✅ Korean label preserved!")
                    return True
                else:
                    print("❌ Korean label missing")
                    return False
        
        print("❌ Category relationship not found")
        return False
    else:
        print(f"Retrieval failed: {response.status_code}")
        return False

if __name__ == "__main__":
    if setup():
        success = test_relationship_labels()
        print(f"\nResult: {'SUCCESS' if success else 'FAILED'}")
    else:
        print("Setup failed")