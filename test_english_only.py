#!/usr/bin/env python3
"""
🔥 THINK ULTRA! English-only label test
"""
import requests
import json

BASE_URL = "http://localhost:8000/api/v1"
DB_NAME = "english_only_test"

def setup():
    """Setup test database"""
    try:
        requests.delete(f"{BASE_URL}/database/{DB_NAME}")
    except:
        pass
    
    response = requests.post(f"{BASE_URL}/database/create", json={"name": DB_NAME})
    print(f"Database creation: {response.status_code}")
    return response.status_code == 200

def test_english_labels():
    """Test English-only labels"""
    print("\n=== English-Only Label Test ===")
    
    # Create reference class first
    category_data = {
        "id": "Category",
        "type": "Class", 
        "label": "Category",  # Simple string
        "properties": [
            {
                "name": "name",
                "type": "STRING",
                "label": "Name",  # Simple string
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
    
    # Create class with Property → Relationship conversion
    product_data = {
        "id": "Product",
        "type": "Class",
        "label": "Product",  # Simple string
        "description": "A product in the catalog",  # Simple string
        "properties": [
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
        
        # Verification
        print(f"\n=== Verification ===")
        
        # Check class-level labels
        class_label = product.get("label")
        if class_label == "Product":
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
                rel_label = rel.get("label")
                if rel_label == "Category":
                    print("✅ Relationship label: Simple string working!")
                    return True
                else:
                    print(f"❌ Relationship label issue: {rel_label}")
                    return False
        
        print("❌ Category relationship not found")
        return False
    else:
        print(f"Retrieval failed: {response.status_code}")
        return False

def test_explicit_relationships():
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
                "required": True
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
                "name": "name",
                "type": "STRING",
                "label": "Customer Name",
                "required": True
            }
        ]
    }
    
    print("Creating Customer class...")
    response = requests.post(f"{BASE_URL}/ontology/{DB_NAME}/create", json=customer_data)
    print(f"Customer creation: {response.status_code}")
    
    print("Creating Order class with explicit relationship...")
    response = requests.post(f"{BASE_URL}/ontology/{DB_NAME}/create", json=order_data)
    print(f"Order creation: {response.status_code}")
    
    if response.status_code != 200:
        print(f"Order error: {response.text}")
        return False
    
    # Check Order
    response = requests.get(f"{BASE_URL}/ontology/{DB_NAME}/Order")
    if response.status_code == 200:
        data = response.json()
        order = data.get("data", data)
        
        relationships = order.get("relationships", [])
        for rel in relationships:
            if rel.get("predicate") == "customer":
                rel_label = rel.get("label")
                if rel_label == "Customer":
                    print("✅ Explicit relationship English label working!")
                    return True
                else:
                    print(f"❌ Explicit relationship label issue: {rel_label}")
        
        print("❌ Customer relationship not found")
        return False
    else:
        print(f"Order retrieval failed: {response.status_code}")
        return False

if __name__ == "__main__":
    print("🔥 THINK ULTRA! English-Only System Test")
    print("=" * 60)
    
    if setup():
        print("✅ Database setup complete")
        
        # Test property conversion
        success1 = test_english_labels()
        
        # Test explicit relationships  
        success2 = test_explicit_relationships()
        
        if success1 and success2:
            print("\n🎉 ALL TESTS PASSED! English-only system working perfectly!")
        else:
            print(f"\n❌ Tests failed - conversion: {success1}, explicit: {success2}")
            
    else:
        print("❌ Database setup failed")