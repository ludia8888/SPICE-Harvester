#!/usr/bin/env python3
"""
Test model changes for English-only system
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

def _check_property_model():
    """Test Property model with simple strings"""
    print("🔍 Testing Property model...")
    
    from shared.models.ontology import Property
    
    try:
        # Test simple string labels
        prop = Property(
            name="category",
            type="Category", 
            label="Category",  # Simple string
            description="Product category",  # Simple string
            required=True,
            isRelationship=True,
            cardinality="n:1"
        )
        
        print(f"✅ Property created successfully:")
        print(f"   name: {prop.name}")
        print(f"   label: {prop.label} (type: {type(prop.label)})")
        print(f"   description: {prop.description} (type: {type(prop.description)})")
        print(f"   is_class_reference: {prop.is_class_reference()}")
        
        # Test conversion to relationship
        if prop.is_class_reference():
            rel_data = prop.to_relationship()
            print(f"✅ Converted to relationship:")
            print(f"   {rel_data}")
            return True
        else:
            print("❌ is_class_reference failed")
            return False
            
    except Exception as e:
        print(f"❌ Property model error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_property_model():
    """Test Property model with simple strings"""
    assert _check_property_model()

def _check_relationship_model():
    """Test Relationship model with simple strings"""
    print("\n🔍 Testing Relationship model...")
    
    from shared.models.ontology import Relationship
    
    try:
        rel = Relationship(
            predicate="category",
            target="Category",
            label="Category",  # Simple string
            description="Product category",  # Simple string
            cardinality="n:1"
        )
        
        print(f"✅ Relationship created successfully:")
        print(f"   predicate: {rel.predicate}")
        print(f"   label: {rel.label} (type: {type(rel.label)})")
        print(f"   description: {rel.description} (type: {type(rel.description)})")
        return True
        
    except Exception as e:
        print(f"❌ Relationship model error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_relationship_model():
    """Test Relationship model with simple strings"""
    assert _check_relationship_model()

def _check_ontology_request():
    """Test OntologyCreateRequest with simple strings"""
    print("\n🔍 Testing OntologyCreateRequest...")
    
    from shared.models.ontology import OntologyCreateRequest, Property
    
    try:
        # Create with simple string labels
        ontology = OntologyCreateRequest(
            id="Product",
            label="Product",  # Simple string
            description="A product in the catalog",  # Simple string
            properties=[
                Property(
                    name="name",
                    type="STRING",
                    label="Product Name",
                    required=True
                ),
                Property(
                    name="category",
                    type="Category",
                    label="Category",
                    isRelationship=True,
                    cardinality="n:1"
                )
            ]
        )
        
        print(f"✅ OntologyCreateRequest created successfully:")
        print(f"   id: {ontology.id}")
        print(f"   label: {ontology.label} (type: {type(ontology.label)})")
        print(f"   description: {ontology.description} (type: {type(ontology.description)})")
        print(f"   properties: {len(ontology.properties)}")
        return True
        
    except Exception as e:
        print(f"❌ OntologyCreateRequest error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_ontology_request():
    """Test OntologyCreateRequest with simple strings"""
    assert _check_ontology_request()

def _check_converter():
    """Test PropertyToRelationshipConverter with new models"""
    print("\n🔍 Testing PropertyToRelationshipConverter...")
    
    from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
    
    try:
        converter = PropertyToRelationshipConverter()
        
        # Test data with simple strings
        class_data = {
            "id": "Product",
            "type": "Class",
            "label": "Product",
            "properties": [
                {
                    "name": "name",
                    "type": "STRING",
                    "label": "Product Name",
                    "required": True
                },
                {
                    "name": "category",
                    "type": "Category",
                    "label": "Category",
                    "isRelationship": True,
                    "cardinality": "n:1"
                }
            ]
        }
        
        result = converter.process_class_data(class_data)
        
        print(f"✅ Converter working:")
        print(f"   properties: {len(result.get('properties', []))}")
        print(f"   relationships: {len(result.get('relationships', []))}")
        
        # Check relationship
        for rel in result.get('relationships', []):
            if rel.get('predicate') == 'category':
                print(f"   category label: {rel.get('label')} (type: {type(rel.get('label'))})")
                return True
        
        print("❌ Category relationship not found")
        return False
        
    except Exception as e:
        print(f"❌ Converter error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_converter():
    """Test PropertyToRelationshipConverter with new models"""
    assert _check_converter()

if __name__ == "__main__":
    print("🔥 THINK ULTRA! Model Changes Test")
    print("=" * 50)
    
    results = []
    results.append(_check_property_model())
    results.append(_check_relationship_model())
    results.append(_check_ontology_request())
    results.append(_check_converter())
    
    passed = sum(results)
    total = len(results)
    
    print(f"\n📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL MODEL TESTS PASSED! English-only models working!")
    else:
        print("❌ Some model tests failed")
