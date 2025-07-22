#!/usr/bin/env python3
"""
Debug the property → relationship conversion process
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from shared.models.ontology import Property
from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter

def test_conversion():
    """Test the property to relationship conversion"""
    print("🔍 Testing property → relationship conversion")
    
    # Create test property data matching the test case
    prop_data = {
        "name": "category",
        "type": "Category",
        "label": {"en": "Category", "ko": "카테고리"},
        "description": {"en": "Product category", "ko": "제품 카테고리"},
        "cardinality": "n:1",
        "isRelationship": True
    }
    
    print(f"📝 Input property data:")
    print(f"   {prop_data}")
    
    # Create Property object
    try:
        prop = Property(**prop_data)
        print(f"\n✅ Property object created successfully")
        print(f"   name: {prop.name}")
        print(f"   type: {prop.type}")
        print(f"   label: {prop.label}")
        print(f"   label type: {type(prop.label)}")
        if hasattr(prop.label, 'ko'):
            print(f"   label.ko: {prop.label.ko}")
            print(f"   label.en: {prop.label.en}")
        
        # Test is_class_reference
        is_ref = prop.is_class_reference()
        print(f"\n🔗 is_class_reference(): {is_ref}")
        
        # Test to_relationship conversion
        if is_ref:
            rel_data = prop.to_relationship()
            print(f"\n🔄 Converted to relationship:")
            print(f"   {rel_data}")
            
            # Check label in relationship
            rel_label = rel_data.get("label")
            print(f"\n🏷️ Relationship label:")
            print(f"   label: {rel_label}")
            print(f"   label type: {type(rel_label)}")
            if hasattr(rel_label, 'ko'):
                print(f"   label.ko: {rel_label.ko}")
                print(f"   label.en: {rel_label.en}")
            elif isinstance(rel_label, dict):
                print(f"   label dict: {rel_label}")
            
    except Exception as e:
        print(f"❌ Error creating Property: {e}")
        import traceback
        traceback.print_exc()

def test_converter():
    """Test the PropertyToRelationshipConverter"""
    print("\n" + "="*60)
    print("🔧 Testing PropertyToRelationshipConverter")
    
    converter = PropertyToRelationshipConverter()
    
    # Test class data with the property
    class_data = {
        "id": "Product",
        "type": "Class",
        "label": {"en": "Product", "ko": "제품"},
        "properties": [
            {
                "name": "name",
                "type": "STRING",
                "label": {"en": "Product Name", "ko": "제품명"},
                "required": True
            },
            {
                "name": "category",
                "type": "Category",
                "label": {"en": "Category", "ko": "카테고리"},
                "description": {"en": "Product category", "ko": "제품 카테고리"},
                "cardinality": "n:1",
                "isRelationship": True
            }
        ]
    }
    
    print(f"📝 Input class data:")
    print(f"   {len(class_data['properties'])} properties")
    
    # Process the class data
    try:
        processed_data = converter.process_class_data(class_data)
        print(f"\n✅ Conversion completed")
        print(f"   {len(processed_data.get('properties', []))} properties")
        print(f"   {len(processed_data.get('relationships', []))} relationships")
        
        # Check the relationships
        for rel in processed_data.get('relationships', []):
            print(f"\n🔗 Relationship:")
            print(f"   predicate: {rel.get('predicate')}")
            print(f"   target: {rel.get('target')}")
            print(f"   label: {rel.get('label')}")
            print(f"   label type: {type(rel.get('label'))}")
            
    except Exception as e:
        print(f"❌ Error in converter: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_conversion()
    test_converter()
    print("\n🔚 Debug complete")