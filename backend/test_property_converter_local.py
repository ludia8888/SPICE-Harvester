#!/usr/bin/env python3
"""
PropertyToRelationshipConverter 로컬 테스트 - 정확한 동작 검증
"""

import sys
import os
import json

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from oms.services.property_to_relationship_converter import PropertyToRelationshipConverter
from shared.models.ontology import Property

def test_property_converter():
    """PropertyToRelationshipConverter 로컬 테스트"""
    
    print("🧪 Testing PropertyToRelationshipConverter locally")
    print("=" * 60)
    
    # Initialize converter
    converter = PropertyToRelationshipConverter()
    
    # Test data - 정확히 BFF에서 보내는 데이터 형식
    team_data = {
        "id": "Team",
        "label": "Team",
        "properties": [
            {"name": "name", "type": "string", "required": True, "label": "Team Name"},
            {
                "name": "leader",
                "type": "link",
                "target": "Employee",
                "required": True,
                "label": "Team Leader"
            },
            {
                "name": "members",
                "type": "array",
                "items": {"type": "link", "target": "Employee"},
                "label": "Members"
            }
        ]
    }
    
    print("📝 Input data:")
    print(json.dumps(team_data, indent=2))
    
    print("\n🔍 Testing individual properties:")
    
    # Test each property individually
    for i, prop_data in enumerate(team_data["properties"]):
        print(f"\n--- Property {i+1}: {prop_data['name']} ---")
        print(f"Raw data: {prop_data}")
        
        # Create Property object
        try:
            prop = Property(**prop_data)
            print(f"✅ Property object created: name={prop.name}, type={prop.type}")
            print(f"   target={prop.target}, linkTarget={prop.linkTarget}")
            
            # Test is_class_reference
            is_ref = prop.is_class_reference()
            print(f"🔍 is_class_reference(): {is_ref}")
            
            if is_ref:
                try:
                    relationship = prop.to_relationship()
                    print(f"🔗 to_relationship(): {relationship}")
                except Exception as e:
                    print(f"❌ to_relationship() failed: {e}")
            else:
                print("⚪ Not a class reference - remains as property")
                
        except Exception as e:
            print(f"❌ Property creation failed: {e}")
    
    print("\n🔄 Testing full conversion process:")
    print("-" * 40)
    
    # Test full conversion
    try:
        result = converter.process_class_data(team_data)
        
        print("📊 Conversion result:")
        print(f"Properties: {len(result.get('properties', []))}")
        for prop in result.get('properties', []):
            print(f"  - {prop.get('name')}: {prop.get('type')}")
            
        print(f"Relationships: {len(result.get('relationships', []))}")
        for rel in result.get('relationships', []):
            print(f"  - {rel.get('predicate')}: -> {rel.get('target')}")
            
        print("\n📄 Full result:")
        print(json.dumps(result, indent=2))
        
        # Verify expectations
        property_names = {prop.get("name") for prop in result.get("properties", [])}
        relationship_predicates = {rel.get("predicate") for rel in result.get("relationships", [])}
        
        print("\n🎯 Verification:")
        print(f"Properties: {property_names}")
        print(f"Relationships: {relationship_predicates}")
        
        # Check results
        success = True
        if "leader" in property_names:
            print("❌ ISSUE: 'leader' should be converted to relationship")
            success = False
        if "members" in property_names:
            print("❌ ISSUE: 'members' should be converted to relationship")
            success = False
        if "leader" not in relationship_predicates:
            print("❌ ISSUE: 'leader' relationship not found")
            success = False
        if "members" not in relationship_predicates:
            print("❌ ISSUE: 'members' relationship not found")
            success = False
            
        if success:
            print("✅ SUCCESS: All conversions worked correctly!")
        else:
            print("❌ CONVERSION ISSUES DETECTED!")
            
    except Exception as e:
        print(f"❌ Full conversion failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_property_converter()