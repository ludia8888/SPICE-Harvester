#!/usr/bin/env python3
"""
Test enum validation edge cases to find potential bugs
"""

from shared.models.common import DataType
from shared.validators.enum_validator import EnumValidator
from shared.validators.complex_type_validator import ComplexTypeValidator, ComplexTypeConstraints


def test_all_invalid_cases():
    """Test various invalid cases that should fail validation"""
    print("🔍 Testing cases that SHOULD fail validation...")
    
    validator = EnumValidator()
    
    # Valid constraints for reference
    valid_constraints = {"enum": ["active", "inactive", "pending"]}
    
    test_cases = [
        # Test case: (value, constraints, description)
        ("invalid", valid_constraints, "Simple invalid string"),
        ("ACTIVE", valid_constraints, "Wrong case"),
        (" active", valid_constraints, "Leading space"),
        ("active ", valid_constraints, "Trailing space"),
        ("", valid_constraints, "Empty string"),
        (None, valid_constraints, "None value"),
        (123, valid_constraints, "Number instead of string"),
        ([], valid_constraints, "Empty list"),
        ({}, valid_constraints, "Empty dict"),
        ("active", {"enum": []}, "Empty enum list"),
        ("active", {}, "Missing enum key"),
        ("active", None, "None constraints"),
        (True, {"enum": ["true", "false"]}, "Boolean vs string"),
        (1, {"enum": ["1", "2", "3"]}, "Number vs string"),
        ("1.0", {"enum": [1.0, 2.0, 3.0]}, "String vs float"),
    ]
    
    failed_validations = []
    
    for i, (value, constraints, description) in enumerate(test_cases, 1):
        try:
            result = validator.validate(value, constraints)
            if result.is_valid:
                failed_validations.append(f"Test {i}: {description} - INCORRECTLY PASSED")
                print(f"❌ Test {i}: {description} - Value '{value}' INCORRECTLY PASSED validation")
            else:
                print(f"✅ Test {i}: {description} - Correctly rejected")
        except Exception as e:
            print(f"⚠️ Test {i}: {description} - Exception: {e}")
    
    if failed_validations:
        print(f"\n🚨 BUGS FOUND: {len(failed_validations)} invalid values incorrectly passed validation:")
        for bug in failed_validations:
            print(f"   {bug}")
    else:
        print(f"\n✅ All {len(test_cases)} invalid cases were correctly rejected")
    
    return len(failed_validations) == 0


def test_all_valid_cases():
    """Test various valid cases that should pass validation"""
    print("\n🔍 Testing cases that SHOULD pass validation...")
    
    validator = EnumValidator()
    
    test_cases = [
        # Test case: (value, constraints, description)
        ("active", {"enum": ["active", "inactive"]}, "Simple valid string"),
        (1, {"enum": [1, 2, 3]}, "Valid integer"),
        (1.5, {"enum": [1.5, 2.5, 3.5]}, "Valid float"),
        (True, {"enum": [True, False]}, "Valid boolean"),
        (None, {"enum": [None, "null"]}, "Valid None"),
        ("", {"enum": ["", "empty"]}, "Valid empty string"),
        ("café", {"enum": ["café", "naïve"]}, "Valid Unicode"),
        ([1, 2], {"enum": [[1, 2], [3, 4]]}, "Valid list"),
        ({"a": 1}, {"enum": [{"a": 1}, {"b": 2}]}, "Valid dict"),
    ]
    
    failed_validations = []
    
    for i, (value, constraints, description) in enumerate(test_cases, 1):
        try:
            result = validator.validate(value, constraints)
            if not result.is_valid:
                failed_validations.append(f"Test {i}: {description} - INCORRECTLY REJECTED")
                print(f"❌ Test {i}: {description} - Value '{value}' INCORRECTLY REJECTED: {result.message}")
            else:
                print(f"✅ Test {i}: {description} - Correctly accepted")
        except Exception as e:
            print(f"⚠️ Test {i}: {description} - Exception: {e}")
    
    if failed_validations:
        print(f"\n🚨 BUGS FOUND: {len(failed_validations)} valid values incorrectly rejected:")
        for bug in failed_validations:
            print(f"   {bug}")
    else:
        print(f"\n✅ All {len(test_cases)} valid cases were correctly accepted")
    
    return len(failed_validations) == 0


def test_complex_type_validator_routing():
    """Test that ComplexTypeValidator correctly routes to EnumValidator"""
    print("\n🔍 Testing ComplexTypeValidator routing...")
    
    # Test direct vs ComplexTypeValidator validation
    enum_validator = EnumValidator()
    constraints = {"enum": ["red", "green", "blue"]}
    
    # Direct validation
    direct_result = enum_validator.validate("yellow", constraints)
    
    # ComplexTypeValidator validation
    complex_constraints = ComplexTypeConstraints.enum_constraints(["red", "green", "blue"])
    complex_valid, complex_msg, complex_normalized = ComplexTypeValidator.validate(
        "yellow", DataType.ENUM.value, complex_constraints
    )
    
    print(f"Direct EnumValidator: valid={direct_result.is_valid}, message='{direct_result.message}'")
    print(f"ComplexTypeValidator: valid={complex_valid}, message='{complex_msg}'")
    
    # They should give the same result
    if direct_result.is_valid == complex_valid and direct_result.message == complex_msg:
        print("✅ ComplexTypeValidator correctly routes to EnumValidator")
        return True
    else:
        print("❌ BUG: ComplexTypeValidator gives different result than direct EnumValidator")
        return False


def test_constraint_creation():
    """Test constraint creation methods"""
    print("\n🔍 Testing constraint creation...")
    
    # Test ComplexTypeConstraints.enum_constraints
    values = ["a", "b", "c"]
    constraints1 = ComplexTypeConstraints.enum_constraints(values)
    print(f"ComplexTypeConstraints.enum_constraints({values}) = {constraints1}")
    
    # Test EnumValidator.create_constraints
    constraints2 = EnumValidator.create_constraints(values)
    print(f"EnumValidator.create_constraints({values}) = {constraints2}")
    
    # They should be the same
    if constraints1 == constraints2:
        print("✅ Both constraint creation methods produce identical results")
        return True
    else:
        print("❌ BUG: Different constraint creation methods produce different results")
        return False


def test_potential_bypass_scenarios():
    """Test scenarios where enum validation might be bypassed"""
    print("\n🔍 Testing potential bypass scenarios...")
    
    bypass_found = False
    
    # Test 1: What if constraints is not a dict?
    try:
        validator = EnumValidator()
        result = validator.validate("test", "not_a_dict")
        if result.is_valid:
            print("❌ BUG: Non-dict constraints bypassed validation")
            bypass_found = True
        else:
            print("✅ Non-dict constraints properly handled")
    except Exception as e:
        print(f"✅ Non-dict constraints raised exception: {e}")
    
    # Test 2: What if enum key has wrong type?
    try:
        validator = EnumValidator()
        result = validator.validate("test", {"enum": "not_a_list"})
        if result.is_valid:
            print("❌ BUG: Non-list enum value bypassed validation")
            bypass_found = True
        else:
            print("✅ Non-list enum value properly handled")
    except Exception as e:
        print(f"✅ Non-list enum value raised exception: {e}")
    
    # Test 3: What if allowed_values is not iterable?
    try:
        validator = EnumValidator()
        # Manually create constraints with non-iterable enum
        constraints = {"enum": 123}  # int instead of list
        result = validator.validate("test", constraints)
        if result.is_valid:
            print("❌ BUG: Non-iterable enum bypassed validation")
            bypass_found = True
        else:
            print("✅ Non-iterable enum properly handled")
    except Exception as e:
        print(f"✅ Non-iterable enum raised exception: {e}")
    
    return not bypass_found


def main():
    """Run all tests"""
    print("🚀 Comprehensive Enum Validation Bug Detection")
    print("=" * 60)
    
    all_passed = True
    
    try:
        # Test invalid cases
        all_passed &= test_all_invalid_cases()
        
        # Test valid cases
        all_passed &= test_all_valid_cases()
        
        # Test routing
        all_passed &= test_complex_type_validator_routing()
        
        # Test constraint creation
        all_passed &= test_constraint_creation()
        
        # Test bypass scenarios
        all_passed &= test_potential_bypass_scenarios()
        
        print("\n" + "=" * 60)
        if all_passed:
            print("🎉 NO BUGS FOUND: Enum validation appears to be working correctly!")
            print("   All invalid values are properly rejected")
            print("   All valid values are properly accepted")
            print("   ComplexTypeValidator correctly routes to EnumValidator")
            print("   No bypass scenarios found")
        else:
            print("🚨 BUGS DETECTED: Issues found in enum validation logic!")
            print("   Please review the test output above for details")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()