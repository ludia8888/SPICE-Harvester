#!/usr/bin/env python3
"""
🔥 ULTRA! Validated test for TerminusDB complex data types
Tests types that are confirmed to work with current implementation
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ValidatedComplexTypesTest:
    def __init__(self):
        # Use proper connection configuration
        self.connection_config = ConnectionConfig(
            server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
            user=os.getenv("TERMINUS_USER", "admin"),
            account=os.getenv("TERMINUS_ACCOUNT", "admin"),
            key=os.getenv("TERMINUS_KEY", "admin123"),
        )
        self.terminus = AsyncTerminusService(self.connection_config)
        self.test_db = f"test_validated_types_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.created_classes = []
        
    async def setUp(self):
        """Set up test environment"""
        logger.info("📦 Setting up test environment...")
        await self.terminus.connect()
        await self.terminus.create_database(self.test_db, "Validated Complex Types Test Database")
        
    async def tearDown(self):
        """Clean up test environment"""
        logger.info("🧹 Cleaning up test environment...")
        try:
            await self.terminus.delete_database(self.test_db)
        except:
            pass
        await self.terminus.disconnect()
        
    async def test_validated_complex_types(self):
        """Test validated complex data types"""
        logger.info("\n" + "="*80)
        logger.info("🔥 TESTING VALIDATED COMPLEX DATA TYPES")
        logger.info("="*80 + "\n")
        
        # Define class with validated complex types
        complex_class = {
            "id": "ValidatedComplexTypes",
            "label": "Validated Complex Types Test",
            "description": "Testing validated TerminusDB data types",
            "properties": [
                # 1. Basic String
                {
                    "name": "type1_string",
                    "type": "string",
                    "required": True,
                    "label": "1. Basic String",
                    "constraints": {"min_length": 1, "max_length": 100}
                },
                # 2. Integer
                {
                    "name": "type2_integer",
                    "type": "integer",
                    "required": True,
                    "label": "2. Integer",
                    "constraints": {"min": 0, "max": 1000}
                },
                # 3. Boolean
                {
                    "name": "type3_boolean",
                    "type": "boolean",
                    "required": True,
                    "label": "3. Boolean"
                },
                # 4. Decimal
                {
                    "name": "type4_decimal",
                    "type": "decimal",
                    "required": False,
                    "label": "4. Decimal Number"
                },
                # 5. Float
                {
                    "name": "type5_float",
                    "type": "float",
                    "required": False,
                    "label": "5. Float Number"
                },
                # 6. Double
                {
                    "name": "type6_double",
                    "type": "double",
                    "required": False,
                    "label": "6. Double Precision"
                },
                # 7. Date
                {
                    "name": "type7_date",
                    "type": "date",
                    "required": False,
                    "label": "7. Date Only"
                },
                # 8. DateTime
                {
                    "name": "type8_datetime",
                    "type": "datetime",
                    "required": False,
                    "label": "8. Date and Time"
                },
                # 9. Enum (string with enum constraint)
                {
                    "name": "type9_enum",
                    "type": "string",
                    "required": True,
                    "label": "9. Enum Type",
                    "constraints": {
                        "enum": ["option1", "option2", "option3", "option4"]
                    }
                },
                # 10. List<String>
                {
                    "name": "type10_list_string",
                    "type": "list<string>",
                    "required": False,
                    "label": "10. List of Strings"
                },
                # 11. Set<Integer>
                {
                    "name": "type11_set_integer",
                    "type": "set<integer>",
                    "required": False,
                    "label": "11. Set of Integers"
                },
                # 12. Text (long string)
                {
                    "name": "type12_text",
                    "type": "text",
                    "required": False,
                    "label": "12. Long Text"
                },
                # 13. URL (string with URL validation)
                {
                    "name": "type13_url",
                    "type": "string",
                    "required": False,
                    "label": "13. URL",
                    "constraints": {
                        "pattern": "^https?://[\\w.-]+(?:\\.[\\w\\.-]+)+[\\w\\-\\._~:/?#[\\]@!\\$&'\\(\\)\\*\\+,;=.]+$"
                    }
                },
                # 14. Email
                {
                    "name": "type14_email",
                    "type": "string",
                    "required": False,
                    "label": "14. Email Address",
                    "constraints": {
                        "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                    }
                },
                # 15. Optional Integer (explicitly optional)
                {
                    "name": "type15_optional_int",
                    "type": "integer",
                    "required": False,
                    "label": "15. Optional Integer"
                },
                # 16. BigInt (as string)
                {
                    "name": "type16_bigint",
                    "type": "string",
                    "required": False,
                    "label": "16. Big Integer (as string)",
                    "constraints": {
                        "pattern": "^-?[0-9]+$"
                    }
                },
                # 17. Percentage (decimal with constraints)
                {
                    "name": "type17_percentage",
                    "type": "decimal",
                    "required": False,
                    "label": "17. Percentage",
                    "constraints": {
                        "min": 0,
                        "max": 100
                    }
                },
                # 18. Currency (decimal with precision)
                {
                    "name": "type18_currency",
                    "type": "decimal",
                    "required": False,
                    "label": "18. Currency Amount",
                    "constraints": {
                        "precision": 10,
                        "scale": 2
                    }
                },
                # 19. Phone Number (string with pattern)
                {
                    "name": "type19_phone",
                    "type": "string",
                    "required": False,
                    "label": "19. Phone Number",
                    "constraints": {
                        "pattern": "^\\+?[1-9]\\d{1,14}$"
                    }
                }
            ],
            "relationships": []
        }
        
        try:
            # Create the class
            logger.info("📤 Creating class with validated complex types...")
            result = await self.terminus.create_ontology_class(self.test_db, complex_class)
            self.created_classes.append("ValidatedComplexTypes")
            logger.info("✅ Class created successfully!")
            
            # Retrieve and verify the class
            logger.info("\n🔍 Retrieving created class...")
            retrieved = await self.terminus.get_ontology(self.test_db, "ValidatedComplexTypes")
            
            # Verify all types
            logger.info("\n📊 Verifying data types:")
            properties = retrieved.get("properties", [])
            properties_dict = {p["name"]: p for p in properties}
            
            type_checks = [
                ("type1_string", "STRING", "Basic String"),
                ("type2_integer", "INTEGER", "Integer"),
                ("type3_boolean", "BOOLEAN", "Boolean"),
                ("type4_decimal", "DECIMAL", "Decimal"),
                ("type5_float", "FLOAT", "Float"),
                ("type6_double", "DOUBLE", "Double"),
                ("type7_date", "DATE", "Date"),
                ("type8_datetime", "DATETIME", "DateTime"),
                ("type9_enum", "STRING", "Enum (with constraint)"),
                ("type10_list_string", "LIST", "List<String>"),
                ("type11_set_integer", "SET", "Set<Integer>"),
                ("type12_text", "STRING", "Text (Long String)"),
                ("type13_url", "STRING", "URL (with pattern)"),
                ("type14_email", "STRING", "Email (with pattern)"),
                ("type15_optional_int", "INTEGER", "Optional Integer"),
                ("type16_bigint", "STRING", "BigInt (as string)"),
                ("type17_percentage", "DECIMAL", "Percentage"),
                ("type18_currency", "DECIMAL", "Currency"),
                ("type19_phone", "STRING", "Phone Number")
            ]
            
            passed = 0
            failed = 0
            
            for prop_name, expected_type, description in type_checks:
                if prop_name in properties_dict:
                    actual_type = properties_dict[prop_name].get("type", "UNKNOWN")
                    # Normalize type names for comparison
                    actual_normalized = actual_type.upper()
                    expected_normalized = expected_type.upper()
                    
                    # Handle special cases
                    if (actual_normalized == expected_normalized or
                        (expected_normalized == "LIST" and "LIST" in actual_normalized) or
                        (expected_normalized == "SET" and "SET" in actual_normalized)):
                        logger.info(f"✅ {description}: {actual_type}")
                        passed += 1
                    else:
                        logger.error(f"❌ {description}: Expected {expected_type}, got {actual_type}")
                        failed += 1
                else:
                    logger.error(f"❌ {description}: Property '{prop_name}' not found!")
                    failed += 1
            
            # Check constraints
            logger.info("\n🔧 Checking key constraints:")
            
            # Check enum constraint
            if "type9_enum" in properties_dict:
                enum_constraint = properties_dict["type9_enum"].get("constraints", {}).get("enum")
                if enum_constraint:
                    logger.info(f"✅ Enum constraint preserved: {enum_constraint}")
                else:
                    logger.warning("⚠️ Enum constraint not preserved in metadata")
                    
            # Check email pattern
            if "type14_email" in properties_dict:
                email_pattern = properties_dict["type14_email"].get("constraints", {}).get("pattern")
                if email_pattern:
                    logger.info(f"✅ Email pattern constraint preserved")
                else:
                    logger.warning("⚠️ Email pattern constraint not preserved")
                    
            # Check numeric constraints
            if "type2_integer" in properties_dict:
                int_constraints = properties_dict["type2_integer"].get("constraints", {})
                if "min_value" in int_constraints or "max_value" in int_constraints:
                    logger.info(f"✅ Integer range constraints preserved")
                else:
                    logger.warning("⚠️ Integer range constraints not preserved")
            
            # Summary
            logger.info("\n" + "="*60)
            logger.info(f"📊 SUMMARY: {passed}/{len(type_checks)} types validated successfully")
            if failed > 0:
                logger.error(f"❌ {failed} types failed validation")
            else:
                logger.info("🎉 All types validated successfully!")
            logger.info("="*60)
            
            return passed == len(type_checks)
            
        except Exception as e:
            logger.error(f"❌ Test failed with error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False


async def main():
    test = ValidatedComplexTypesTest()
    try:
        await test.setUp()
        success = await test.test_validated_complex_types()
        if success:
            logger.info("\n🎉 ALL VALIDATED COMPLEX TYPES TESTED SUCCESSFULLY!")
        else:
            logger.error("\n❌ SOME COMPLEX TYPES FAILED VALIDATION")
    finally:
        await test.tearDown()


if __name__ == "__main__":
    asyncio.run(main())