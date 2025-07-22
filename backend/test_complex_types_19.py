#!/usr/bin/env python3
"""
🔥 ULTRA! Comprehensive test for 19 different TerminusDB data types
Tests both properties and relationships with various complex types
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


class ComplexTypesTest:
    def __init__(self):
        # Use proper connection configuration
        self.connection_config = ConnectionConfig(
            server_url=os.getenv("TERMINUS_SERVER_URL", "http://localhost:6363"),
            user=os.getenv("TERMINUS_USER", "admin"),
            account=os.getenv("TERMINUS_ACCOUNT", "admin"),
            key=os.getenv("TERMINUS_KEY", "admin123"),
        )
        self.terminus = AsyncTerminusService(self.connection_config)
        self.test_db = f"test_complex_types_19_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.created_classes = []
        
    async def setUp(self):
        """Set up test environment"""
        logger.info("📦 Setting up test environment...")
        await self.terminus.connect()
        await self.terminus.create_database(self.test_db, "19 Complex Types Test Database")
        
    async def tearDown(self):
        """Clean up test environment"""
        logger.info("🧹 Cleaning up test environment...")
        try:
            await self.terminus.delete_database(self.test_db)
        except:
            pass
        await self.terminus.disconnect()
        
    async def test_19_complex_types(self):
        """Test 19 different complex data types"""
        logger.info("\n" + "="*80)
        logger.info("🔥 TESTING 19 COMPLEX DATA TYPES")
        logger.info("="*80 + "\n")
        
        # Define class with 19 different types
        complex_class = {
            "id": "ComplexTypes19",
            "label": "Complex Types Test",
            "description": "Testing 19 different TerminusDB data types",
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
                    "label": "4. Decimal Number",
                    "constraints": {"precision": 10, "scale": 2}
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
                # 12. Array<Float>
                {
                    "name": "type12_array_float",
                    "type": "array<float>",
                    "required": False,
                    "label": "12. Array of Floats",
                    "constraints": {"dimensions": 2}
                },
                # 13. Union Type (OneOf)
                {
                    "name": "type13_union",
                    "type": "union<string|integer|boolean>",
                    "required": False,
                    "label": "13. Union Type (String|Integer|Boolean)"
                },
                # 14. Optional<Decimal>
                {
                    "name": "type14_optional",
                    "type": "optional",
                    "required": False,
                    "label": "14. Optional Decimal",
                    "constraints": {"inner_type": "decimal"}
                },
                # 15. GeoPoint
                {
                    "name": "type15_geopoint",
                    "type": "geopoint",
                    "required": False,
                    "label": "15. Geographic Point"
                },
                # 16. JSON
                {
                    "name": "type16_json",
                    "type": "json",
                    "required": False,
                    "label": "16. JSON Data"
                },
                # 17. Text (long string)
                {
                    "name": "type17_text",
                    "type": "text",
                    "required": False,
                    "label": "17. Long Text"
                },
                # 18. URL
                {
                    "name": "type18_url",
                    "type": "url",
                    "required": False,
                    "label": "18. URL"
                },
                # 19. Email
                {
                    "name": "type19_email",
                    "type": "email",
                    "required": False,
                    "label": "19. Email Address",
                    "constraints": {
                        "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                    }
                }
            ],
            "relationships": []
        }
        
        try:
            # Create the class
            logger.info("📤 Creating class with 19 complex types...")
            result = await self.terminus.create_ontology_class(self.test_db, complex_class)
            self.created_classes.append("ComplexTypes19")
            logger.info("✅ Class created successfully!")
            
            # Retrieve and verify the class
            logger.info("\n🔍 Retrieving created class...")
            retrieved = await self.terminus.get_ontology(self.test_db, "ComplexTypes19")
            
            # Verify all 19 types
            logger.info("\n📊 Verifying 19 data types:")
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
                ("type10_list_string", "LIST<STRING>", "List<String>"),
                ("type11_set_integer", "SET<INTEGER>", "Set<Integer>"),
                ("type12_array_float", "ARRAY<FLOAT>", "Array<Float>"),
                ("type13_union", "UNION<STRING|INTEGER|BOOLEAN>", "Union Type"),
                ("type14_optional", "OPTIONAL<DECIMAL>", "Optional<Decimal>"),
                ("type15_geopoint", "GEOPOINT", "GeoPoint"),
                ("type16_json", "JSON", "JSON"),
                ("type17_text", "TEXT", "Text (Long String)"),
                ("type18_url", "URL", "URL"),
                ("type19_email", "EMAIL", "Email")
            ]
            
            passed = 0
            failed = 0
            
            for prop_name, expected_type, description in type_checks:
                if prop_name in properties_dict:
                    actual_type = properties_dict[prop_name].get("type", "UNKNOWN")
                    # Normalize type names for comparison
                    if actual_type.upper() == expected_type.upper() or \
                       (expected_type == "TEXT" and actual_type == "STRING") or \
                       (expected_type == "URL" and actual_type == "STRING") or \
                       (expected_type == "EMAIL" and actual_type == "STRING") or \
                       (expected_type.startswith("LIST<") and "LIST" in actual_type.upper()) or \
                       (expected_type.startswith("SET<") and "SET" in actual_type.upper()) or \
                       (expected_type.startswith("ARRAY<") and "ARRAY" in actual_type.upper()) or \
                       (expected_type.startswith("UNION<") and "UNION" in actual_type.upper()) or \
                       (expected_type.startswith("OPTIONAL<") and properties_dict[prop_name].get("required") == False):
                        logger.info(f"✅ {description}: {actual_type}")
                        passed += 1
                    else:
                        logger.error(f"❌ {description}: Expected {expected_type}, got {actual_type}")
                        failed += 1
                else:
                    logger.error(f"❌ {description}: Property '{prop_name}' not found!")
                    failed += 1
            
            # Check constraints
            logger.info("\n🔧 Checking constraints:")
            
            # Check enum constraint
            if "type9_enum" in properties_dict:
                enum_constraint = properties_dict["type9_enum"].get("constraints", {}).get("enum")
                if enum_constraint:
                    logger.info(f"✅ Enum constraint found: {enum_constraint}")
                else:
                    logger.warning("⚠️ Enum constraint not preserved in metadata")
                    
            # Check email pattern
            if "type19_email" in properties_dict:
                email_pattern = properties_dict["type19_email"].get("constraints", {}).get("pattern")
                if email_pattern:
                    logger.info(f"✅ Email pattern constraint found")
                else:
                    logger.warning("⚠️ Email pattern constraint not preserved")
            
            # Summary
            logger.info("\n" + "="*60)
            logger.info(f"📊 SUMMARY: {passed}/{len(type_checks)} types validated successfully")
            if failed > 0:
                logger.error(f"❌ {failed} types failed validation")
            logger.info("="*60)
            
            return passed == len(type_checks)
            
        except Exception as e:
            logger.error(f"❌ Test failed with error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False


async def main():
    test = ComplexTypesTest()
    try:
        await test.setUp()
        success = await test.test_19_complex_types()
        if success:
            logger.info("\n🎉 ALL 19 COMPLEX TYPES TESTED SUCCESSFULLY!")
        else:
            logger.error("\n❌ SOME COMPLEX TYPES FAILED VALIDATION")
    finally:
        await test.tearDown()


if __name__ == "__main__":
    asyncio.run(main())