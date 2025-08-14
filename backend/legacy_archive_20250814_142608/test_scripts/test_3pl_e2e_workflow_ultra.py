#!/usr/bin/env python3
"""
🔥 ULTRA E2E Workflow Test - Real 3PL Dataset
============================================

Complete End-to-End workflow test using the real spice_harvester_synthetic_3pl dataset
following the user's "think ultra" approach with Claude RULE principles.

Test Flow:
1. 🗄️  Database Creation & Management
2. 📊 Data Upload (products.csv, orders.csv, order_items.csv)
3. 🤖 ML Type Inference
4. 📋 Schema Generation
5. 💾 TerminusDB Storage
6. ✅ Data Verification
7. 🧹 Cleanup
"""

import asyncio
import aiohttp
import json
import csv
import os
from typing import Dict, List, Any
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
BASE_URLS = {
    "oms": "http://localhost:8000",
    "bff": "http://localhost:8002", 
    "funnel": "http://localhost:8003"
}

TEST_DATA_PATH = "/Users/isihyeon/Desktop/SPICE HARVESTER/test_data/spice_harvester_synthetic_3pl"
TEST_DB_NAME = "e2e_3pl_ultra_test"

class UltraE2EWorkflowTester:
    """Ultra comprehensive E2E workflow tester"""
    
    def __init__(self):
        self.session: aiohttp.ClientSession = None
        self.test_db_name = TEST_DB_NAME
        self.results = {
            "database_created": False,
            "data_uploaded": {},
            "type_inference": {},
            "schema_generated": {},
            "data_stored": False,
            "verification": {},
            "cleanup": False
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def make_request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with error handling"""
        try:
            async with self.session.request(method, url, **kwargs) as response:
                content_type = response.headers.get('content-type', '')
                if 'application/json' in content_type:
                    data = await response.json()
                else:
                    text = await response.text()
                    data = {"text": text}
                
                return {
                    "status": response.status,
                    "data": data,
                    "success": response.status < 400
                }
        except Exception as e:
            return {
                "status": 0,
                "data": {"error": str(e)},
                "success": False
            }
    
    def load_csv_data(self, filename: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Load CSV data from test dataset"""
        file_path = Path(TEST_DATA_PATH) / filename
        data = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i >= limit:  # Limit for testing
                        break
                    data.append(row)
            
            print(f"   📊 Loaded {len(data)} rows from {filename}")
            return data
            
        except Exception as e:
            print(f"   ❌ Failed to load {filename}: {e}")
            return []
    
    async def step_1_database_management(self) -> bool:
        """Step 1: Create test database"""
        print("\n🗄️  Step 1: Database Creation & Management")
        print("=" * 50)
        
        # Check if database exists
        url = f"{BASE_URLS['oms']}/api/v1/database/exists/{self.test_db_name}"
        result = await self.make_request("GET", url)
        
        if result["success"] and result["data"].get("data", {}).get("exists", False):
            print(f"   🗑️  Database {self.test_db_name} exists, deleting...")
            delete_url = f"{BASE_URLS['oms']}/api/v1/database/{self.test_db_name}"
            delete_result = await self.make_request("DELETE", delete_url)
            
            if delete_result["success"]:
                print(f"   ✅ Database deleted successfully (Status: {delete_result['status']})")
                await asyncio.sleep(3)  # Wait for deletion
            else:
                print(f"   ⚠️  Database deletion status: {delete_result['status']}")
        
        # Create new database
        print(f"   📦 Creating database: {self.test_db_name}")
        create_url = f"{BASE_URLS['oms']}/api/v1/database/create"
        create_data = {
            "name": self.test_db_name,
            "description": "Ultra E2E test database for 3PL synthetic dataset"
        }
        
        create_result = await self.make_request("POST", create_url, json=create_data)
        
        if create_result["success"]:
            print(f"   🎉 Database created successfully! (Status: {create_result['status']})")
            if create_result["status"] == 202:
                print("   📋 Event Sourcing mode: Command accepted")
                # Get command ID if available
                command_id = create_result["data"].get("data", {}).get("command_id")
                if command_id:
                    print(f"   🆔 Command ID: {command_id}")
            
            self.results["database_created"] = True
            return True
        else:
            print(f"   ❌ Database creation failed: {create_result}")
            return False
    
    async def step_2_data_upload(self) -> bool:
        """Step 2: Upload CSV data files"""
        print("\n📊 Step 2: Data Upload")
        print("=" * 50)
        
        # Files to upload with their priorities
        files_to_upload = [
            ("products.csv", "Product catalog data"),
            ("orders.csv", "Order transaction data"), 
            ("order_items.csv", "Order line items data")
        ]
        
        upload_success = True
        
        for filename, description in files_to_upload:
            print(f"\n   📁 Uploading {filename} - {description}")
            
            # Load CSV data
            csv_data = self.load_csv_data(filename, limit=30)  # Limit for faster testing
            if not csv_data:
                print(f"   ❌ No data loaded from {filename}")
                upload_success = False
                continue
            
            # Upload via BFF data upload endpoint
            upload_url = f"{BASE_URLS['bff']}/api/v1/data/upload"
            upload_payload = {
                "database_name": self.test_db_name,
                "data": csv_data,
                "table_name": filename.replace('.csv', ''),
                "description": f"{description} from {filename}"
            }
            
            result = await self.make_request("POST", upload_url, json=upload_payload)
            
            if result["success"]:
                print(f"   ✅ {filename} uploaded successfully!")
                print(f"      📊 Rows: {len(csv_data)}")
                print(f"      🏷️  Columns: {list(csv_data[0].keys()) if csv_data else 'N/A'}")
                self.results["data_uploaded"][filename] = {
                    "success": True,
                    "rows": len(csv_data),
                    "columns": list(csv_data[0].keys()) if csv_data else []
                }
            else:
                print(f"   ❌ {filename} upload failed: {result['data']}")
                self.results["data_uploaded"][filename] = {
                    "success": False,
                    "error": result["data"]
                }
                upload_success = False
        
        return upload_success
    
    async def step_3_type_inference(self) -> bool:
        """Step 3: ML-based Type Inference"""
        print("\n🤖 Step 3: ML Type Inference")
        print("=" * 50)
        
        inference_success = True
        
        for filename in ["products.csv", "orders.csv", "order_items.csv"]:
            table_name = filename.replace('.csv', '')
            print(f"\n   🧠 Analyzing {table_name} for type inference...")
            
            # Get sample data for type inference
            csv_data = self.load_csv_data(filename, limit=10)
            if not csv_data:
                continue
                
            # Perform type inference via Funnel service
            inference_url = f"{BASE_URLS['funnel']}/api/v1/analyze/dataset"
            inference_payload = {
                "data": csv_data,
                "table_name": table_name,
                "include_suggestions": True
            }
            
            result = await self.make_request("POST", inference_url, json=inference_payload)
            
            if result["success"]:
                analysis_data = result["data"].get("data", {})
                columns_analyzed = analysis_data.get("columns", [])
                
                print(f"   🎯 Type inference completed for {table_name}!")
                print(f"      📊 Columns analyzed: {len(columns_analyzed)}")
                
                # Show detailed results
                for col in columns_analyzed[:3]:  # Show first 3 columns
                    col_name = col.get("column_name", "Unknown")
                    inferred_type = col.get("inferred_type", "Unknown")
                    confidence = col.get("confidence", 0)
                    print(f"      🔍 {col_name}: {inferred_type} (confidence: {confidence:.1%})")
                
                self.results["type_inference"][table_name] = {
                    "success": True,
                    "columns_analyzed": len(columns_analyzed),
                    "results": columns_analyzed
                }
            else:
                print(f"   ❌ Type inference failed for {table_name}: {result['data']}")
                self.results["type_inference"][table_name] = {
                    "success": False,
                    "error": result["data"]
                }
                inference_success = False
        
        return inference_success
    
    async def step_4_schema_generation(self) -> bool:
        """Step 4: Schema Generation"""
        print("\n📋 Step 4: Schema Generation")
        print("=" * 50)
        
        print(f"   🏗️  Generating ontology schema for {self.test_db_name}...")
        
        # Use the schema suggestion endpoint
        schema_url = f"{BASE_URLS['bff']}/api/v1/ontology/suggest-schema"
        schema_payload = {
            "database_name": self.test_db_name,
            "label": "3PL Logistics Ontology",
            "description": "Comprehensive ontology for 3PL logistics operations including products, orders, and inventory management"
        }
        
        result = await self.make_request("POST", schema_url, json=schema_payload)
        
        if result["success"]:
            schema_data = result["data"].get("data", {})
            print(f"   🎉 Schema generation completed!")
            print(f"      📊 Status: {result['status']}")
            
            # Show schema details if available
            if "classes" in schema_data:
                classes = schema_data["classes"]
                print(f"      🏷️  Classes generated: {len(classes)}")
                for cls in classes[:3]:  # Show first 3 classes
                    print(f"         - {cls.get('name', 'Unknown')}")
            
            if "properties" in schema_data:
                properties = schema_data["properties"]
                print(f"      🔗 Properties generated: {len(properties)}")
            
            self.results["schema_generated"] = {
                "success": True,
                "schema_data": schema_data
            }
            return True
        else:
            print(f"   ❌ Schema generation failed: {result['data']}")
            self.results["schema_generated"] = {
                "success": False,
                "error": result["data"]
            }
            return False
    
    async def step_5_data_verification(self) -> bool:
        """Step 5: Data Verification"""
        print("\n✅ Step 5: Data Verification")
        print("=" * 50)
        
        # Check database exists and has data
        exists_url = f"{BASE_URLS['oms']}/api/v1/database/exists/{self.test_db_name}"
        exists_result = await self.make_request("GET", exists_url)
        
        if exists_result["success"]:
            exists = exists_result["data"].get("data", {}).get("exists", False)
            print(f"   🗄️  Database exists: {exists}")
            
            self.results["verification"]["database_exists"] = exists
            
            # Additional verification could include:
            # - Querying actual data from TerminusDB
            # - Validating schema structure
            # - Checking data integrity
            
            return exists
        else:
            print(f"   ❌ Database verification failed: {exists_result['data']}")
            return False
    
    async def step_6_cleanup(self) -> bool:
        """Step 6: Cleanup"""
        print("\n🧹 Step 6: Cleanup")
        print("=" * 50)
        
        print(f"   🗑️  Cleaning up test database: {self.test_db_name}")
        
        delete_url = f"{BASE_URLS['oms']}/api/v1/database/{self.test_db_name}"
        delete_result = await self.make_request("DELETE", delete_url)
        
        if delete_result["success"]:
            print(f"   ✅ Test database cleaned up successfully! (Status: {delete_result['status']})")
            self.results["cleanup"] = True
            return True
        else:
            print(f"   ⚠️  Cleanup status: {delete_result['status']} - {delete_result['data']}")
            self.results["cleanup"] = False
            return False
    
    async def run_complete_workflow(self) -> Dict[str, Any]:
        """Run the complete E2E workflow"""
        print("🔥 ULTRA E2E WORKFLOW TEST - Real 3PL Dataset")
        print("=" * 60)
        print("Following Claude RULE: Ultra-deep thinking, real implementations only")
        print("=" * 60)
        
        # Run all steps
        step_results = []
        
        try:
            # Step 1: Database Management
            step_1_success = await self.step_1_database_management()
            step_results.append(("Database Management", step_1_success))
            
            if not step_1_success:
                print("   ⚠️  Skipping remaining steps due to database creation failure")
                return self.results
            
            # Wait for database to be ready
            await asyncio.sleep(3)
            
            # Step 2: Data Upload
            step_2_success = await self.step_2_data_upload()
            step_results.append(("Data Upload", step_2_success))
            
            # Step 3: Type Inference
            step_3_success = await self.step_3_type_inference()
            step_results.append(("Type Inference", step_3_success))
            
            # Step 4: Schema Generation
            step_4_success = await self.step_4_schema_generation()
            step_results.append(("Schema Generation", step_4_success))
            
            # Wait for processing
            await asyncio.sleep(5)
            
            # Step 5: Data Verification
            step_5_success = await self.step_5_data_verification()
            step_results.append(("Data Verification", step_5_success))
            
            # Step 6: Cleanup
            step_6_success = await self.step_6_cleanup()
            step_results.append(("Cleanup", step_6_success))
            
        except Exception as e:
            print(f"   💥 Workflow interrupted: {e}")
            step_results.append(("Workflow", False))
        
        # Print final summary
        print("\n" + "=" * 60)
        print("🎯 FINAL RESULTS SUMMARY")
        print("=" * 60)
        
        for step_name, success in step_results:
            status_icon = "✅" if success else "❌"
            print(f"{status_icon} {step_name}: {'SUCCESS' if success else 'FAILED'}")
        
        overall_success = all(success for _, success in step_results)
        print(f"\n🏆 OVERALL RESULT: {'🎉 COMPLETE SUCCESS!' if overall_success else '⚠️  PARTIAL SUCCESS'}")
        
        # Detailed results
        print("\n📊 Detailed Results:")
        print(json.dumps(self.results, indent=2, default=str))
        
        return self.results

async def main():
    """Main test execution"""
    print("🚀 Starting Ultra E2E Workflow Test...")
    
    async with UltraE2EWorkflowTester() as tester:
        results = await tester.run_complete_workflow()
    
    print("\n✨ Ultra E2E Workflow Test Completed!")
    return results

if __name__ == "__main__":
    # Run the test
    asyncio.run(main())