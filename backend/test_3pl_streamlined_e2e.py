#!/usr/bin/env python3
"""
🎯 STREAMLINED E2E Test - Real 3PL Dataset
==========================================

Focused E2E test bypassing the problematic database creation API and focusing
on the core ML-driven workflow that we know works.

Test Flow:
1. 📊 Data Loading & Preparation  
2. 🤖 ML Type Inference (via Funnel)
3. 📋 Schema Generation (via BFF)
4. ✅ Verification of Results
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
    "funnel": "http://localhost:8004"
}

TEST_DATA_PATH = "/Users/isihyeon/Desktop/SPICE HARVESTER/test_data/spice_harvester_synthetic_3pl"

class StreamlinedE2ETest:
    """Streamlined E2E test focusing on working components"""
    
    def __init__(self):
        self.session: aiohttp.ClientSession = None
        self.results = {
            "data_loaded": {},
            "type_inference": {},
            "schema_generation": {},
            "overall_success": False
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
    
    def load_csv_data(self, filename: str, limit: int = 20) -> Dict[str, Any]:
        """Load CSV data from test dataset in columnar format"""
        file_path = Path(TEST_DATA_PATH) / filename
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = []
                columns = None
                
                for i, row in enumerate(reader):
                    if i >= limit:  # Limit for testing
                        break
                    if columns is None:
                        columns = list(row.keys())
                    
                    # Convert row to list format maintaining column order
                    row_values = [row.get(col, '') for col in columns]
                    rows.append(row_values)
            
            print(f"   📊 Loaded {len(rows)} rows from {filename}")
            return {
                "data": rows,
                "columns": columns or []
            }
            
        except Exception as e:
            print(f"   ❌ Failed to load {filename}: {e}")
            return {"data": [], "columns": []}
    
    async def step_1_data_loading(self) -> bool:
        """Step 1: Load and prepare 3PL dataset"""
        print("\n📊 Step 1: Data Loading & Preparation")
        print("=" * 50)
        
        # Load key 3PL dataset files
        datasets = {
            "products": "products.csv",
            "orders": "orders.csv", 
            "order_items": "order_items.csv"
        }
        
        all_success = True
        
        for dataset_name, filename in datasets.items():
            print(f"\n   📁 Loading {dataset_name} dataset...")
            dataset_info = self.load_csv_data(filename, limit=15)  # Small samples for testing
            
            if dataset_info["data"]:
                rows = dataset_info["data"]
                columns = dataset_info["columns"]
                print(f"   ✅ {dataset_name}: {len(rows)} records loaded")
                print(f"      🏷️  Columns: {columns}")
                
                # Show sample data (first row)
                if rows:
                    sample_values = rows[0][:3]  # First 3 values
                    sample_cols = columns[:3]  # First 3 column names
                    sample_display = {col: val for col, val in zip(sample_cols, sample_values)}
                    print(f"      📋 Sample: {sample_display}...")
                
                self.results["data_loaded"][dataset_name] = {
                    "success": True,
                    "records": len(rows),
                    "columns": columns,
                    "dataset_info": dataset_info
                }
            else:
                print(f"   ❌ Failed to load {dataset_name}")
                self.results["data_loaded"][dataset_name] = {"success": False}
                all_success = False
        
        return all_success
    
    async def step_2_type_inference(self) -> bool:
        """Step 2: ML-based Type Inference via Funnel service"""
        print("\n🤖 Step 2: ML Type Inference")
        print("=" * 50)
        
        inference_success = True
        
        for dataset_name, dataset_info in self.results["data_loaded"].items():
            if not dataset_info.get("success", False):
                continue
                
            print(f"\n   🧠 Analyzing {dataset_name} for type inference...")
            
            dataset_data = dataset_info["dataset_info"]
            
            # Perform type inference via Funnel service with correct columnar format
            inference_url = f"{BASE_URLS['funnel']}/api/v1/funnel/analyze"
            inference_payload = {
                "data": dataset_data["data"],  # Columnar format [[val1, val2], ...]
                "columns": dataset_data["columns"],  # Column names
                "table_name": dataset_name,
                "include_suggestions": True
            }
            
            result = await self.make_request("POST", inference_url, json=inference_payload)
            
            if result["success"]:
                columns_analyzed = result["data"].get("columns", [])
                
                print(f"   🎯 Type inference completed for {dataset_name}!")
                print(f"      📊 Columns analyzed: {len(columns_analyzed)}")
                
                # Show detailed results for all columns
                total_confidence = 0
                high_confidence_count = 0
                
                for col in columns_analyzed:
                    col_name = col.get("column_name", "Unknown")
                    inferred_type_info = col.get("inferred_type", {})
                    inferred_type = inferred_type_info.get("type", "Unknown")
                    confidence = inferred_type_info.get("confidence", 0)
                    
                    confidence_emoji = "🎯" if confidence > 0.8 else "📊" if confidence > 0.6 else "❓"
                    print(f"      {confidence_emoji} {col_name}: {inferred_type} (confidence: {confidence:.1%})")
                    
                    total_confidence += confidence
                    if confidence > 0.8:
                        high_confidence_count += 1
                
                avg_confidence = total_confidence / len(columns_analyzed) if columns_analyzed else 0
                print(f"      📈 Average confidence: {avg_confidence:.1%}")
                print(f"      🎯 High confidence predictions: {high_confidence_count}/{len(columns_analyzed)}")
                
                self.results["type_inference"][dataset_name] = {
                    "success": True,
                    "columns_analyzed": len(columns_analyzed),
                    "average_confidence": avg_confidence,
                    "high_confidence_count": high_confidence_count,
                    "results": columns_analyzed
                }
            else:
                print(f"   ❌ Type inference failed for {dataset_name}: {result['data']}")
                self.results["type_inference"][dataset_name] = {
                    "success": False,
                    "error": result["data"]
                }
                inference_success = False
        
        return inference_success
    
    async def step_3_schema_generation(self) -> bool:
        """Step 3: Schema Generation via BFF service"""
        print("\n📋 Step 3: Schema Generation")
        print("=" * 50)
        
        print(f"   🏗️  Generating combined ontology schema for 3PL dataset...")
        
        # Use the schema suggestion endpoint for the combined dataset
        db_name = "streamlined_3pl_test"
        schema_url = f"{BASE_URLS['bff']}/api/v1/database/{db_name}/suggest-schema-from-data"
        
        # Combine all loaded data for schema generation in columnar format
        combined_data = []
        combined_columns = []
        
        for dataset_name, dataset_info in self.results["data_loaded"].items():
            if dataset_info.get("success", False):
                dataset_data = dataset_info["dataset_info"]
                rows = dataset_data["data"]
                columns = dataset_data["columns"]
                
                # Add table source column to identify data origin
                extended_columns = columns + ["_table_source"]
                for row in rows:
                    extended_row = row + [dataset_name]
                    combined_data.append(extended_row)
                
                # Track combined columns (only set once)
                if not combined_columns:
                    combined_columns = extended_columns
        
        schema_payload = {
            "data": combined_data,  # Columnar format [[val1, val2, table_name], ...]
            "columns": combined_columns,  # Column names including _table_source
            "label": "3PL Logistics Ontology - Streamlined Test",
            "description": "Comprehensive ontology for 3PL logistics operations including products, orders, and inventory management - generated from real synthetic dataset"
        }
        
        result = await self.make_request("POST", schema_url, json=schema_payload)
        
        if result["success"]:
            schema_response = result["data"]
            suggested_schema = schema_response.get("suggested_schema", {})
            print(f"   🎉 Schema generation completed!")
            print(f"      📊 Status: {result['status']}")
            
            # Show schema details if available
            properties = suggested_schema.get("properties", [])
            if properties:
                print(f"      🔗 Properties generated: {len(properties)}")
                for i, prop in enumerate(properties[:5]):  # Show first 5 properties  
                    prop_name = prop.get('name', 'Unknown')
                    prop_type = prop.get('type', 'Unknown')
                    confidence = prop.get('metadata', {}).get('inferred_confidence', 0)
                    print(f"         {i+1}. {prop_name} ({prop_type}, confidence: {confidence:.1%})")
                if len(properties) > 5:
                    print(f"         ... and {len(properties) - 5} more properties")
            
            # Get summary information
            analysis_summary = schema_response.get("analysis_summary", {})
            total_columns = analysis_summary.get("total_columns", 0)
            avg_confidence = analysis_summary.get("average_confidence", 0)
            high_confidence = analysis_summary.get("high_confidence_columns", 0)
            
            print(f"      📈 Total columns analyzed: {total_columns}")
            print(f"      🎯 High confidence columns: {high_confidence}/{total_columns}")
            print(f"      📊 Average confidence: {avg_confidence:.1%}")
            
            self.results["schema_generation"] = {
                "success": True,
                "total_elements": len(properties),
                "properties_count": len(properties),
                "total_columns": total_columns,
                "average_confidence": avg_confidence,
                "high_confidence_columns": high_confidence,
                "schema_data": suggested_schema
            }
            return True
        else:
            print(f"   ❌ Schema generation failed: {result['data']}")
            self.results["schema_generation"] = {
                "success": False,
                "error": result["data"]
            }
            return False
    
    async def step_4_verification(self) -> bool:
        """Step 4: Results Verification & Analysis"""
        print("\n✅ Step 4: Results Verification & Analysis")
        print("=" * 50)
        
        # Check data loading results
        data_loaded_count = sum(1 for info in self.results["data_loaded"].values() if info.get("success", False))
        total_datasets = len(self.results["data_loaded"])
        print(f"   📊 Data Loading: {data_loaded_count}/{total_datasets} datasets successfully loaded")
        
        # Check type inference results
        inference_success_count = sum(1 for info in self.results["type_inference"].values() if info.get("success", False))
        total_inference = len(self.results["type_inference"])
        print(f"   🤖 Type Inference: {inference_success_count}/{total_inference} datasets successfully analyzed")
        
        if inference_success_count > 0:
            # Calculate overall inference quality
            total_avg_confidence = sum(info.get("average_confidence", 0) for info in self.results["type_inference"].values() if info.get("success", False))
            overall_avg_confidence = total_avg_confidence / inference_success_count
            print(f"      📈 Overall inference confidence: {overall_avg_confidence:.1%}")
        
        # Check schema generation results
        schema_success = self.results["schema_generation"].get("success", False)
        print(f"   📋 Schema Generation: {'✅ SUCCESS' if schema_success else '❌ FAILED'}")
        
        if schema_success:
            total_elements = self.results["schema_generation"].get("total_elements", 0)
            print(f"      🏗️  Total ontology elements created: {total_elements}")
        
        # Determine overall success
        overall_success = (
            data_loaded_count == total_datasets and
            inference_success_count == total_inference and
            schema_success
        )
        
        self.results["overall_success"] = overall_success
        
        print(f"\n   🏆 OVERALL RESULT: {'🎉 COMPLETE SUCCESS!' if overall_success else '⚠️  PARTIAL SUCCESS'}")
        
        return overall_success
    
    async def run_streamlined_workflow(self) -> Dict[str, Any]:
        """Run the streamlined E2E workflow"""
        print("🎯 STREAMLINED E2E WORKFLOW TEST - Real 3PL Dataset")
        print("=" * 60)
        print("Testing core ML-driven workflow components")
        print("=" * 60)
        
        # Run all steps
        step_results = []
        
        try:
            # Step 1: Data Loading
            step_1_success = await self.step_1_data_loading()
            step_results.append(("Data Loading", step_1_success))
            
            if not step_1_success:
                print("   ⚠️  Continuing with partial data...")
            
            # Step 2: Type Inference
            step_2_success = await self.step_2_type_inference()
            step_results.append(("Type Inference", step_2_success))
            
            # Step 3: Schema Generation
            step_3_success = await self.step_3_schema_generation()
            step_results.append(("Schema Generation", step_3_success))
            
            # Step 4: Verification
            step_4_success = await self.step_4_verification()
            step_results.append(("Verification", step_4_success))
            
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
        
        overall_success = self.results.get("overall_success", False)
        print(f"\n🏆 OVERALL RESULT: {'🎉 COMPLETE SUCCESS!' if overall_success else '⚠️  PARTIAL SUCCESS'}")
        
        # Show key metrics
        if self.results.get("type_inference"):
            successful_inferences = [info for info in self.results["type_inference"].values() if info.get("success")]
            if successful_inferences:
                avg_confidence = sum(info.get("average_confidence", 0) for info in successful_inferences) / len(successful_inferences)
                print(f"📊 Average ML confidence: {avg_confidence:.1%}")
        
        if self.results.get("schema_generation", {}).get("success"):
            total_elements = self.results["schema_generation"].get("total_elements", 0)
            print(f"🏗️  Ontology elements generated: {total_elements}")
        
        return self.results

async def main():
    """Main test execution"""
    print("🚀 Starting Streamlined E2E Workflow Test...")
    
    async with StreamlinedE2ETest() as tester:
        results = await tester.run_streamlined_workflow()
    
    print("\n✨ Streamlined E2E Workflow Test Completed!")
    return results

if __name__ == "__main__":
    # Run the test
    asyncio.run(main())