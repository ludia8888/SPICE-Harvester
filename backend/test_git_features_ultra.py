#!/usr/bin/env python3
"""
🔥 ULTRA: Comprehensive Git-like Features Test
Tests all git-like functionality: commit, push, merge, conflict, diff, Pull Request, Revert
"""

import asyncio
import logging
import sys
import os
import time
import json

# PYTHONPATH 설정
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oms.services.async_terminus import AsyncTerminusService

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class GitFeaturesTest:
    """🔥 ULTRA: Comprehensive Git Features Test Suite"""
    
    def __init__(self):
        self.terminus = AsyncTerminusService()
        self.test_results = {
            "commit": {"tested": False, "success": False, "details": ""},
            "push": {"tested": False, "success": False, "details": ""},
            "merge": {"tested": False, "success": False, "details": ""},
            "conflict": {"tested": False, "success": False, "details": ""},
            "diff": {"tested": False, "success": False, "details": ""},
            "pull_request": {"tested": False, "success": False, "details": ""},
            "revert": {"tested": False, "success": False, "details": ""}
        }
        self.test_db = f"git_test_{int(time.time())}"
        
    async def setup_test_database(self):
        """Create test database and initial schema"""
        try:
            await self.terminus.create_database(self.test_db, "Git Features Test Database")
            logger.info(f"✅ Created test database: {self.test_db}")
            
            # Create initial schema
            product_schema = {
                "@type": "Class",
                "@id": "Product",
                "@key": {"@type": "Random"},
                "name": {"@class": "xsd:string", "@type": "Optional"},
                "price": {"@class": "xsd:decimal", "@type": "Optional"}
            }
            
            await self.terminus._make_request(
                "POST",
                f"/api/document/{self.terminus.connection_info.account}/{self.test_db}",
                [product_schema],
                params={"graph_type": "schema", "author": "test", "message": "Initial schema"}
            )
            logger.info("✅ Created initial Product schema")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup test database: {e}")
            return False
    
    async def test_commit_feature(self):
        """Test 1: Commit functionality"""
        logger.info("\n🔍 TEST 1: Testing COMMIT functionality")
        try:
            # Add a new property to Product
            update_schema = {
                "@type": "Class",
                "@id": "Product",
                "@key": {"@type": "Random"},
                "name": {"@class": "xsd:string", "@type": "Optional"},
                "price": {"@class": "xsd:decimal", "@type": "Optional"},
                "description": {"@class": "xsd:string", "@type": "Optional"}  # New property
            }
            
            # This should create an implicit commit
            await self.terminus._make_request(
                "PUT",
                f"/api/document/{self.terminus.connection_info.account}/{self.test_db}",
                [update_schema],
                params={"graph_type": "schema", "author": "test", "message": "Add description field"}
            )
            
            # Check commit history
            history = await self.terminus.get_commit_history(self.test_db, limit=5)
            
            if len(history) >= 2:
                latest_commit = history[0]
                logger.info(f"✅ COMMIT SUCCESS: Latest commit: {latest_commit.get('message')}")
                self.test_results["commit"] = {
                    "tested": True,
                    "success": True,
                    "details": f"Successfully created commit with message: {latest_commit.get('message')}"
                }
            else:
                raise Exception("Commit not created properly")
                
        except Exception as e:
            logger.error(f"❌ COMMIT FAILED: {e}")
            self.test_results["commit"] = {"tested": True, "success": False, "details": str(e)}
    
    async def test_push_feature(self):
        """Test 2: Push functionality (branch creation)"""
        logger.info("\n🔍 TEST 2: Testing PUSH functionality (branch operations)")
        try:
            # Create a new branch (simulating push to new branch)
            branch_name = "feature/test-push"
            await self.terminus.create_branch(self.test_db, branch_name, "main")
            
            # List branches to verify
            branches = await self.terminus.list_branches(self.test_db)
            
            if branch_name in branches:
                logger.info(f"✅ PUSH SUCCESS: Created branch {branch_name}")
                self.test_results["push"] = {
                    "tested": True,
                    "success": True,
                    "details": f"Successfully created/pushed to branch: {branch_name}"
                }
            else:
                raise Exception(f"Branch {branch_name} not found after creation")
                
        except Exception as e:
            logger.error(f"❌ PUSH FAILED: {e}")
            self.test_results["push"] = {"tested": True, "success": False, "details": str(e)}
    
    async def test_diff_feature(self):
        """Test 3: Diff functionality"""
        logger.info("\n🔍 TEST 3: Testing DIFF functionality")
        try:
            # Make a change in the feature branch
            await self.terminus.checkout(self.test_db, "feature/test-push")
            
            # Add Customer class in feature branch
            customer_schema = {
                "@type": "Class",
                "@id": "Customer",
                "@key": {"@type": "Random"},
                "name": {"@class": "xsd:string", "@type": "Optional"},
                "email": {"@class": "xsd:string", "@type": "Optional"}
            }
            
            await self.terminus._make_request(
                "POST",
                f"/api/document/{self.terminus.connection_info.account}/{self.test_db}",
                [customer_schema],
                params={"graph_type": "schema", "author": "test", "message": "Add Customer class"}
            )
            
            # Get diff between main and feature branch
            diff_result = await self.terminus.diff(self.test_db, "main", "feature/test-push")
            
            if diff_result and len(diff_result) > 0:
                logger.info(f"✅ DIFF SUCCESS: Found {len(diff_result)} differences")
                logger.info(f"   First diff: {diff_result[0].get('type')} at {diff_result[0].get('path')}")
                self.test_results["diff"] = {
                    "tested": True,
                    "success": True,
                    "details": f"Found {len(diff_result)} differences between branches"
                }
            else:
                raise Exception("No differences found between branches")
                
        except Exception as e:
            logger.error(f"❌ DIFF FAILED: {e}")
            self.test_results["diff"] = {"tested": True, "success": False, "details": str(e)}
    
    async def test_conflict_detection(self):
        """Test 4: Conflict detection"""
        logger.info("\n🔍 TEST 4: Testing CONFLICT detection")
        try:
            # Create another branch from main
            conflict_branch = "feature/conflict-test"
            await self.terminus.checkout(self.test_db, "main")
            await self.terminus.create_branch(self.test_db, conflict_branch, "main")
            
            # Modify Product in main branch
            await self.terminus.checkout(self.test_db, "main")
            main_product = {
                "@type": "Class",
                "@id": "Product",
                "@key": {"@type": "Random"},
                "name": {"@class": "xsd:string", "@type": "Optional"},
                "price": {"@class": "xsd:decimal", "@type": "Optional"},
                "description": {"@class": "xsd:string", "@type": "Optional"},
                "category": {"@class": "xsd:string", "@type": "Optional"}  # Added in main
            }
            
            await self.terminus._make_request(
                "PUT",
                f"/api/document/{self.terminus.connection_info.account}/{self.test_db}",
                [main_product],
                params={"graph_type": "schema", "author": "test", "message": "Add category to Product"}
            )
            
            # Modify Product differently in conflict branch
            await self.terminus.checkout(self.test_db, conflict_branch)
            conflict_product = {
                "@type": "Class",
                "@id": "Product",
                "@key": {"@type": "Random"},
                "name": {"@class": "xsd:string", "@type": "Optional"},
                "price": {"@class": "xsd:decimal", "@type": "Optional"},
                "description": {"@class": "xsd:string", "@type": "Optional"},
                "sku": {"@class": "xsd:string", "@type": "Optional"}  # Added in conflict branch
            }
            
            await self.terminus._make_request(
                "PUT",
                f"/api/document/{self.terminus.connection_info.account}/{self.test_db}",
                [conflict_product],
                params={"graph_type": "schema", "author": "test", "message": "Add SKU to Product"}
            )
            
            # Try to merge and detect conflicts
            merge_result = await self.terminus.merge(self.test_db, conflict_branch, "main", strategy="three_way")
            
            # Check if conflicts were detected
            if "conflicts" in str(merge_result).lower() or "conflict" in str(merge_result).lower():
                logger.info("✅ CONFLICT DETECTION SUCCESS: Conflicts properly detected")
                self.test_results["conflict"] = {
                    "tested": True,
                    "success": True,
                    "details": "Conflicts detected when merging branches with different changes"
                }
            else:
                # Even if merge succeeded, check if both properties exist
                await self.terminus.checkout(self.test_db, "main")
                schema_response = await self.terminus._make_request(
                    "GET",
                    f"/api/document/{self.terminus.connection_info.account}/{self.test_db}",
                    params={"graph_type": "schema"}
                )
                
                # Parse response
                if isinstance(schema_response, str):
                    schema_doc = json.loads(schema_response.strip().split('\n')[0])
                else:
                    schema_doc = schema_response[0]
                
                has_category = "category" in schema_doc
                has_sku = "sku" in schema_doc
                
                if has_category and has_sku:
                    logger.info("✅ CONFLICT RESOLUTION SUCCESS: Merge handled both changes")
                    self.test_results["conflict"] = {
                        "tested": True,
                        "success": True,
                        "details": "Three-way merge successfully merged both changes"
                    }
                else:
                    raise Exception("Merge didn't properly handle both changes")
                    
        except Exception as e:
            logger.error(f"❌ CONFLICT DETECTION FAILED: {e}")
            self.test_results["conflict"] = {"tested": True, "success": False, "details": str(e)}
    
    async def test_merge_feature(self):
        """Test 5: Merge functionality"""
        logger.info("\n🔍 TEST 5: Testing MERGE functionality")
        try:
            # Create a simple branch for clean merge
            merge_branch = "feature/clean-merge"
            await self.terminus.checkout(self.test_db, "main")
            await self.terminus.create_branch(self.test_db, merge_branch, "main")
            
            # Add Order class in merge branch
            await self.terminus.checkout(self.test_db, merge_branch)
            order_schema = {
                "@type": "Class",
                "@id": "Order",
                "@key": {"@type": "Random"},
                "order_id": {"@class": "xsd:string", "@type": "Optional"},
                "total": {"@class": "xsd:decimal", "@type": "Optional"}
            }
            
            await self.terminus._make_request(
                "POST",
                f"/api/document/{self.terminus.connection_info.account}/{self.test_db}",
                [order_schema],
                params={"graph_type": "schema", "author": "test", "message": "Add Order class"}
            )
            
            # Merge back to main
            await self.terminus.checkout(self.test_db, "main")
            merge_result = await self.terminus.merge(self.test_db, merge_branch, "main")
            
            # Verify Order class exists in main
            schema_response = await self.terminus._make_request(
                "GET",
                f"/api/document/{self.terminus.connection_info.account}/{self.test_db}",
                params={"graph_type": "schema"}
            )
            
            # Check if Order class exists
            order_exists = False
            if isinstance(schema_response, str):
                for line in schema_response.strip().split('\n'):
                    if line.strip():
                        doc = json.loads(line)
                        if doc.get("@id") == "Order":
                            order_exists = True
                            break
            else:
                order_exists = any(doc.get("@id") == "Order" for doc in schema_response)
            
            if order_exists:
                logger.info("✅ MERGE SUCCESS: Successfully merged Order class to main")
                self.test_results["merge"] = {
                    "tested": True,
                    "success": True,
                    "details": "Successfully merged feature branch with new class to main"
                }
            else:
                raise Exception("Order class not found in main after merge")
                
        except Exception as e:
            logger.error(f"❌ MERGE FAILED: {e}")
            self.test_results["merge"] = {"tested": True, "success": False, "details": str(e)}
    
    async def test_pull_request_feature(self):
        """Test 6: Pull Request functionality (simulation)"""
        logger.info("\n🔍 TEST 6: Testing PULL REQUEST functionality")
        try:
            # Pull Request is simulated through branch + merge workflow
            pr_branch = "feature/pull-request-test"
            await self.terminus.checkout(self.test_db, "main")
            await self.terminus.create_branch(self.test_db, pr_branch, "main")
            
            # Make changes in PR branch
            await self.terminus.checkout(self.test_db, pr_branch)
            invoice_schema = {
                "@type": "Class",
                "@id": "Invoice",
                "@key": {"@type": "Random"},
                "invoice_number": {"@class": "xsd:string", "@type": "Optional"},
                "amount": {"@class": "xsd:decimal", "@type": "Optional"}
            }
            
            await self.terminus._make_request(
                "POST",
                f"/api/document/{self.terminus.connection_info.account}/{self.test_db}",
                [invoice_schema],
                params={"graph_type": "schema", "author": "test", "message": "Add Invoice class for billing"}
            )
            
            # Get diff for review (PR review simulation)
            diff_result = await self.terminus.diff(self.test_db, "main", pr_branch)
            
            if diff_result and len(diff_result) > 0:
                logger.info(f"✅ PR REVIEW: Found {len(diff_result)} changes to review")
                
                # Approve and merge (PR merge)
                await self.terminus.checkout(self.test_db, "main")
                merge_result = await self.terminus.merge(self.test_db, pr_branch, "main")
                
                logger.info("✅ PULL REQUEST SUCCESS: PR reviewed and merged")
                self.test_results["pull_request"] = {
                    "tested": True,
                    "success": True,
                    "details": "Successfully simulated PR workflow: branch, review diff, merge"
                }
            else:
                raise Exception("No changes found in PR branch")
                
        except Exception as e:
            logger.error(f"❌ PULL REQUEST FAILED: {e}")
            self.test_results["pull_request"] = {"tested": True, "success": False, "details": str(e)}
    
    async def test_revert_feature(self):
        """Test 7: Revert functionality"""
        logger.info("\n🔍 TEST 7: Testing REVERT functionality")
        try:
            # First check if revert method exists
            if not hasattr(self.terminus, 'revert'):
                logger.warning("⚠️ Revert method not found in AsyncTerminusService")
                self.test_results["revert"] = {
                    "tested": True,
                    "success": False,
                    "details": "Revert method not implemented in current codebase"
                }
                return
            
            # Get commit history
            history = await self.terminus.get_commit_history(self.test_db, limit=10)
            
            if len(history) < 2:
                raise Exception("Not enough commits to test revert")
            
            # Try to revert the second latest commit
            target_commit = history[1]
            commit_id = target_commit.get("id") or target_commit.get("identifier")
            
            logger.info(f"🔄 Attempting to revert commit: {target_commit.get('message')}")
            
            try:
                revert_result = await self.terminus.revert(self.test_db, commit_id)
                logger.info(f"✅ REVERT SUCCESS: {revert_result}")
                self.test_results["revert"] = {
                    "tested": True,
                    "success": True,
                    "details": f"Successfully reverted commit: {target_commit.get('message')}"
                }
            except AttributeError:
                # Revert method doesn't exist
                logger.info("⚠️ REVERT NOT IMPLEMENTED: Using rollback alternative")
                # Try rollback as alternative
                rollback_result = await self.terminus.rollback(self.test_db, commit_id)
                self.test_results["revert"] = {
                    "tested": True,
                    "success": True,
                    "details": f"Rollback used as alternative (created branch): {rollback_result}"
                }
                
        except Exception as e:
            logger.error(f"❌ REVERT FAILED: {e}")
            self.test_results["revert"] = {"tested": True, "success": False, "details": str(e)}
    
    async def cleanup(self):
        """Clean up test database"""
        try:
            await self.terminus.delete_database(self.test_db)
            logger.info(f"🧹 Cleaned up test database: {self.test_db}")
        except Exception as e:
            logger.warning(f"⚠️ Cleanup warning: {e}")
    
    def print_results(self):
        """Print comprehensive test results"""
        print("\n" + "="*80)
        print("🔥 ULTRA GIT FEATURES TEST RESULTS")
        print("="*80)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results.values() if r["success"])
        
        for feature, result in self.test_results.items():
            status = "✅ PASS" if result["success"] else "❌ FAIL"
            tested = "Tested" if result["tested"] else "Not tested"
            print(f"\n{feature.upper()}: {status} ({tested})")
            if result["details"]:
                print(f"  → {result['details']}")
        
        print("\n" + "-"*80)
        print(f"TOTAL: {passed_tests}/{total_tests} features working ({passed_tests/total_tests*100:.1f}%)")
        print("="*80)
        
        # Feature-specific analysis
        print("\n🔍 DETAILED ANALYSIS:")
        
        if self.test_results["commit"]["success"]:
            print("✅ Commit: Implicit commits work correctly with schema changes")
        else:
            print("❌ Commit: Issues with commit creation")
            
        if self.test_results["push"]["success"]:
            print("✅ Push: Branch creation simulates push functionality")
        else:
            print("❌ Push: Branch operations have issues")
            
        if self.test_results["diff"]["success"]:
            print("✅ Diff: Can detect changes between branches")
        else:
            print("❌ Diff: Cannot properly compare branches")
            
        if self.test_results["conflict"]["success"]:
            print("✅ Conflict: Detects or auto-resolves conflicts correctly")
        else:
            print("❌ Conflict: Conflict detection/resolution has issues")
            
        if self.test_results["merge"]["success"]:
            print("✅ Merge: Can merge branches successfully")
        else:
            print("❌ Merge: Merge operations fail")
            
        if self.test_results["pull_request"]["success"]:
            print("✅ Pull Request: Branch + diff + merge workflow works")
        else:
            print("❌ Pull Request: PR workflow has issues")
            
        if self.test_results["revert"]["success"]:
            print("✅ Revert: Can revert/rollback commits")
        else:
            print("❌ Revert: Revert functionality not working")
        
        return passed_tests == total_tests

async def main():
    """Main test runner"""
    tester = GitFeaturesTest()
    
    try:
        # Setup
        if not await tester.setup_test_database():
            print("❌ Failed to setup test database")
            return False
        
        # Run all tests
        await tester.test_commit_feature()
        await tester.test_push_feature()
        await tester.test_diff_feature()
        await tester.test_conflict_detection()
        await tester.test_merge_feature()
        await tester.test_pull_request_feature()
        await tester.test_revert_feature()
        
        # Print results
        all_passed = tester.print_results()
        
        return all_passed
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        await tester.cleanup()

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)