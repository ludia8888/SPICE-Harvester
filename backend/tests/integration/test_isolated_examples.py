"""
Example tests demonstrating test isolation mechanisms
"""

import pytest
import asyncio
from datetime import datetime

from tests.utils.test_isolation import (
    TestIsolationManager, 
    TestDataBuilder,
    with_isolation,
    isolated_test_context
)
from tests.test_config import TestConfig


class TestIsolationExamples:
    """Examples of how to use test isolation"""
    
    def test_basic_isolation(self, isolated_test_db):
        """Basic test using isolated database fixture"""
        # isolated_test_db is automatically created and cleaned up
        assert isolated_test_db.startswith("test_db_")
        print(f"Using isolated database: {isolated_test_db}")
    
    def test_unique_names(self, unique_test_id):
        """Test that unique IDs are generated"""
        id1 = unique_test_id
        # Sleep to ensure different timestamp
        import time
        time.sleep(0.01)
        
        isolation_manager = TestIsolationManager()
        id2 = isolation_manager.generate_isolated_name("test")
        
        assert id1 != id2
        print(f"ID 1: {id1}")
        print(f"ID 2: {id2}")
    
    @with_isolation
    def test_with_isolation_decorator(self):
        """Test using the isolation decorator"""
        isolation_manager = TestIsolationManager()
        
        # Create test resource
        db_name = isolation_manager.generate_isolated_name("decorator_test")
        isolation_manager.register_database(db_name)
        
        # Database will be automatically cleaned up after test
        assert db_name.startswith("decorator_test_")
    
    def test_context_manager_isolation(self):
        """Test using context manager for isolation"""
        with isolated_test_context("context_test") as (db_name, isolation_manager):
            # Inside context, we have an isolated database
            assert db_name.startswith("context_test_")
            
            # Register additional resources
            isolation_manager.register_resource("test_file", "/tmp/test.txt")
            
            # Everything is cleaned up when context exits
    
    def test_environment_isolation(self, reset_environment):
        """Test environment variable isolation"""
        import os
        
        # Set test environment variable
        os.environ["TEST_VAR"] = "test_value"
        assert os.environ.get("TEST_VAR") == "test_value"
        
        # After test, environment is automatically reset
    
    def test_file_cleanup(self, cleanup_files, tmp_path):
        """Test file cleanup mechanism"""
        # Create test file
        test_file = tmp_path / "test_output.json"
        test_file.write_text('{"test": "data"}')
        
        # Register for cleanup
        cleanup_files.add_cleanup_file(str(test_file))
        
        assert test_file.exists()
        # File will be cleaned up after test
    
    @pytest.mark.asyncio
    async def test_async_isolation(self, async_isolated_test_db):
        """Test async isolation with database"""
        # async_isolated_test_db is automatically created and cleaned up
        assert async_isolated_test_db.startswith("test_db_")
        
        # Can use the database for async operations
        import httpx
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{TestConfig.get_oms_base_url()}/api/v1/database/{async_isolated_test_db}"
            )
            assert response.status_code == 200
    
    def test_data_builder_isolation(self):
        """Test isolated data builder"""
        isolation_manager = TestIsolationManager()
        data_builder = TestDataBuilder(isolation_manager)
        
        # Create isolated test data
        ontology = data_builder.create_test_ontology("test_db", "product")
        
        # Check that IDs are unique
        class_id = ontology["@graph"][0]["@id"]
        assert "product_" in class_id
        assert len(class_id.split("_")) >= 3  # Has timestamp and UUID
        
        # Create another one
        ontology2 = data_builder.create_test_ontology("test_db", "product")
        class_id2 = ontology2["@graph"][0]["@id"]
        
        assert class_id != class_id2
    
    def test_parallel_isolation(self):
        """Test that parallel tests don't interfere"""
        import concurrent.futures
        
        def create_isolated_resource(index):
            isolation_manager = TestIsolationManager()
            name = isolation_manager.generate_isolated_name(f"parallel_{index}")
            return name
        
        # Run multiple isolated operations in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(create_isolated_resource, i) for i in range(10)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        # All names should be unique
        assert len(set(results)) == 10
        print(f"Generated {len(results)} unique isolated names")
    
    def test_cleanup_callback(self):
        """Test cleanup callback mechanism"""
        isolation_manager = TestIsolationManager()
        
        cleanup_called = False
        
        def cleanup_callback():
            nonlocal cleanup_called
            cleanup_called = True
            print("Cleanup callback executed")
        
        isolation_manager.add_cleanup_callback(cleanup_callback)
        isolation_manager.cleanup_all()
        
        assert cleanup_called
    
    def test_service_availability_check(self):
        """Test service availability checking"""
        isolation_manager = TestIsolationManager()
        
        # Check if services are running
        services = isolation_manager.ensure_services_running()
        
        print("Service status:")
        for name, is_running in services.items():
            status = "✅ Running" if is_running else "❌ Not running"
            print(f"  {name}: {status}")
        
        # At least OMS should be running for tests
        assert services.get("OMS", False), "OMS service must be running for tests"


class TestIsolationIntegration:
    """Integration tests using isolation"""
    
    @pytest.mark.asyncio
    async def test_isolated_ontology_creation(self, async_isolated_test_db):
        """Test creating ontology in isolated database"""
        import httpx
        
        data_builder = TestDataBuilder()
        ontology_data = data_builder.create_test_ontology(
            async_isolated_test_db, 
            "isolated_class"
        )
        
        async with httpx.AsyncClient() as client:
            # Create ontology
            response = await client.post(
                f"{TestConfig.get_oms_base_url()}/api/v1/database/{async_isolated_test_db}/ontology",
                json=ontology_data
            )
            
            assert response.status_code == 200
            
            # Verify it was created
            response = await client.get(
                f"{TestConfig.get_oms_base_url()}/api/v1/database/{async_isolated_test_db}/ontology"
            )
            
            assert response.status_code == 200
            data = response.json()
            
            # Check that our isolated class exists
            assert any("isolated_class_" in str(item) for item in data["data"])
    
    def test_concurrent_database_operations(self):
        """Test that concurrent tests with different databases don't interfere"""
        import concurrent.futures
        import requests
        
        def create_and_test_database(index):
            isolation_manager = TestIsolationManager()
            db_name = isolation_manager.generate_isolated_name(f"concurrent_{index}")
            
            try:
                # Create database
                response = requests.post(
                    f"{TestConfig.get_oms_base_url()}/api/v1/database/create",
                    json={
                        "name": db_name,
                        "description": f"Concurrent test {index}"
                    }
                )
                
                if response.status_code != 200:
                    return False, f"Failed to create {db_name}"
                
                # Register for cleanup
                isolation_manager.register_database(db_name)
                
                # Verify it exists
                response = requests.get(
                    f"{TestConfig.get_oms_base_url()}/api/v1/database/{db_name}"
                )
                
                success = response.status_code == 200
                
                # Cleanup
                isolation_manager.cleanup_database(db_name)
                
                return success, db_name
                
            except Exception as e:
                return False, str(e)
        
        # Run concurrent database operations
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(create_and_test_database, i) for i in range(5)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        # All operations should succeed
        successes = [r[0] for r in results]
        assert all(successes), f"Some operations failed: {results}"
        
        print(f"Successfully ran {len(results)} concurrent isolated database operations")