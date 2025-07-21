"""
Shared test configuration and fixtures
"""

import pytest
import asyncio
import uuid
import os
import sys
from pathlib import Path
from typing import Generator, AsyncGenerator
import requests
from datetime import datetime

# Add backend directory to Python path for OMS imports
backend_dir = Path(__file__).parent.parent.absolute()
backend_str = str(backend_dir)
if backend_str not in sys.path:
    sys.path.insert(0, backend_str)
from tests.test_config import TestConfig

@pytest.fixture
def sample_branch_data():
    """Sample data for branch operations"""
    return {
        "branch_name": "feature-test",
        "from_commit": "main"
    }

@pytest.fixture
def sample_commit_data():
    """Sample data for commit operations"""
    return {
        "message": "Test commit message",
        "author": "test@example.com",
        "branch": "feature-test"
    }

@pytest.fixture
def sample_merge_data():
    """Sample data for merge operations"""
    return {
        "source_branch": "feature-test",
        "target_branch": "main",
        "strategy": "merge",
        "message": "Merge feature-test into main",
        "author": "test@example.com"
    }

@pytest.fixture
def sample_rollback_data():
    """Sample data for rollback operations"""
    return {
        "target_commit": "abc123def456",
        "create_branch": True,
        "branch_name": "rollback-test"
    }

@pytest.fixture
def sample_database_data():
    """Sample data for database creation"""
    return {
        "name": "test_database",
        "description": "Test database for unit tests"
    }


# Test Isolation Fixtures
@pytest.fixture(scope='function')
def unique_test_id():
    """Generate unique ID for test isolation"""
    return f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope='function')
def isolated_test_db_name(unique_test_id):
    """Generate isolated database name for each test"""
    return f"test_db_{unique_test_id}"


@pytest.fixture(scope='function', autouse=True)
def cleanup_test_databases(request):
    """Automatically cleanup test databases after each test"""
    created_databases = []
    
    def track_database(db_name: str):
        created_databases.append(db_name)
    
    request.addfinalizer(lambda: _cleanup_databases(created_databases))
    request.track_database = track_database
    
    yield
    

def _cleanup_databases(databases: list):
    """Cleanup helper function to delete test databases"""
    oms_url = TestConfig.get_oms_base_url()
    
    for db_name in databases:
        try:
            response = requests.delete(
                f"{oms_url}/api/v1/database/{db_name}",
                timeout=5
            )
            if response.status_code == 200:
                print(f"✅ Cleaned up test database: {db_name}")
            elif response.status_code == 404:
                print(f"⚠️  Test database already deleted: {db_name}")
            else:
                print(f"❌ Failed to cleanup database {db_name}: {response.status_code}")
        except Exception as e:
            print(f"❌ Error cleaning up database {db_name}: {str(e)}")


@pytest.fixture(scope='function')
async def async_isolated_test_db(isolated_test_db_name, request):
    """Create and cleanup isolated test database for async tests"""
    import httpx
    
    oms_url = TestConfig.get_oms_base_url()
    
    # Create test database
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{oms_url}/api/v1/database/create",
            json={
                "name": isolated_test_db_name,
                "description": f"Isolated test database created at {datetime.now()}"
            }
        )
        
        if response.status_code == 200:
            print(f"✅ Created isolated test database: {isolated_test_db_name}")
            if hasattr(request, 'track_database'):
                request.track_database(isolated_test_db_name)
        else:
            raise Exception(f"Failed to create test database: {response.text}")
    
    yield isolated_test_db_name
    
    # Cleanup is handled by cleanup_test_databases fixture


@pytest.fixture(scope='function')
def isolated_test_db(isolated_test_db_name, request):
    """Create and cleanup isolated test database for sync tests"""
    oms_url = TestConfig.get_oms_base_url()
    
    # Create test database
    response = requests.post(
        f"{oms_url}/api/v1/database/create",
        json={
            "name": isolated_test_db_name,
            "description": f"Isolated test database created at {datetime.now()}"
        }
    )
    
    if response.status_code == 200:
        print(f"✅ Created isolated test database: {isolated_test_db_name}")
        if hasattr(request, 'track_database'):
            request.track_database(isolated_test_db_name)
    else:
        raise Exception(f"Failed to create test database: {response.text}")
    
    yield isolated_test_db_name
    
    # Cleanup is handled by cleanup_test_databases fixture


@pytest.fixture(scope='session')
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def reset_environment():
    """Reset environment variables for each test"""
    original_env = os.environ.copy()
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_terminus_client(mocker):
    """Mock TerminusDB client for unit tests"""
    mock_client = mocker.Mock()
    mock_client.connect.return_value = True
    mock_client.get_database.return_value = {"name": "test_db"}
    return mock_client


@pytest.fixture
def test_data_dir():
    """Get test data directory"""
    return Path(__file__).parent / "test_data"


@pytest.fixture
def cleanup_files(request):
    """Cleanup files created during tests"""
    files_to_cleanup = []
    
    def add_file(filepath):
        files_to_cleanup.append(filepath)
    
    request.add_cleanup_file = add_file
    
    yield
    
    for filepath in files_to_cleanup:
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"✅ Cleaned up test file: {filepath}")
        except Exception as e:
            print(f"❌ Error cleaning up file {filepath}: {str(e)}")


# HTTP Client Fixtures for Integration Tests
@pytest.fixture
async def async_http_client():
    """Async HTTP client for integration tests"""
    import httpx
    async with httpx.AsyncClient(timeout=30.0) as client:
        yield client


@pytest.fixture
def http_client():
    """Sync HTTP client for integration tests"""
    import httpx
    with httpx.Client(timeout=30.0) as client:
        yield client


# Service URL fixtures
@pytest.fixture
def bff_base_url():
    """Base URL for BFF service"""
    return TestConfig.get_bff_base_url()


@pytest.fixture
def oms_base_url():
    """Base URL for OMS service"""
    return TestConfig.get_oms_base_url()


@pytest.fixture
def funnel_base_url():
    """Base URL for Funnel service"""
    return TestConfig.get_funnel_base_url()