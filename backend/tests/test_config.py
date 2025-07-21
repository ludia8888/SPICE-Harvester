"""
Test configuration settings for SPICE HARVESTER tests
Contains URLs, settings, and helper methods for test execution
"""

import os
from typing import Optional
from shared.config.service_config import ServiceConfig


class TestConfig:
    """Test configuration class for SPICE HARVESTER"""
    
    # Default test database prefix
    DEFAULT_TEST_DB_PREFIX = "test_"
    
    @classmethod
    def get_oms_base_url(cls) -> str:
        """Get OMS service base URL"""
        # First check environment variable for test overrides
        test_url = os.getenv("TEST_OMS_URL")
        if test_url:
            return test_url
        # Otherwise use ServiceConfig
        return os.getenv("OMS_BASE_URL", ServiceConfig.get_oms_url())
    
    @classmethod
    def get_bff_base_url(cls) -> str:
        """Get BFF service base URL"""
        # First check environment variable for test overrides
        test_url = os.getenv("TEST_BFF_URL")
        if test_url:
            return test_url
        # Otherwise use ServiceConfig
        return os.getenv("BFF_BASE_URL", ServiceConfig.get_bff_url())
    
    @classmethod
    def get_funnel_base_url(cls) -> str:
        """Get Funnel service base URL"""
        # First check environment variable for test overrides
        test_url = os.getenv("TEST_FUNNEL_URL")
        if test_url:
            return test_url
        # Otherwise use ServiceConfig
        return os.getenv("FUNNEL_BASE_URL", ServiceConfig.get_funnel_url())
    
    @classmethod
    def get_test_db_prefix(cls) -> str:
        """Get test database prefix"""
        return os.getenv("TEST_DB_PREFIX", cls.DEFAULT_TEST_DB_PREFIX)
    
    @classmethod
    def get_database_create_url(cls) -> str:
        """Get database creation URL"""
        return f"{cls.get_oms_base_url()}/api/v1/database/create"
    
    @classmethod
    def get_database_delete_url(cls, db_name: str) -> str:
        """Get database deletion URL"""
        return f"{cls.get_oms_base_url()}/api/v1/database/{db_name}"
    
    @classmethod
    def get_oms_ontology_url(cls, db_name: str, path: str = "") -> str:
        """Get OMS ontology URL"""
        base_url = f"{cls.get_oms_base_url()}/api/v1/database/{db_name}/ontology"
        if path:
            return f"{base_url}{path}"
        return base_url
    
    @classmethod
    def get_bff_ontology_url(cls, db_name: str, path: str = "") -> str:
        """Get BFF ontology URL"""
        base_url = f"{cls.get_bff_base_url()}/database/{db_name}/ontology"
        if path:
            return f"{base_url}{path}"
        return base_url
    
    @classmethod
    def get_oms_branch_url(cls, db_name: str, path: str = "") -> str:
        """Get OMS branch URL"""
        base_url = f"{cls.get_oms_base_url()}/api/v1/branch/{db_name}"
        if path:
            return f"{base_url}{path}"
        return base_url
    
    @classmethod
    def get_bff_branch_url(cls, db_name: str, path: str = "") -> str:
        """Get BFF branch URL"""
        base_url = f"{cls.get_bff_base_url()}/database/{db_name}/branch"
        if path:
            return f"{base_url}{path}"
        return base_url
    
    @classmethod
    def get_oms_version_url(cls, db_name: str, path: str = "") -> str:
        """Get OMS version URL"""
        base_url = f"{cls.get_oms_base_url()}/api/v1/version/{db_name}"
        if path:
            return f"{base_url}{path}"
        return base_url
    
    @classmethod
    def get_bff_version_url(cls, db_name: str, path: str = "") -> str:
        """Get BFF version URL"""
        base_url = f"{cls.get_bff_base_url()}/database/{db_name}/version"
        if path:
            return f"{base_url}{path}"
        return base_url
    
    @classmethod
    def get_test_timeout(cls) -> int:
        """Get test timeout in seconds"""
        return int(os.getenv("TEST_TIMEOUT", "30"))
    
    @classmethod
    def get_performance_test_timeout(cls) -> int:
        """Get performance test timeout in seconds"""
        return int(os.getenv("PERFORMANCE_TEST_TIMEOUT", "60"))
    
    @classmethod
    def get_max_concurrent_requests(cls) -> int:
        """Get maximum concurrent requests for load testing"""
        return int(os.getenv("MAX_CONCURRENT_REQUESTS", "50"))
    
    @classmethod
    def is_debug_mode(cls) -> bool:
        """Check if debug mode is enabled"""
        return os.getenv("DEBUG", "false").lower() == "true"
    
    @classmethod
    def get_log_level(cls) -> str:
        """Get log level for tests"""
        return os.getenv("LOG_LEVEL", "INFO").upper()