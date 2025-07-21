"""
Test isolation utilities for SPICE HARVESTER tests
Ensures each test runs in complete isolation
"""

import os
import uuid
import time
import threading
from contextlib import contextmanager
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests
import logging

from tests.test_config import TestConfig

logger = logging.getLogger(__name__)


class TestIsolationManager:
    """
    Manages test isolation to ensure tests don't interfere with each other
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        self._active_databases: Dict[str, datetime] = {}
        self._active_resources: Dict[str, Any] = {}
        self._cleanup_callbacks: List[callable] = []
        self._resource_lock = threading.Lock()
    
    def generate_isolated_name(self, prefix: str = "test") -> str:
        """Generate unique isolated name for test resources"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:8]
        return f"{prefix}_{timestamp}_{unique_id}"
    
    def register_database(self, db_name: str) -> None:
        """Register a database for cleanup"""
        with self._resource_lock:
            self._active_databases[db_name] = datetime.now()
            logger.info(f"Registered database for cleanup: {db_name}")
    
    def register_resource(self, resource_id: str, resource_data: Any) -> None:
        """Register any resource for cleanup"""
        with self._resource_lock:
            self._active_resources[resource_id] = resource_data
            logger.info(f"Registered resource for cleanup: {resource_id}")
    
    def add_cleanup_callback(self, callback: callable) -> None:
        """Add a cleanup callback to be executed"""
        with self._resource_lock:
            self._cleanup_callbacks.append(callback)
    
    def cleanup_database(self, db_name: str) -> bool:
        """Cleanup a specific database"""
        try:
            oms_url = TestConfig.get_oms_base_url()
            response = requests.delete(
                f"{oms_url}/api/v1/database/{db_name}",
                timeout=10
            )
            
            if response.status_code in [200, 204]:
                logger.info(f"Successfully cleaned up database: {db_name}")
                return True
            elif response.status_code == 404:
                logger.warning(f"Database already deleted: {db_name}")
                return True
            else:
                logger.error(f"Failed to cleanup database {db_name}: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error cleaning up database {db_name}: {str(e)}")
            return False
    
    def cleanup_all(self) -> None:
        """Cleanup all registered resources"""
        with self._resource_lock:
            # Cleanup databases
            for db_name in list(self._active_databases.keys()):
                if self.cleanup_database(db_name):
                    del self._active_databases[db_name]
            
            # Execute cleanup callbacks
            for callback in self._cleanup_callbacks:
                try:
                    callback()
                except Exception as e:
                    logger.error(f"Error executing cleanup callback: {str(e)}")
            
            self._cleanup_callbacks.clear()
            self._active_resources.clear()
    
    @contextmanager
    def isolated_database(self, prefix: str = "test_db"):
        """Context manager for isolated database"""
        db_name = self.generate_isolated_name(prefix)
        
        try:
            # Create database
            oms_url = TestConfig.get_oms_base_url()
            response = requests.post(
                f"{oms_url}/api/v1/database/create",
                json={
                    "name": db_name,
                    "description": f"Isolated test database created at {datetime.now()}"
                },
                timeout=10
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to create test database: {response.text}")
            
            self.register_database(db_name)
            logger.info(f"Created isolated database: {db_name}")
            
            yield db_name
            
        finally:
            # Cleanup
            self.cleanup_database(db_name)
    
    @contextmanager
    def isolated_environment(self, env_vars: Optional[Dict[str, str]] = None):
        """Context manager for isolated environment variables"""
        original_env = os.environ.copy()
        
        try:
            if env_vars:
                os.environ.update(env_vars)
            
            yield
            
        finally:
            # Restore original environment
            os.environ.clear()
            os.environ.update(original_env)
    
    def wait_for_service(self, service_url: str, timeout: int = 30) -> bool:
        """Wait for a service to be available"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{service_url}/health", timeout=2)
                if response.status_code == 200:
                    return True
            except (requests.RequestException, ConnectionError, TimeoutError) as e:
                logger.debug(f"Service health check failed for {service_url}: {e}")
            
            time.sleep(1)
        
        return False
    
    def ensure_services_running(self) -> Dict[str, bool]:
        """Ensure required services are running"""
        services = {
            "OMS": TestConfig.get_oms_base_url(),
            "BFF": TestConfig.get_bff_base_url()
        }
        
        results = {}
        for name, url in services.items():
            results[name] = self.wait_for_service(url, timeout=5)
            
        return results


class TestDataBuilder:
    """Builder for creating isolated test data"""
    
    def __init__(self, isolation_manager: Optional[TestIsolationManager] = None):
        self.isolation_manager = isolation_manager or TestIsolationManager()
    
    def create_test_ontology(self, db_name: str, name_prefix: str = "test_class") -> Dict[str, Any]:
        """Create test ontology with isolation"""
        class_name = self.isolation_manager.generate_isolated_name(name_prefix)
        
        return {
            "@context": {
                "@type": "@json"
            },
            "@graph": [{
                "@type": "rdf:Class",
                "@id": f"Category/{class_name}",
                "rdfs:label": {
                    "ko": f"{class_name} 테스트",
                    "en": f"{class_name} Test"
                },
                "rdfs:comment": {
                    "ko": f"격리된 테스트 클래스 {class_name}",
                    "en": f"Isolated test class {class_name}"
                },
                "properties": [
                    {
                        "@id": f"Property/{class_name}_name",
                        "@type": "rdf:Property",
                        "rdfs:label": {"ko": "이름", "en": "Name"},
                        "rdfs:range": {"@id": "xsd:string"}
                    }
                ]
            }]
        }
    
    def create_test_property(self, class_id: str, property_prefix: str = "test_prop") -> Dict[str, Any]:
        """Create test property with isolation"""
        prop_name = self.isolation_manager.generate_isolated_name(property_prefix)
        
        return {
            "@type": "rdf:Property",
            "@id": f"Property/{prop_name}",
            "rdfs:label": {
                "ko": f"{prop_name} 속성",
                "en": f"{prop_name} Property"
            },
            "rdfs:domain": {"@id": class_id},
            "rdfs:range": {"@id": "xsd:string"}
        }
    
    def create_test_instance(self, class_id: str, instance_prefix: str = "test_inst") -> Dict[str, Any]:
        """Create test instance with isolation"""
        instance_name = self.isolation_manager.generate_isolated_name(instance_prefix)
        
        return {
            "@type": class_id,
            "@id": f"Instance/{instance_name}",
            "rdfs:label": {
                "ko": f"{instance_name} 인스턴스",
                "en": f"{instance_name} Instance"
            }
        }


# Global instance for easy access
test_isolation = TestIsolationManager()


def with_isolation(test_func):
    """Decorator to ensure test isolation"""
    def wrapper(*args, **kwargs):
        isolation_manager = TestIsolationManager()
        
        try:
            result = test_func(*args, **kwargs)
            return result
        finally:
            # Cleanup after test
            isolation_manager.cleanup_all()
    
    return wrapper


@contextmanager
def isolated_test_context(db_prefix: str = "test_db", env_vars: Optional[Dict[str, str]] = None):
    """Combined context manager for complete test isolation"""
    isolation_manager = TestIsolationManager()
    
    with isolation_manager.isolated_environment(env_vars):
        with isolation_manager.isolated_database(db_prefix) as db_name:
            yield db_name, isolation_manager