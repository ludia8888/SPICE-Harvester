"""
Unit tests for ProjectionWorker - Cache Stampede Prevention

Tests the critical anti-pattern fix for Cache Stampede/Dog-piling:
- Ensures distributed locking prevents multiple concurrent Elasticsearch queries
- Validates proper cache behavior under high concurrency
- Tests fallback mechanisms and negative caching
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime
from typing import Dict, Any

# Test imports with mocking for missing dependencies
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

# Mock missing modules before importing the actual module
sys.modules['confluent_kafka'] = Mock()
sys.modules['shared.config.service_config'] = Mock()
sys.modules['shared.config.search_config'] = Mock()
sys.modules['shared.config.app_config'] = Mock()
sys.modules['shared.models.events'] = Mock()
sys.modules['shared.services'] = Mock()


class TestProjectionWorkerCacheStampede:
    """Test suite for projection worker cache stampede prevention"""
    
    @pytest.fixture
    def mock_redis_service(self):
        """Create mock Redis service"""
        redis_service = Mock()
        redis_service.client = AsyncMock()
        return redis_service
    
    @pytest.fixture  
    def mock_elasticsearch_service(self):
        """Create mock Elasticsearch service"""
        es_service = AsyncMock()
        return es_service
    
    @pytest.fixture
    def projection_worker(self, mock_redis_service, mock_elasticsearch_service):
        """Create projection worker with mocked services"""
        # Mock the entire projection worker to avoid complex dependencies
        worker = Mock()
        worker.redis_service = mock_redis_service
        worker.elasticsearch_service = mock_elasticsearch_service
        
        # Initialize cache metrics like the real worker
        worker.cache_metrics = {
            'cache_hits': 0,
            'cache_misses': 0,
            'negative_cache_hits': 0,
            'lock_acquisitions': 0,
            'lock_failures': 0,
            'elasticsearch_queries': 0,
            'fallback_queries': 0,
            'total_lock_wait_time': 0.0
        }
        
        # Import the actual method we want to test
        from projection_worker.main import ProjectionWorker
        worker._get_class_label = ProjectionWorker._get_class_label.__get__(worker)
        worker._get_class_label_fallback = ProjectionWorker._get_class_label_fallback.__get__(worker)
        
        return worker
        
    @pytest.mark.asyncio
    async def test_cache_hit_single_request(self, projection_worker, mock_redis_service):
        """Test normal cache hit scenario (no stampede)"""
        class_id = "TestClass"
        db_name = "test_db"
        expected_label = "Test Class Label"
        
        # Mock cache hit
        mock_redis_service.client.get.return_value = expected_label
        
        # Mock AppConfig
        with patch('projection_worker.main.AppConfig') as mock_config:
            mock_config.get_class_label_key.return_value = f"class_label:{db_name}:{class_id}"
            
            result = await projection_worker._get_class_label(class_id, db_name)
        
        assert result == expected_label
        # Verify no lock attempt was made
        mock_redis_service.client.set.assert_not_called()
        
    @pytest.mark.asyncio
    async def test_cache_miss_single_request(self, projection_worker, mock_redis_service, mock_elasticsearch_service):
        """Test cache miss with successful lock acquisition"""
        class_id = "TestClass"
        db_name = "test_db"
        expected_label = "Test Class Label"
        
        # Mock cache miss initially, then cache miss after lock, then ES response
        mock_redis_service.client.get.side_effect = [None, None]  # Cache misses
        mock_redis_service.client.set.return_value = True  # Lock acquired
        
        # Mock Elasticsearch response
        mock_doc = {"label": expected_label}
        mock_elasticsearch_service.get_document.return_value = mock_doc
        
        # Mock AppConfig and search config
        with patch('projection_worker.main.AppConfig') as mock_config, \
             patch('projection_worker.main.get_ontologies_index_name') as mock_index:
            
            mock_config.get_class_label_key.return_value = f"class_label:{db_name}:{class_id}"
            mock_config.CLASS_LABEL_CACHE_TTL = 3600
            mock_index.return_value = f"{db_name}_ontologies"
            
            result = await projection_worker._get_class_label(class_id, db_name)
        
        assert result == expected_label
        
        # Verify lock was acquired and released
        mock_redis_service.client.set.assert_called_once()
        mock_redis_service.client.delete.assert_called_once()
        
        # Verify ES was queried
        mock_elasticsearch_service.get_document.assert_called_once()
        
        # Verify result was cached
        mock_redis_service.client.setex.assert_called()
        
    @pytest.mark.asyncio 
    async def test_cache_stampede_prevention(self, mock_redis_service, mock_elasticsearch_service):
        """
        Critical Test: Verify that concurrent requests don't cause cache stampede
        
        Simulates 10 concurrent requests for the same class_id where:
        - First request acquires lock and queries Elasticsearch
        - Other 9 requests wait and get result from cache
        """
        class_id = "TestClass"
        db_name = "test_db"
        expected_label = "Test Class Label"
        num_concurrent_requests = 10
        
        # Track lock acquisitions and ES queries
        lock_acquisitions = []
        es_queries = []
        
        def mock_lock_acquire(*args, **kwargs):
            """Mock lock acquisition - only first request gets lock"""
            if len(lock_acquisitions) == 0:
                lock_acquisitions.append(1)
                return True
            return False
            
        def mock_get_document(*args, **kwargs):
            """Track ES queries - should only be called once"""
            es_queries.append(1)
            return {"label": expected_label}
        
        # Set up mocks
        mock_redis_service.client.get.side_effect = [None] * 20  # Cache misses initially
        mock_redis_service.client.set.side_effect = mock_lock_acquire
        mock_elasticsearch_service.get_document.side_effect = mock_get_document
        
        # Create multiple worker instances (simulating concurrent workers)
        workers = []
        for _ in range(num_concurrent_requests):
            worker = Mock()
            worker.redis_service = mock_redis_service
            worker.elasticsearch_service = mock_elasticsearch_service
            
            # Initialize cache metrics for each worker
            worker.cache_metrics = {
                'cache_hits': 0,
                'cache_misses': 0,
                'negative_cache_hits': 0,
                'lock_acquisitions': 0,
                'lock_failures': 0,
                'elasticsearch_queries': 0,
                'fallback_queries': 0,
                'total_lock_wait_time': 0.0
            }
            
            from projection_worker.main import ProjectionWorker
            worker._get_class_label = ProjectionWorker._get_class_label.__get__(worker)
            worker._get_class_label_fallback = ProjectionWorker._get_class_label_fallback.__get__(worker)
            workers.append(worker)
        
        # Execute concurrent requests
        with patch('projection_worker.main.AppConfig') as mock_config, \
             patch('projection_worker.main.get_ontologies_index_name') as mock_index, \
             patch('asyncio.sleep'):  # Speed up the test
            
            mock_config.get_class_label_key.return_value = f"class_label:{db_name}:{class_id}"
            mock_config.CLASS_LABEL_CACHE_TTL = 3600
            mock_index.return_value = f"{db_name}_ontologies"
            
            # Simulate the scenario where after first ES query, cache is populated
            async def updated_get_cache(*args):
                if len(es_queries) > 0:  # After first ES query
                    return expected_label
                return None
                
            mock_redis_service.client.get.side_effect = updated_get_cache
            
            tasks = [
                worker._get_class_label(class_id, db_name) 
                for worker in workers
            ]
            
            results = await asyncio.gather(*tasks)
        
        # Critical assertions
        assert len(lock_acquisitions) == 1, f"Expected exactly 1 lock acquisition, got {len(lock_acquisitions)}"
        assert len(es_queries) == 1, f"Expected exactly 1 ES query, got {len(es_queries)} (Cache Stampede occurred!)"
        
        # All requests should get the same result
        assert all(result == expected_label for result in results)
        
    @pytest.mark.asyncio
    async def test_negative_caching(self, projection_worker, mock_redis_service, mock_elasticsearch_service):
        """Test negative caching for non-existent class labels"""
        class_id = "NonExistentClass"
        db_name = "test_db"
        
        # Mock cache miss, lock acquisition, and ES returning None
        mock_redis_service.client.get.side_effect = [None, None]  # Cache misses
        mock_redis_service.client.set.return_value = True  # Lock acquired
        mock_elasticsearch_service.get_document.return_value = None  # No document found
        
        with patch('projection_worker.main.AppConfig') as mock_config, \
             patch('projection_worker.main.get_ontologies_index_name') as mock_index:
            
            mock_config.get_class_label_key.return_value = f"class_label:{db_name}:{class_id}"
            mock_index.return_value = f"{db_name}_ontologies"
            
            result = await projection_worker._get_class_label(class_id, db_name)
        
        assert result is None
        
        # Verify negative caching was applied
        cache_calls = mock_redis_service.client.setex.call_args_list
        negative_cache_call = None
        for call in cache_calls:
            if call[0][2] == "__NONE__":  # Negative cache marker
                negative_cache_call = call
                break
                
        assert negative_cache_call is not None, "Negative caching should be applied"
        assert negative_cache_call[0][1] == 300, "Negative cache TTL should be 5 minutes"
        
    @pytest.mark.asyncio
    async def test_negative_cache_hit(self, projection_worker, mock_redis_service):
        """Test that negative cache hits return None without ES query"""
        class_id = "NonExistentClass"
        db_name = "test_db"
        
        # Mock negative cache hit
        mock_redis_service.client.get.return_value = "__NONE__"
        
        with patch('projection_worker.main.AppConfig') as mock_config:
            mock_config.get_class_label_key.return_value = f"class_label:{db_name}:{class_id}"
            
            result = await projection_worker._get_class_label(class_id, db_name)
        
        assert result is None
        # Verify no lock attempt or ES query was made
        mock_redis_service.client.set.assert_not_called()
        
    @pytest.mark.asyncio
    async def test_lock_timeout_fallback(self, projection_worker, mock_redis_service, mock_elasticsearch_service):
        """Test fallback mechanism when lock acquisition times out"""
        class_id = "TestClass"
        db_name = "test_db"
        expected_label = "Test Class Label"
        
        # Mock persistent cache miss and lock acquisition failure
        mock_redis_service.client.get.return_value = None
        mock_redis_service.client.set.return_value = False  # Lock never acquired
        
        # Mock ES response for fallback
        mock_elasticsearch_service.get_document.return_value = {"label": expected_label}
        
        with patch('projection_worker.main.AppConfig') as mock_config, \
             patch('projection_worker.main.get_ontologies_index_name') as mock_index, \
             patch('asyncio.sleep'):  # Speed up the test
            
            mock_config.get_class_label_key.return_value = f"class_label:{db_name}:{class_id}"
            mock_index.return_value = f"{db_name}_ontologies"
            
            result = await projection_worker._get_class_label(class_id, db_name)
        
        assert result == expected_label
        
        # Verify fallback was called (ES queried without lock)
        mock_elasticsearch_service.get_document.assert_called()
        
    @pytest.mark.asyncio
    async def test_lock_cleanup_on_exception(self, projection_worker, mock_redis_service, mock_elasticsearch_service):
        """Test that locks are properly cleaned up even when exceptions occur"""
        class_id = "TestClass"
        db_name = "test_db"
        
        # Mock cache miss and lock acquisition
        mock_redis_service.client.get.side_effect = [None, None]
        mock_redis_service.client.set.return_value = True  # Lock acquired
        
        # Mock ES to raise exception
        mock_elasticsearch_service.get_document.side_effect = Exception("ES Error")
        
        with patch('projection_worker.main.AppConfig') as mock_config, \
             patch('projection_worker.main.get_ontologies_index_name') as mock_index:
            
            mock_config.get_class_label_key.return_value = f"class_label:{db_name}:{class_id}"
            mock_index.return_value = f"{db_name}_ontologies"
            
            result = await projection_worker._get_class_label(class_id, db_name)
        
        # Should return None due to exception handling
        assert result is None
        
        # Critical: Verify lock was released despite exception
        mock_redis_service.client.delete.assert_called_once()
        

if __name__ == "__main__":
    pytest.main([__file__, "-v"])