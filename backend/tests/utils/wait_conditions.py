"""
Wait conditions and backoff polling utilities for robust E2E testing.

Replaces hard sleeps with conditional waiting to eliminate flakiness.
"""

import asyncio
import time
import logging
from typing import Callable, Awaitable, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class WaitConfig:
    """Configuration for wait conditions"""
    timeout: float = 30.0
    initial_interval: float = 0.5
    max_interval: float = 2.0
    backoff_factor: float = 1.2
    

async def wait_until(
    predicate: Callable[[], Awaitable[bool]], 
    config: Optional[WaitConfig] = None,
    description: str = "condition"
) -> bool:
    """
    Wait until a condition is met using exponential backoff.
    
    Args:
        predicate: Async function that returns True when condition is met
        config: Wait configuration (timeout, intervals, backoff)
        description: Description for logging
        
    Returns:
        True if condition was met, False if timeout
        
    Example:
        # Wait for database to exist
        async def db_exists():
            response = await client.get("/api/v1/databases")
            databases = response.json()
            return "test_db" in [db.get("name") for db in databases.get("data", {}).get("databases", [])]
            
        success = await wait_until(db_exists, WaitConfig(timeout=60), "database creation")
    """
    if config is None:
        config = WaitConfig()
    
    start_time = time.time()
    interval = config.initial_interval
    attempt = 1
    
    logger.debug(f"⏳ Waiting for {description} (timeout: {config.timeout}s)")
    
    while time.time() - start_time < config.timeout:
        try:
            if await predicate():
                elapsed = time.time() - start_time
                logger.info(f"✅ {description} met after {elapsed:.2f}s (attempt {attempt})")
                return True
        except Exception as e:
            # Log but continue waiting - temporary failures are expected
            logger.debug(f"⚠️ {description} check failed (attempt {attempt}): {e}")
        
        await asyncio.sleep(interval)
        interval = min(interval * config.backoff_factor, config.max_interval)
        attempt += 1
    
    elapsed = time.time() - start_time
    logger.warning(f"❌ {description} timeout after {elapsed:.2f}s ({attempt-1} attempts)")
    return False


async def wait_for_event_sourcing_propagation(
    check_function: Callable[[], Awaitable[bool]],
    resource_name: str,
    timeout: float = 30.0
) -> bool:
    """
    Wait for Event Sourcing propagation to complete.
    
    Args:
        check_function: Function to check if propagation is complete
        resource_name: Name of resource for logging
        timeout: Maximum wait time
    """
    config = WaitConfig(
        timeout=timeout,
        initial_interval=0.5,
        max_interval=1.5,
        backoff_factor=1.3
    )
    
    return await wait_until(
        check_function,
        config,
        f"Event Sourcing propagation for {resource_name}"
    )


async def wait_for_elasticsearch_index(
    es_client,
    index_name: str,
    expected_docs: Optional[int] = None,
    timeout: float = 20.0
) -> bool:
    """
    Wait for Elasticsearch index to exist and optionally have expected document count.
    
    Args:
        es_client: AsyncElasticsearch client
        index_name: Name of the index to wait for
        expected_docs: Expected number of documents (None to just check existence)
        timeout: Maximum wait time
    """
    async def index_ready():
        try:
            # Check if index exists
            exists = await es_client.indices.exists(index=index_name)
            if not exists:
                return False
            
            # If document count specified, check that too
            if expected_docs is not None:
                response = await es_client.count(index=index_name)
                doc_count = response.get('count', 0)
                return doc_count >= expected_docs
            
            return True
        except Exception as e:
            logger.debug(f"ES index check failed: {e}")
            return False
    
    config = WaitConfig(timeout=timeout, initial_interval=1.0)
    description = f"Elasticsearch index {index_name}"
    if expected_docs:
        description += f" with {expected_docs} docs"
    
    return await wait_until(index_ready, config, description)


async def wait_for_background_task_completion(
    client,
    task_id: str,
    base_url: str,
    timeout: float = 120.0
) -> tuple[bool, Optional[dict]]:
    """
    Wait for background task to complete.
    
    Args:
        client: HTTP client
        task_id: Task ID to monitor
        base_url: Base URL for task status endpoint
        timeout: Maximum wait time
        
    Returns:
        (success: bool, final_status: Optional[dict])
    """
    final_status = None
    
    async def task_completed():
        nonlocal final_status
        try:
            response = await client.get(f"{base_url}/api/v1/tasks/{task_id}/status")
            if response.status_code != 200:
                return False
            
            final_status = response.json()
            status = final_status.get("status", "").lower()
            
            # Task is complete if it's in a final state
            return status in ["completed", "failed", "cancelled"]
        except Exception as e:
            logger.debug(f"Task status check failed: {e}")
            return False
    
    config = WaitConfig(timeout=timeout, initial_interval=2.0)
    success = await wait_until(
        task_completed, 
        config, 
        f"background task {task_id}"
    )
    
    return success, final_status


async def wait_for_service_health(
    client,
    service_url: str,
    service_name: str,
    timeout: float = 30.0
) -> bool:
    """
    Wait for service to be healthy.
    
    Args:
        client: HTTP client
        service_url: Service health check URL
        service_name: Service name for logging
        timeout: Maximum wait time
    """
    async def service_healthy():
        try:
            response = await client.get(f"{service_url}/health", timeout=5.0)
            return response.status_code == 200
        except Exception:
            return False
    
    config = WaitConfig(timeout=timeout, initial_interval=1.0)
    return await wait_until(
        service_healthy, 
        config, 
        f"{service_name} health check"
    )


async def wait_for_database_operation(
    client,
    check_url: str,
    database_name: str,
    operation: str,  # "creation" or "deletion"
    timeout: float = 30.0
) -> bool:
    """
    Wait for database operation to propagate across services.
    
    Args:
        client: HTTP client
        check_url: URL to check database existence
        database_name: Name of database
        operation: Type of operation ("creation" or "deletion")
        timeout: Maximum wait time
    """
    async def operation_complete():
        try:
            response = await client.get(check_url, timeout=10.0)
            if response.status_code != 200:
                return False
            
            data = response.json()
            # Handle different response formats
            databases = []
            if isinstance(data, dict):
                if "data" in data and isinstance(data["data"], dict):
                    databases = data["data"].get("databases", [])
                elif "databases" in data:
                    databases = data["databases"]
                elif isinstance(data, list):
                    databases = data
            elif isinstance(data, list):
                databases = data
            
            # Extract database names from various formats
            db_names = []
            for db in databases:
                if isinstance(db, dict):
                    name = db.get("name") or db.get("database") or db.get("id")
                    if name:
                        db_names.append(name)
                elif isinstance(db, str):
                    db_names.append(db)
            
            if operation == "creation":
                return database_name in db_names
            elif operation == "deletion":
                return database_name not in db_names
            else:
                return False
                
        except Exception as e:
            logger.debug(f"Database {operation} check failed: {e}")
            return False
    
    config = WaitConfig(timeout=timeout)
    return await wait_until(
        operation_complete,
        config,
        f"database {database_name} {operation}"
    )