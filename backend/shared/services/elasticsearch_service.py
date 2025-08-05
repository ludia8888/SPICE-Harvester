"""
Elasticsearch Client Service

Provides a centralized Elasticsearch client with connection pooling,
error handling, and common operations for search and indexing.
"""

import json
import logging
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import (
    ElasticsearchException,
    NotFoundError,
    RequestError,
    ConnectionError as ESConnectionError
)
from elasticsearch.helpers import async_bulk

logger = logging.getLogger(__name__)


class ElasticsearchService:
    """
    Async Elasticsearch client service with connection pooling and error handling.
    
    Features:
    - Connection pooling for performance
    - Automatic reconnection
    - JSON document support
    - Index management
    - Bulk operations support
    - Search and aggregations
    - Alias management with filters
    """
    
    def __init__(
        self,
        host: str = "elasticsearch",
        port: int = 9200,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_ssl: bool = False,
        verify_certs: bool = False,
        request_timeout: int = 30,
        max_retries: int = 3,
        retry_on_timeout: bool = True
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        
        # Build hosts configuration
        hosts = [{"host": host, "port": port, "scheme": "https" if use_ssl else "http"}]
        
        # Build client configuration
        self.config = {
            "hosts": hosts,
            "verify_certs": verify_certs,
            "request_timeout": request_timeout,
            "max_retries": max_retries,
            "retry_on_timeout": retry_on_timeout
        }
        
        # Add authentication if provided
        if username and password:
            self.config["basic_auth"] = (username, password)
            
        self._client: Optional[AsyncElasticsearch] = None
        
    async def connect(self) -> None:
        """Initialize Elasticsearch connection."""
        try:
            self._client = AsyncElasticsearch(**self.config)
            # Test connection
            info = await self._client.info()
            logger.info(
                f"Connected to Elasticsearch {info['version']['number']} "
                f"at {self.host}:{self.port}"
            )
        except ESConnectionError as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            raise
            
    async def disconnect(self) -> None:
        """Close Elasticsearch connection."""
        try:
            if self._client:
                await self._client.close()
                logger.info("Disconnected from Elasticsearch")
        except Exception as e:
            logger.error(f"Error disconnecting from Elasticsearch: {e}")
            
    @property
    def client(self) -> AsyncElasticsearch:
        """Get Elasticsearch client instance."""
        if not self._client:
            raise RuntimeError("Elasticsearch client not connected. Call connect() first.")
        return self._client
        
    # Index Operations
    
    async def create_index(
        self,
        index: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
        aliases: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create an index with optional mappings, settings, and aliases.
        
        Args:
            index: Index name
            mappings: Index mappings
            settings: Index settings
            aliases: Index aliases
            
        Returns:
            Success status
        """
        try:
            body = {}
            if mappings:
                body["mappings"] = mappings
            if settings:
                body["settings"] = settings
            if aliases:
                body["aliases"] = aliases
                
            await self.client.indices.create(index=index, body=body)
            logger.info(f"Created index: {index}")
            return True
        except RequestError as e:
            if e.error == "resource_already_exists_exception":
                logger.info(f"Index {index} already exists")
                return True
            logger.error(f"Failed to create index {index}: {e}")
            raise
            
    async def delete_index(self, index: str) -> bool:
        """
        Delete an index.
        
        Args:
            index: Index name
            
        Returns:
            Success status
        """
        try:
            await self.client.indices.delete(index=index)
            logger.info(f"Deleted index: {index}")
            return True
        except NotFoundError:
            logger.warning(f"Index {index} not found")
            return False
        except ElasticsearchException as e:
            logger.error(f"Failed to delete index {index}: {e}")
            raise
            
    async def index_exists(self, index: str) -> bool:
        """Check if index exists."""
        try:
            return await self.client.indices.exists(index=index)
        except ElasticsearchException as e:
            logger.error(f"Failed to check index existence: {e}")
            return False
            
    async def update_mapping(
        self,
        index: str,
        properties: Dict[str, Any]
    ) -> bool:
        """
        Update index mapping.
        
        Args:
            index: Index name
            properties: Mapping properties to add/update
            
        Returns:
            Success status
        """
        try:
            await self.client.indices.put_mapping(
                index=index,
                body={"properties": properties}
            )
            logger.info(f"Updated mapping for index: {index}")
            return True
        except ElasticsearchException as e:
            logger.error(f"Failed to update mapping: {e}")
            raise
            
    # Document Operations
    
    async def index_document(
        self,
        index: str,
        document: Dict[str, Any],
        doc_id: Optional[str] = None,
        refresh: Union[bool, str] = False
    ) -> str:
        """
        Index a single document.
        
        Args:
            index: Index name
            document: Document to index
            doc_id: Optional document ID
            refresh: Refresh behavior
            
        Returns:
            Document ID
        """
        try:
            response = await self.client.index(
                index=index,
                body=document,
                id=doc_id,
                refresh=refresh
            )
            return response["_id"]
        except ElasticsearchException as e:
            logger.error(f"Failed to index document: {e}")
            raise
            
    async def get_document(
        self,
        index: str,
        doc_id: str,
        source_includes: Optional[List[str]] = None,
        source_excludes: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get a document by ID.
        
        Args:
            index: Index name
            doc_id: Document ID
            source_includes: Fields to include
            source_excludes: Fields to exclude
            
        Returns:
            Document or None if not found
        """
        try:
            response = await self.client.get(
                index=index,
                id=doc_id,
                _source_includes=source_includes,
                _source_excludes=source_excludes
            )
            return response["_source"]
        except NotFoundError:
            return None
        except ElasticsearchException as e:
            logger.error(f"Failed to get document: {e}")
            raise
            
    async def update_document(
        self,
        index: str,
        doc_id: str,
        doc: Optional[Dict[str, Any]] = None,
        script: Optional[Dict[str, Any]] = None,
        upsert: Optional[Dict[str, Any]] = None,
        refresh: Union[bool, str] = False
    ) -> bool:
        """
        Update a document.
        
        Args:
            index: Index name
            doc_id: Document ID
            doc: Partial document to merge
            script: Script to execute
            upsert: Document to insert if doesn't exist
            refresh: Refresh behavior
            
        Returns:
            Success status
        """
        try:
            body = {}
            if doc:
                body["doc"] = doc
            if script:
                body["script"] = script
            if upsert:
                body["upsert"] = upsert
                
            await self.client.update(
                index=index,
                id=doc_id,
                body=body,
                refresh=refresh
            )
            return True
        except NotFoundError:
            if upsert:
                await self.index_document(index, upsert, doc_id, refresh)
                return True
            return False
        except ElasticsearchException as e:
            logger.error(f"Failed to update document: {e}")
            raise
            
    async def delete_document(
        self,
        index: str,
        doc_id: str,
        refresh: Union[bool, str] = False
    ) -> bool:
        """
        Delete a document.
        
        Args:
            index: Index name
            doc_id: Document ID
            refresh: Refresh behavior
            
        Returns:
            Success status
        """
        try:
            await self.client.delete(
                index=index,
                id=doc_id,
                refresh=refresh
            )
            return True
        except NotFoundError:
            return False
        except ElasticsearchException as e:
            logger.error(f"Failed to delete document: {e}")
            raise
            
    # Bulk Operations
    
    async def bulk_index(
        self,
        index: str,
        documents: List[Dict[str, Any]],
        chunk_size: int = 500,
        refresh: Union[bool, str] = False
    ) -> Dict[str, int]:
        """
        Bulk index documents.
        
        Args:
            index: Index name
            documents: List of documents
            chunk_size: Chunk size for bulk operations
            refresh: Refresh behavior
            
        Returns:
            Success and failed counts
        """
        try:
            actions = []
            for doc in documents:
                action = {
                    "_index": index,
                    "_source": doc
                }
                # Handle _id if present in document
                if "_id" in doc:
                    action["_id"] = doc["_id"]
                    doc_copy = doc.copy()
                    del doc_copy["_id"]
                    action["_source"] = doc_copy
                actions.append(action)
                
            success, failed = await async_bulk(
                self.client,
                actions,
                chunk_size=chunk_size,
                refresh=refresh
            )
            
            logger.info(f"Bulk indexed {success} documents, {len(failed)} failed")
            return {"success": success, "failed": len(failed)}
        except ElasticsearchException as e:
            logger.error(f"Bulk index failed: {e}")
            raise
            
    # Search Operations
    
    async def search(
        self,
        index: str,
        query: Optional[Dict[str, Any]] = None,
        size: int = 10,
        from_: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None,
        source_includes: Optional[List[str]] = None,
        source_excludes: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Search documents.
        
        Args:
            index: Index name or pattern
            query: Query DSL
            size: Number of results
            from_: Starting offset
            sort: Sort criteria
            source_includes: Fields to include
            source_excludes: Fields to exclude
            aggregations: Aggregations to compute
            
        Returns:
            Search results with hits and aggregations
        """
        try:
            body = {}
            if query:
                body["query"] = query
            if aggregations:
                body["aggs"] = aggregations
                
            response = await self.client.search(
                index=index,
                body=body,
                size=size,
                from_=from_,
                sort=sort,
                _source_includes=source_includes,
                _source_excludes=source_excludes
            )
            
            return {
                "total": response["hits"]["total"]["value"],
                "hits": [hit["_source"] for hit in response["hits"]["hits"]],
                "aggregations": response.get("aggregations", {})
            }
        except ElasticsearchException as e:
            logger.error(f"Search failed: {e}")
            raise
            
    async def count(
        self,
        index: str,
        query: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Count documents matching query.
        
        Args:
            index: Index name
            query: Query DSL
            
        Returns:
            Document count
        """
        try:
            body = {}
            if query:
                body["query"] = query
                
            response = await self.client.count(index=index, body=body)
            return response["count"]
        except ElasticsearchException as e:
            logger.error(f"Count failed: {e}")
            raise
            
    # Alias Operations
    
    async def create_alias(
        self,
        index: str,
        alias: str,
        filter: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create an alias for an index with optional filter.
        
        Args:
            index: Index name
            alias: Alias name
            filter: Optional filter for the alias
            
        Returns:
            Success status
        """
        try:
            body = {}
            if filter:
                body["filter"] = filter
                
            await self.client.indices.put_alias(
                index=index,
                name=alias,
                body=body if body else None
            )
            logger.info(f"Created alias {alias} for index {index}")
            return True
        except ElasticsearchException as e:
            logger.error(f"Failed to create alias: {e}")
            raise
            
    async def delete_alias(self, index: str, alias: str) -> bool:
        """
        Delete an alias.
        
        Args:
            index: Index name
            alias: Alias name
            
        Returns:
            Success status
        """
        try:
            await self.client.indices.delete_alias(index=index, name=alias)
            logger.info(f"Deleted alias {alias} from index {index}")
            return True
        except NotFoundError:
            logger.warning(f"Alias {alias} not found")
            return False
        except ElasticsearchException as e:
            logger.error(f"Failed to delete alias: {e}")
            raise
            
    async def update_aliases(
        self,
        actions: List[Dict[str, Any]]
    ) -> bool:
        """
        Perform multiple alias operations atomically.
        
        Args:
            actions: List of alias actions (add/remove)
            
        Returns:
            Success status
        """
        try:
            await self.client.indices.update_aliases(body={"actions": actions})
            logger.info(f"Updated aliases with {len(actions)} actions")
            return True
        except ElasticsearchException as e:
            logger.error(f"Failed to update aliases: {e}")
            raise
            
    # Utility Operations
    
    async def refresh_index(self, index: str) -> bool:
        """Force refresh an index to make changes searchable."""
        try:
            await self.client.indices.refresh(index=index)
            return True
        except ElasticsearchException as e:
            logger.error(f"Failed to refresh index: {e}")
            raise
            
    async def ping(self) -> bool:
        """Check Elasticsearch connection."""
        try:
            return await self.client.ping()
        except ElasticsearchException:
            return False


# Factory function for creating Elasticsearch service instances
def create_elasticsearch_service(
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None
) -> ElasticsearchService:
    """
    Create Elasticsearch service instance with environment-based configuration.
    
    Args:
        host: Elasticsearch host (defaults to env var or 'elasticsearch')
        port: Elasticsearch port (defaults to env var or 9200)
        username: Elasticsearch username (defaults to env var)
        password: Elasticsearch password (defaults to env var)
        
    Returns:
        ElasticsearchService instance
    """
    import os
    
    return ElasticsearchService(
        host=host or os.getenv("ELASTICSEARCH_HOST", "elasticsearch"),
        port=port or int(os.getenv("ELASTICSEARCH_PORT", "9200")),
        username=username or os.getenv("ELASTICSEARCH_USERNAME"),
        password=password or os.getenv("ELASTICSEARCH_PASSWORD")
    )