"""
Elasticsearch Client Service

Provides a centralized Elasticsearch client with connection pooling,
error handling, and common operations for search and indexing.
"""

import asyncio
import importlib
import logging
import os
from typing import TYPE_CHECKING, Optional, Dict, Any, List, Tuple, Union
from elasticsearch import AsyncElasticsearch, __versionstr__ as _ELASTIC_CLIENT_VERSION
# elasticsearch>=8.11.0 is now required in shared/pyproject.toml
from elasticsearch.exceptions import (
    ApiError as ElasticsearchException,  # ElasticsearchException renamed to ApiError in v9+
    NotFoundError,
    RequestError,
    ConnectionError as ESConnectionError
)
from elasticsearch.helpers import async_bulk

from shared.observability.tracing import trace_storage_operation
from shared.services.storage.connectivity import AsyncClientPingMixin

if TYPE_CHECKING:
    from shared.config.settings import ApplicationSettings

logger = logging.getLogger(__name__)


def _resolve_compat_version() -> Optional[str]:
    explicit = str(
        os.getenv("ELASTICSEARCH_COMPAT_VERSION")
        or os.getenv("ES_COMPAT_VERSION")
        or ""
    ).strip()
    if explicit:
        return explicit
    try:
        major = int(str(_ELASTIC_CLIENT_VERSION).split(".", maxsplit=1)[0])
    except Exception:
        return None
    # elasticsearch-py 9 defaults to `compatible-with=9`; our runtime ES is 8.x.
    return "8" if major >= 9 else None


def _apply_compat_mimetype_patch(compat_version: Optional[str]) -> None:
    """Patch elasticsearch-py v9 compatibility media type to target ES8 clusters.

    elasticsearch-py 9 hardcodes ``compatible-with=9`` in its generated request
    layer. Our local/runtime clusters are ES8, so we patch the internal template
    before instantiating clients.
    """

    normalized = str(compat_version or "").strip()
    if not normalized:
        return

    template = f"application/vnd.elasticsearch+%s; compatible-with={normalized}"
    for module_name in ("elasticsearch._async.client._base", "elasticsearch._sync.client._base"):
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue
        try:
            setattr(module, "_COMPAT_MIMETYPE_TEMPLATE", template)
            setattr(module, "_COMPAT_MIMETYPE_SUB", template % (r"\g<1>",))
        except Exception as exc:
            logger.debug("Failed to patch Elasticsearch compatibility template (%s): %s", module_name, exc)


class ElasticsearchService(AsyncClientPingMixin):
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
    
    _ping_exception_types = (ElasticsearchException,)

    def __init__(
        self,
        host: str = "localhost",
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

        compat_version = _resolve_compat_version()
        if compat_version:
            _apply_compat_mimetype_patch(compat_version)
            media_type = f"application/vnd.elasticsearch+json; compatible-with={compat_version}"
            self.config["headers"] = {
                "accept": media_type,
                "content-type": media_type,
            }
        
        # Add authentication if provided
        if username and password:
            self.config["basic_auth"] = (username, password)
            
        self._client: Optional[AsyncElasticsearch] = None
        self._connect_lock = asyncio.Lock()
        
    @trace_storage_operation("es.connect", system="elasticsearch")
    async def connect(self) -> None:
        """Initialize Elasticsearch connection."""
        async with self._connect_lock:
            existing = self._client
            if existing is not None:
                try:
                    await existing.ping()
                    return
                except Exception:
                    try:
                        await existing.close()
                    except Exception:
                        pass
                    self._client = None

            client = AsyncElasticsearch(**self.config)
            try:
                info = await client.info()
            except ESConnectionError as e:
                try:
                    await client.close()
                except Exception:
                    pass
                logger.error(f"Failed to connect to Elasticsearch: {e}")
                raise
            except Exception:
                try:
                    await client.close()
                except Exception:
                    pass
                raise

            self._client = client
            logger.info(
                f"Connected to Elasticsearch {info['version']['number']} "
                f"at {self.host}:{self.port}"
            )

    async def initialize(self) -> None:
        """ServiceContainer lifecycle hook — delegates to ``connect()``.

        The shared DI container calls ``initialize()`` on newly-created
        service instances (see ``ServiceContainer.get``).  Without this
        alias the ES client would be created but never connected, causing
        ``ElasticsearchServiceDep`` routes to fail with "not connected".
        """
        await self.connect()

    @trace_storage_operation("es.disconnect", system="elasticsearch")
    async def disconnect(self) -> None:
        """Close Elasticsearch connection."""
        client = self._client
        self._client = None
        try:
            if client:
                await client.close()
                logger.info("Disconnected from Elasticsearch")
        except Exception as e:
            logger.error(f"Error disconnecting from Elasticsearch: {e}")
            
    @property
    def client(self) -> AsyncElasticsearch:
        """Get Elasticsearch client instance."""
        if not self._client:
            raise RuntimeError("Elasticsearch client not connected. Call connect() first.")
        return self._client
    
    @trace_storage_operation("es.get_cluster_health", system="elasticsearch")
    async def get_cluster_health(self) -> Dict[str, Any]:
        """Get Elasticsearch cluster health status."""
        try:
            health = await self.client.cluster.health()
            # elasticsearch-py returns ObjectApiResponse (not JSON-serializable) in v8+.
            return getattr(health, "body", health)
        except Exception as e:
            logger.error(f"Error getting cluster health: {e}")
            raise
        
    # Index Operations
    
    @trace_storage_operation("es.create_index", system="elasticsearch")
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
            
    @trace_storage_operation("es.delete_index", system="elasticsearch")
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
            
    @trace_storage_operation("es.index_exists", system="elasticsearch")
    async def index_exists(self, index: str) -> bool:
        """Check if index exists."""
        try:
            return await self.client.indices.exists(index=index)
        except ElasticsearchException as e:
            logger.error(f"Failed to check index existence: {e}")
            return False
            
    @trace_storage_operation("es.update_mapping", system="elasticsearch")
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
    
    @trace_storage_operation("es.index_document", system="elasticsearch")
    async def index_document(
        self,
        index: str,
        document: Dict[str, Any],
        doc_id: Optional[str] = None,
        refresh: Union[bool, str] = False,
        *,
        version: Optional[int] = None,
        version_type: Optional[str] = None,
        op_type: Optional[str] = None,
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
            kwargs: Dict[str, Any] = {
                "index": index,
                "body": document,
                "id": doc_id,
                "refresh": refresh,
            }
            if version is not None:
                kwargs["version"] = int(version)
            if version_type is not None:
                kwargs["version_type"] = version_type
            if op_type is not None:
                kwargs["op_type"] = op_type

            response = await self.client.index(**kwargs)
            return response["_id"]
        except ElasticsearchException as e:
            logger.error(f"Failed to index document: {e}")
            raise
            
    @trace_storage_operation("es.get_document", system="elasticsearch")
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
            
    @trace_storage_operation("es.update_document", system="elasticsearch")
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
            
    @trace_storage_operation("es.delete_document", system="elasticsearch")
    async def delete_document(
        self,
        index: str,
        doc_id: str,
        refresh: Union[bool, str] = False,
        *,
        version: Optional[int] = None,
        version_type: Optional[str] = None,
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
            kwargs: Dict[str, Any] = {"index": index, "id": doc_id, "refresh": refresh}
            if version is not None:
                kwargs["version"] = int(version)
            if version_type is not None:
                kwargs["version_type"] = version_type

            await self.client.delete(**kwargs)
            return True
        except NotFoundError:
            return False
        except ElasticsearchException as e:
            logger.error(f"Failed to delete document: {e}")
            raise
            
    # Bulk Operations
    
    @trace_storage_operation("es.bulk_index", system="elasticsearch")
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
    
    @trace_storage_operation("es.search", system="elasticsearch")
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
            
    @trace_storage_operation("es.count", system="elasticsearch")
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
    
    @trace_storage_operation("es.create_alias", system="elasticsearch")
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
            
    @trace_storage_operation("es.delete_alias", system="elasticsearch")
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
            
    @trace_storage_operation("es.update_aliases", system="elasticsearch")
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
    
    @trace_storage_operation("es.refresh_index", system="elasticsearch")
    async def refresh_index(self, index: str) -> bool:
        """Force refresh an index to make changes searchable."""
        try:
            await self.client.indices.refresh(index=index)
            return True
        except ElasticsearchException as e:
            logger.error(f"Failed to refresh index: {e}")
            raise
            
# Factory function for creating Elasticsearch service instances
def create_elasticsearch_service(settings: 'ApplicationSettings') -> ElasticsearchService:
    """
    Elasticsearch 서비스 팩토리 함수 (Anti-pattern 13 해결)
    
    Args:
        settings: 중앙화된 애플리케이션 설정 객체
        
    Returns:
        ElasticsearchService 인스턴스
        
    Note:
        이 함수는 더 이상 내부에서 환경변수를 로드하지 않습니다.
        모든 설정은 ApplicationSettings를 통해 중앙화되어 관리됩니다.
    """
    return ElasticsearchService(
        host=settings.database.elasticsearch_host,
        port=settings.database.elasticsearch_port,
        username=settings.database.elasticsearch_username,
        password=settings.database.elasticsearch_password,
        request_timeout=settings.database.elasticsearch_request_timeout,
    )


async def promote_alias_to_index(
    *,
    elasticsearch_service: ElasticsearchService,
    base_index: str,
    new_index: str,
    allow_delete_base_index: bool = False,
) -> Tuple[bool, Optional[str]]:
    """Atomically promote *new_index* behind the *base_index* alias.

    Blue-Green swap pattern:
    - If *base_index* is already an alias → atomic swap (remove old targets, add new).
    - If *base_index* is a concrete index and *allow_delete_base_index* → delete then create alias.
    - Otherwise create the alias from scratch.

    Returns ``(success, error_message)``.
    """
    try:
        alias_exists = False
        try:
            alias_exists = await elasticsearch_service.client.indices.exists_alias(name=base_index)
        except Exception:
            logging.getLogger(__name__).warning("Exception fallback at shared/services/storage/elasticsearch_service.py:690", exc_info=True)
            alias_exists = False

        if alias_exists:
            current = await elasticsearch_service.client.indices.get_alias(name=base_index)
            current_indices = list(current.keys())
            actions: List[Dict[str, Any]] = [
                {"remove": {"index": idx, "alias": base_index}} for idx in current_indices
            ]
            actions.append({"add": {"index": new_index, "alias": base_index}})
            await elasticsearch_service.update_aliases(actions)
            return True, None

        base_exists = await elasticsearch_service.index_exists(base_index)
        if base_exists and not allow_delete_base_index:
            raise RuntimeError(
                "Base index exists as a concrete index; set allow_delete_base_index=true to convert it to an alias"
            )
        if base_exists:
            await elasticsearch_service.delete_index(base_index)
        await elasticsearch_service.create_alias(index=new_index, alias=base_index)
        return True, None
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception fallback at shared/services/storage/elasticsearch_service.py:712", exc_info=True)
        return False, str(exc)
