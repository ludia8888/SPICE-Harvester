"""
Async TerminusDB 서비스 모듈 - Clean Facade
httpx를 사용한 비동기 TerminusDB 클라이언트 구현
모듈화된 서비스들을 통합하는 깔끔한 파사드 패턴
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

import httpx

from oms.exceptions import (
    ConnectionError,
    DuplicateOntologyError,
    OntologyNotFoundError,
    OntologyValidationError,
)
from oms.utils.terminus_retry import build_async_retry

from oms.validators.relationship_validator import RelationshipValidator, ValidationSeverity
from shared.models.config import ConnectionConfig
from shared.models.ontology import OntologyBase, OntologyResponse

from .property_to_relationship_converter import PropertyToRelationshipConverter

# Import new TerminusDB schema type support

# Import constraint and default value extraction
from shared.observability.tracing import trace_external_call

# Import modular TerminusDB services
from .terminus import (
    DatabaseService,
    QueryService,
    InstanceService,
    OntologyService,
    VersionControlService,
    DocumentService
)

logger = logging.getLogger(__name__)

# Atomic update specific exceptions
class AtomicUpdateError(Exception):
    """Base exception for atomic update operations"""
    pass

class PatchUpdateError(AtomicUpdateError):
    """Exception for PATCH-based update failures"""
    pass

class TransactionUpdateError(AtomicUpdateError):
    """Exception for transaction-based update failures"""
    pass

class BackupCreationError(Exception):
    """Exception for backup creation failures"""
    pass

class RestoreError(Exception):
    """Exception for restore operation failures"""
    pass

class BackupRestoreError(Exception):
    """Exception for backup and restore operation failures"""
    pass

# 하위 호환성을 위한 별칭
OntologyNotFoundError = OntologyNotFoundError
DuplicateOntologyError = DuplicateOntologyError
OntologyValidationError = OntologyValidationError
DatabaseError = ConnectionError


async_terminus_retry = build_async_retry(
    retry_exceptions=(httpx.ConnectError, httpx.TimeoutException, ConnectionError),
    backoff="exponential",
    logger=logger,
    on_failure=lambda exc, retries: ConnectionError(f"Failed after {retries} attempts: {exc}"),
)


class AsyncTerminusService:
    """
    비동기 TerminusDB 서비스 클래스 - Clean Facade Pattern
    
    이 클래스는 modular service pattern을 사용하여 기능별로 분리된 서비스를 통합합니다:
    - DatabaseService: 데이터베이스 관리 (생성, 삭제, 목록)
    - QueryService: Foundry-style query-spec 실행
    - InstanceService: 인스턴스 조회 최적화 (N+1 query 해결)
    - OntologyService: 온톨로지/스키마 CRUD
    - VersionControlService: 브랜치/커밋 관리
    - DocumentService: 문서(인스턴스) CRUD
    
    모든 레거시 코드와 deprecated 메서드는 제거되었습니다.
    """

    def __init__(self, connection_info: Optional[ConnectionConfig] = None):
        """
        초기화

        Args:
            connection_info: 연결 정보 객체
        """
        # Use centralized settings if no connection info provided
        self.connection_info = connection_info or ConnectionConfig.from_settings()

        self._client = None
        self._auth_token = None
        self._db_cache = set()

        # Initialize modular TerminusDB services
        self.database_service = DatabaseService(self.connection_info)
        self.query_service = QueryService(self.connection_info)
        self.instance_service = InstanceService(self.connection_info)
        self.ontology_service = OntologyService(self.connection_info)
        self.version_control_service = VersionControlService(self.connection_info)
        self.document_service = DocumentService(self.connection_info)

        self.property_converter = PropertyToRelationshipConverter()

        # Relationship cache for performance
        self._ontology_cache: Dict[str, List[OntologyResponse]] = {}

        # Stateless HTTP API에서 "current branch"는 서버에 존재하지 않지만,
        # 일부 라우터가 UX 목적으로 사용하므로 서비스 레벨에서 best-effort로 추적.
        self._current_branch_by_db: Dict[str, str] = {}
        
        # 동시 요청 제한으로 TerminusDB 부하 조절
        self._request_semaphore = asyncio.Semaphore(50)  # 최대 50개 동시 요청
        
        # 메타데이터 스키마 캐시로 성능 최적화
        self._metadata_schema_cache: set = set()  # 이미 생성된 DB의 메타데이터 스키마

    @trace_external_call("oms.terminus.check_connection")
    async def check_connection(self) -> bool:
        """연결 상태 확인"""
        try:
            # database_service를 통해 연결 확인
            if hasattr(self.database_service, 'check_connection'):
                return await self.database_service.check_connection()
            else:
                # 기본적으로 데이터베이스 목록 조회로 연결 확인
                await self.list_databases()
                return True
        except Exception as exc:
            logger.warning("Terminus check_connection failed: %s", exc, exc_info=True)
            return False

    async def connect(self) -> None:
        """연결 설정"""
        # database_service를 통해 연결
        if hasattr(self.database_service, 'connect'):
            await self.database_service.connect()

    async def disconnect(self) -> None:
        """연결 해제"""
        await self.close()

    async def close(self):
        """모든 서비스 종료"""
        # Close all modular services using disconnect method
        for service in [
            self.database_service,
            self.query_service,
            self.instance_service,
            self.ontology_service,
            self.version_control_service,
            self.document_service
        ]:
            if hasattr(service, 'disconnect'):
                await service.disconnect()
            elif hasattr(service, 'close'):
                await service.close()
        
        # Close HTTP client if exists
        if self._client:
            await self._client.aclose()
            self._client = None

    # ==========================================
    # Database Management - Facade Methods
    # ==========================================
    
    @async_terminus_retry(max_retries=3)
    @trace_external_call("oms.terminus.create_database")
    async def create_database(self, db_name: str, description: str = "") -> bool:
        """데이터베이스 생성"""
        result = await self.database_service.create_database(db_name, description)
        
        # 성공 시 캐시 갱신 및 True 반환
        if result and isinstance(result, dict):
            self._db_cache.add(db_name)
            return True
        
        return False

    @async_terminus_retry(max_retries=3)
    @trace_external_call("oms.terminus.database_exists")
    async def database_exists(self, db_name: str) -> bool:
        """데이터베이스 존재 여부 확인"""
        return await self.database_service.database_exists(db_name)

    @trace_external_call("oms.terminus.list_databases")
    async def list_databases(self) -> List[Dict[str, Any]]:
        """사용 가능한 데이터베이스 목록 조회"""
        return await self.database_service.list_databases()

    @async_terminus_retry(max_retries=3)
    @trace_external_call("oms.terminus.delete_database")
    async def delete_database(self, db_name: str) -> bool:
        """데이터베이스 삭제"""
        return await self.database_service.delete_database(db_name)

    # ==========================================
    # Query Execution - Facade Methods
    # ==========================================
    
    @trace_external_call("oms.terminus.execute_query")
    async def execute_query(
        self, db_name: str, query_dict: Dict[str, Any], *, branch: str = "main"
    ) -> Dict[str, Any]:
        """Foundry-style query-spec 실행."""
        return await self.query_service.execute_query(db_name, query_dict, branch=branch)

    # ==========================================
    # Instance Operations - Facade Methods
    # ==========================================
    
    @trace_external_call("oms.terminus.get_class_instances_optimized")
    async def get_class_instances_optimized(
        self, 
        db_name: str, 
        class_id: str, 
        branch: str = "main",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        filter_conditions: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """특정 클래스의 모든 인스턴스를 효율적으로 조회"""
        # Extract search from filter_conditions if present
        search = None
        if filter_conditions and isinstance(filter_conditions, dict):
            search = filter_conditions.get("search")
        
        return await self.instance_service.get_class_instances_optimized(
            db_name=db_name,
            class_id=class_id,
            branch=branch,
            limit=limit or 100,
            offset=offset or 0,
            search=search,
        )

    @trace_external_call("oms.terminus.get_instance_optimized")
    async def get_instance_optimized(
        self, 
        db_name: str, 
        instance_id: str, 
        branch: str = "main",
        class_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """개별 인스턴스를 효율적으로 조회"""
        return await self.instance_service.get_instance_optimized(
            db_name=db_name,
            instance_id=instance_id,
            branch=branch,
            class_id=class_id,
        )

    @trace_external_call("oms.terminus.count_class_instances")
    async def count_class_instances(
        self, 
        db_name: str, 
        class_id: str, 
        branch: str = "main",
        filter_conditions: Optional[Dict[str, Any]] = None
    ) -> int:
        """특정 클래스의 인스턴스 개수를 효율적으로 조회"""
        return await self.instance_service.count_class_instances(
            db_name=db_name,
            class_id=class_id,
            branch=branch,
            filter_conditions=filter_conditions,
        )

    # ==========================================
    # Ontology Operations - Facade Methods
    # ==========================================
    
    @trace_external_call("oms.terminus.get_ontology")
    async def get_ontology(
        self, 
        db_name: str, 
        class_id: Optional[str] = None, 
        raise_if_missing: bool = True,
        *,
        branch: str = "main",
    ) -> Any:
        """온톨로지 조회 (branch-aware).

        Compatibility:
        - If `class_id` is provided: returns a single OntologyResponse or None.
        - If `class_id` is None: returns a list of OntologyResponse.
        """
        if not class_id:
            return await self.ontology_service.list_ontologies(db_name, branch=branch, limit=1000, offset=0)
        result = await self.ontology_service.get_ontology(db_name, class_id, branch=branch)
        if result is None and raise_if_missing:
            raise OntologyNotFoundError(f"Ontology not found: {class_id}")
        return result

    @trace_external_call("oms.terminus.create_ontology")
    async def create_ontology(
        self, db_name: str, ontology_data: OntologyBase, *, branch: str = "main"
    ) -> OntologyResponse:
        """온톨로지 생성"""
        # Create ontology using TerminusDB service
        try:
            result = await self.ontology_service.create_ontology(db_name, ontology_data, branch=branch)
            logger.info(f"Successfully created ontology '{ontology_data.id}' in database '{db_name}'")
            return result
        except Exception as e:
            logger.error(f"Failed to create ontology '{ontology_data.id}': {e}")
            raise

    @trace_external_call("oms.terminus.update_ontology")
    async def update_ontology(
        self, 
        db_name: str, 
        class_id: str, 
        ontology_data: OntologyBase,
        *,
        branch: str = "main",
    ) -> OntologyResponse:
        """온톨로지 업데이트 - Atomic 버전"""
        return await self.ontology_service.update_ontology(db_name, class_id, ontology_data, branch=branch)

    @trace_external_call("oms.terminus.delete_ontology")
    async def delete_ontology(self, db_name: str, class_id: str, *, branch: str = "main") -> bool:
        """온톨로지 삭제"""
        return await self.ontology_service.delete_ontology(db_name, class_id, branch=branch)

    @trace_external_call("oms.terminus.list_ontology_classes")
    async def list_ontology_classes(self, db_name: str) -> List[OntologyResponse]:
        """데이터베이스의 모든 온톨로지 목록 조회"""
        # Use default limit and offset values
        return await self.ontology_service.list_ontologies(db_name, limit=100, offset=0)

    # ==========================================
    # Version Control - Facade Methods
    # ==========================================

    @staticmethod
    def _is_protected_branch_name(branch_name: str) -> bool:
        return str(branch_name) in {"main", "master", "production"}
    
    @trace_external_call("oms.terminus.create_branch")
    async def create_branch(self, db_name: str, branch_name: str, from_branch: str = "main") -> bool:
        """브랜치 생성"""
        await self.version_control_service.create_branch(db_name, branch_name, from_branch)
        return True

    @trace_external_call("oms.terminus.list_branches")
    async def list_branches(self, db_name: str) -> List[str]:
        """브랜치 목록 조회"""
        branches = await self.version_control_service.list_branches(db_name)
        if branches and isinstance(branches[0], dict):
            return [b.get("name") for b in branches if isinstance(b, dict) and b.get("name")]
        return [str(b) for b in branches] if isinstance(branches, list) else []

    @trace_external_call("oms.terminus.get_branch_info")
    async def get_branch_info(self, db_name: str, branch_name: str) -> Dict[str, Any]:
        branches = await self.list_branches(db_name)
        if branch_name not in branches:
            raise ValueError(f"branch not found: {branch_name}")
        current_branch = await self.get_current_branch(db_name)
        return {
            "name": branch_name,
            "current": branch_name == current_branch,
            "protected": self._is_protected_branch_name(branch_name),
        }

    async def get_current_branch(self, db_name: str) -> str:
        """현재 브랜치 (best-effort, 기본값: main)"""
        return self._current_branch_by_db.get(db_name, "main")

    @trace_external_call("oms.terminus.delete_branch")
    async def delete_branch(self, db_name: str, branch_name: str) -> bool:
        """브랜치 삭제"""
        return await self.version_control_service.delete_branch(db_name, branch_name)

    @trace_external_call("oms.terminus.checkout_branch")
    async def checkout_branch(self, db_name: str, branch_name: str) -> bool:
        """브랜치 체크아웃"""
        ok = await self.version_control_service.checkout_branch(db_name, branch_name)
        if ok:
            self._current_branch_by_db[db_name] = branch_name
        return ok

    async def checkout(self, db_name: str, target: str, target_type: str = "branch") -> bool:
        """Router 호환 checkout (branch/commit)."""
        if target_type == "branch":
            return await self.checkout_branch(db_name, target)
        # commit checkout is stateless; accept for compatibility
        return True

    @trace_external_call("oms.terminus.merge_branches")
    async def merge_branches(
        self, 
        db_name: str, 
        source_branch: str, 
        target_branch: str,
        message: Optional[str] = None,
        author: Optional[str] = None
    ) -> Dict[str, Any]:
        """브랜치 병합"""
        return await self.version_control_service.merge(
            db_name,
            source_branch=source_branch,
            target_branch=target_branch,
            author=author,
            message=message,
        )

    @trace_external_call("oms.terminus.commit")
    async def commit(
        self, 
        db_name: str, 
        message: str, 
        author: str = "admin",
        branch: Optional[str] = None
    ) -> str:
        """커밋 생성"""
        return await self.version_control_service.commit(db_name, message, author, branch)

    @trace_external_call("oms.terminus.get_commit_history")
    async def get_commit_history(
        self, 
        db_name: str, 
        branch: str = "main", 
        limit: int = 10,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """커밋 히스토리 조회"""
        return await self.version_control_service.get_commit_history(db_name, branch, limit, offset)

    @trace_external_call("oms.terminus.diff")
    async def diff(self, db_name: str, from_ref: str, to_ref: str) -> Any:
        """차이점 조회"""
        return await self.version_control_service.diff(db_name, from_ref, to_ref)

    @trace_external_call("oms.terminus.merge")
    async def merge(
        self,
        db_name: str,
        *,
        source_branch: str,
        target_branch: str,
        strategy: str = "auto",
    ) -> Dict[str, Any]:
        """Router 호환 merge API."""
        return await self.version_control_service.merge(
            db_name,
            source_branch=source_branch,
            target_branch=target_branch,
            strategy=strategy,
        )

    @trace_external_call("oms.terminus.rebase")
    async def rebase(
        self, 
        db_name: str, 
        *,
        onto: str,
        branch: str,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Router 호환 rebase API (branch -> onto)."""
        return await self.version_control_service.rebase(db_name, branch=branch, onto=onto, message=message)

    @trace_external_call("oms.terminus.rollback")
    async def rollback(self, db_name: str, target: str) -> Dict[str, Any]:
        """Router 호환 rollback API (reset current branch to target)."""
        branch = await self.get_current_branch(db_name)
        return await self.version_control_service.reset_branch(db_name, branch_name=branch, commit_id=target)

    async def find_common_ancestor(
        self, db_name: str, branch1: str, branch2: str
    ) -> Optional[str]:
        """공통 조상 찾기 (현재는 best-effort 미구현)."""
        return None

    # ==========================================
    # Document Operations - Facade Methods
    # ==========================================
    
    @trace_external_call("oms.terminus.create_instance")
    async def create_instance(
        self, 
        db_name: str, 
        class_id: str, 
        instance_data: Dict[str, Any],
        *,
        branch: str = "main",
    ) -> Dict[str, Any]:
        """인스턴스 생성"""
        return await self.document_service.create_instance(db_name, class_id, instance_data, branch=branch)

    @trace_external_call("oms.terminus.update_instance")
    async def update_instance(
        self, 
        db_name: str, 
        class_id: str, 
        instance_id: str,
        update_data: Dict[str, Any],
        *,
        branch: str = "main",
    ) -> Dict[str, Any]:
        """인스턴스 업데이트"""
        return await self.document_service.update_instance(
            db_name, class_id, instance_id, update_data, branch=branch
        )

    @trace_external_call("oms.terminus.delete_instance")
    async def delete_instance(
        self, 
        db_name: str, 
        class_id: str, 
        instance_id: str,
        *,
        branch: str = "main",
    ) -> bool:
        """인스턴스 삭제"""
        return await self.document_service.delete_instance(db_name, class_id, instance_id, branch=branch)

    # ==========================================
    # Relationship Management - Direct Methods
    # ==========================================
    
    @trace_external_call("oms.terminus.validate_relationships")
    async def validate_relationships(
        self,
        db_name: str,
        ontology_data: Dict[str, Any],
        *,
        branch: str = "main",
        fix_issues: bool = False,
    ) -> Dict[str, Any]:
        """Validate ontology relationships against current schema (no write)."""
        existing = await self.get_ontology(db_name, class_id=None, raise_if_missing=False, branch=branch)
        existing_ontologies: List[OntologyResponse] = existing if isinstance(existing, list) else []

        payload = dict(ontology_data or {})
        if not payload.get("id"):
            from shared.utils.id_generator import generate_simple_id

            label_value = payload.get("label") or "UnnamedClass"
            label_str = ""
            if isinstance(label_value, str):
                label_str = label_value.strip()
            elif isinstance(label_value, dict):
                label_str = (
                    str(label_value.get("en") or "").strip()
                    or str(label_value.get("ko") or "").strip()
                    or next((str(v).strip() for v in label_value.values() if v and str(v).strip()), "")
                )

            payload["id"] = generate_simple_id(
                label=label_str,
                use_timestamp_for_korean=True,
                default_fallback="UnnamedClass",
            )

        candidate = OntologyResponse(**OntologyBase(**payload).model_dump(mode="json"))

        validator = RelationshipValidator(existing_ontologies=existing_ontologies)
        results = validator.validate_ontology_relationships(candidate)

        def _serialize(result: Any) -> Dict[str, Any]:
            severity = result.severity.value if hasattr(result.severity, "value") else str(result.severity)
            return {
                "severity": severity,
                "code": result.code,
                "message": result.message,
                "field": result.field,
                "related_objects": result.related_objects,
            }

        serialized = [_serialize(r) for r in results]
        errors = [r for r in serialized if r["severity"] == ValidationSeverity.ERROR.value]
        warnings = [r for r in serialized if r["severity"] == ValidationSeverity.WARNING.value]
        info = [r for r in serialized if r["severity"] == ValidationSeverity.INFO.value]

        if fix_issues:
            # No automatic fixer exists yet; be explicit so this never becomes a silent no-op.
            logger.info("fix_issues requested but no fixer is implemented; returning analysis only")

        return {
            "ok": len(errors) == 0,
            "db_name": db_name,
            "branch": branch,
            "class_id": candidate.id,
            "issues": serialized,
            "errors": errors,
            "warnings": warnings,
            "info": info,
            "summary": {
                "total": len(serialized),
                "errors": len(errors),
                "warnings": len(warnings),
                "info": len(info),
                "fix_issues_requested": bool(fix_issues),
                "fix_issues_applied": False,
            },
        }

    def convert_properties_to_relationships(
        self, 
        ontology: OntologyResponse
    ) -> OntologyResponse:
        """속성을 관계로 변환"""
        return self.property_converter.convert(ontology)

    # ==========================================
    # Utility Methods
    # ==========================================
    
    def clear_cache(self, db_name: Optional[str] = None):
        """캐시 초기화"""
        if db_name:
            self._ontology_cache.pop(db_name, None)
        else:
            self._ontology_cache.clear()

    @trace_external_call("oms.terminus.ping")
    async def ping(self) -> bool:
        """서버 연결 상태 확인"""
        try:
            # Try to list databases as a ping test
            await self.list_databases()
            return True
        except Exception as exc:
            logger.warning("Terminus ping failed: %s", exc, exc_info=True)
            return False

    def get_connection_info(self) -> ConnectionConfig:
        """현재 연결 정보 반환"""
        return self.connection_info

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        """Async context manager exit"""
        await self.close()
