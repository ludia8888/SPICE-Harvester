"""
Instance Service for TerminusDB
인스턴스 데이터 조회 최적화 서비스 (N+1 Query 해결)
"""

import logging
import json
from typing import Any, Dict, List, Optional

from .base import BaseTerminusService
from .database import DatabaseService
from oms.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class InstanceService(BaseTerminusService):
    """
    TerminusDB 인스턴스 관리 서비스
    
    N+1 Query 문제를 해결하는 최적화된 인스턴스 조회 기능을 제공합니다.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # DatabaseService 인스턴스 (데이터베이스 존재 확인용)
        self.db_service = DatabaseService(*args, **kwargs)

    async def disconnect(self) -> None:
        # Close nested db_service first to avoid leaking its httpx client in short-lived contexts/tests.
        try:
            await self.db_service.disconnect()
        finally:
            await super().disconnect()
    
    async def get_class_instances_optimized(
        self,
        db_name: str,
        class_id: str,
        branch: str = "main",
        limit: int = 100,
        offset: int = 0,
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        특정 클래스의 인스턴스 목록을 효율적으로 조회
        
        TerminusDB Document API를 사용하여 인스턴스를 조회합니다.
        
        Args:
            db_name: 데이터베이스 이름
            class_id: 클래스 ID
            limit: 최대 결과 수
            offset: 시작 위치
            search: 검색어 (선택사항)
            
        Returns:
            완전히 조립된 인스턴스 목록과 총 개수
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            # Document API를 사용하여 인스턴스 조회
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {
                "type": class_id,
                "graph_type": "instance",
                "count": limit,
                "skip": offset
            }
            
            # 검색어가 있는 경우 필터 추가 (TerminusDB는 query 파라미터 지원)
            if search:
                params["query"] = search
            
            # Document API 호출
            result = await self._make_request("GET", endpoint, params=params)
            
            # Handle empty or text responses
            if result is None or result == "" or (isinstance(result, str) and not result.strip()):
                logger.info(f"No instances found for class {class_id} in database {db_name}")
                return {
                    "instances": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset
                }
            
            # Try to parse if it's a string that looks like JSON
            if isinstance(result, str):
                try:
                    import json
                    result = json.loads(result)
                except json.JSONDecodeError:
                    logger.warning(f"Received non-JSON response: {result[:100]}")
                    return {
                        "instances": [],
                        "total": 0,
                        "limit": limit,
                        "offset": offset
                    }
            
            # 결과 처리
            instances = []
            if isinstance(result, list):
                # 결과가 리스트인 경우 (여러 문서)
                instances = result
            elif isinstance(result, dict):
                # 결과가 단일 문서인 경우
                if "@id" in result:
                    instances = [result]
                else:
                    # 페이징된 결과일 수 있음
                    instances = result.get("documents", [])
            
            # 인스턴스 정리 (불필요한 메타데이터 제거)
            cleaned_instances = []
            for inst in instances:
                if isinstance(inst, dict):
                    # @id에서 인스턴스 ID 추출
                    if "@id" in inst:
                        instance_id = inst["@id"].split("/")[-1] if "/" in inst["@id"] else inst["@id"]
                        inst["instance_id"] = instance_id
                    
                    # class_id 확인/추가
                    if "@type" not in inst:
                        inst["@type"] = class_id
                    inst["class_id"] = class_id
                    
                    cleaned_instances.append(inst)
            
            # 총 개수 조회 (Document API는 별도의 count 엔드포인트가 없으므로 추정)
            # 실제 구현에서는 더 정확한 카운트를 위해 별도 쿼리나 메타데이터 사용
            total_count = len(cleaned_instances)
            if len(cleaned_instances) == limit:
                # 더 많은 결과가 있을 수 있음
                total_count = offset + limit + 1  # 최소 추정치
            
            return {
                "instances": cleaned_instances,
                "total": total_count
            }
            
        except Exception as e:
            logger.error(f"Failed to get optimized class instances: {e}")
            raise DatabaseError(f"인스턴스 목록 조회 실패: {e}")
    
    async def get_instance_optimized(
        self,
        db_name: str,
        instance_id: str,
        branch: str = "main",
        class_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        개별 인스턴스를 효율적으로 조회
        
        TerminusDB Document API를 사용하여 단일 문서를 조회합니다.
        
        Args:
            db_name: 데이터베이스 이름
            instance_id: 인스턴스 ID
            class_id: 클래스 ID (선택사항)
            
        Returns:
            완전한 인스턴스 객체 또는 None
        """
        try:
            await self.db_service.ensure_db_exists(db_name)

            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )

            candidates: List[str] = []
            if instance_id and "/" in instance_id:
                candidates.append(instance_id)
            if class_id and instance_id and "/" not in instance_id:
                candidates.append(f"{class_id}/{instance_id}")
            if instance_id and instance_id not in candidates:
                candidates.append(instance_id)

            last_error: Optional[Exception] = None
            for doc_id in candidates:
                params = {"graph_type": "instance", "id": doc_id}
                try:
                    result = await self._make_request("GET", endpoint, params=params)
                except ValueError as e:
                    # Treat "not found" as a miss and try the next candidate.
                    last_error = e
                    msg = str(e).lower()
                    if "404" in msg or "not found" in msg or "documentnotfound" in msg:
                        continue
                    raise

                if result is None or result == "" or (isinstance(result, str) and not result.strip()):
                    continue

                if isinstance(result, str):
                    try:
                        result = json.loads(result)
                    except json.JSONDecodeError:
                        continue

                doc: Optional[Dict[str, Any]] = None
                if isinstance(result, list):
                    if result and isinstance(result[0], dict):
                        doc = result[0]
                elif isinstance(result, dict):
                    # Terminus sometimes returns {"documents": [...]}
                    if "@id" in result or "@type" in result:
                        doc = result
                    elif isinstance(result.get("documents"), list) and result["documents"]:
                        if isinstance(result["documents"][0], dict):
                            doc = result["documents"][0]

                if not doc or not isinstance(doc, dict):
                    continue

                doc_id_value = doc.get("@id") or doc.get("id") or doc_id
                short_instance_id = (
                    doc_id_value.split("/")[-1] if isinstance(doc_id_value, str) and "/" in doc_id_value else doc_id_value
                )
                doc["instance_id"] = short_instance_id

                if class_id:
                    doc["class_id"] = class_id
                    doc["@type"] = doc.get("@type") or class_id
                else:
                    # Best-effort class inference.
                    inferred = None
                    raw_type = doc.get("@type")
                    if isinstance(raw_type, str) and raw_type:
                        inferred = raw_type.split("/")[-1] if "/" in raw_type else raw_type
                    elif isinstance(doc_id_value, str) and "/" in doc_id_value:
                        inferred = doc_id_value.split("/")[0]
                    if inferred:
                        doc["class_id"] = inferred
                        doc["@type"] = doc.get("@type") or inferred

                return doc

            # Not found
            return None
            
        except Exception as e:
            logger.error(f"Failed to get optimized instance: {e}")
            raise DatabaseError(f"인스턴스 조회 실패: {e}")
    
    async def count_class_instances(
        self,
        db_name: str,
        class_id: str,
        branch: str = "main",
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        특정 클래스의 인스턴스 개수를 효율적으로 조회
        
        Args:
            db_name: 데이터베이스 이름
            class_id: 클래스 ID
            
        Returns:
            인스턴스 개수
        """
        try:
            await self.db_service.ensure_db_exists(db_name)

            # NOTE: TerminusDB v12 supports filtering documents by `type` via the document API.
            # This avoids WOQL/SPARQL dialect differences and keeps the endpoint deterministic.
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            limit = 1000
            offset = 0
            total = 0

            # Filter conditions are currently ignored; count endpoint is used for fast health/smoke checks.
            while True:
                params = {
                    "graph_type": "instance",
                    "type": class_id,
                    "limit": limit,
                    "offset": offset,
                }
                result = await self._make_request("GET", endpoint, params=params)

                documents: List[Dict[str, Any]] = []
                if isinstance(result, list):
                    documents = [d for d in result if isinstance(d, dict)]
                elif isinstance(result, dict):
                    if isinstance(result.get("@graph"), list):
                        documents = [d for d in result["@graph"] if isinstance(d, dict)]
                    elif isinstance(result.get("documents"), list):
                        documents = [d for d in result["documents"] if isinstance(d, dict)]
                    else:
                        documents = [result]

                if not documents:
                    break

                total += len(documents)
                if len(documents) < limit:
                    break

                offset += limit

            return total
            
        except Exception as e:
            logger.error(f"Failed to count class instances: {e}")
            raise DatabaseError(f"인스턴스 개수 조회 실패: {e}")
