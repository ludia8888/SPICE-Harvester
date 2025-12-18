"""
Document Service for TerminusDB
문서(인스턴스) CRUD 작업 서비스
"""

import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone
import json

from .base import BaseTerminusService
from .database import DatabaseService
from oms.exceptions import (
    DatabaseError,
    OntologyNotFoundError
)

logger = logging.getLogger(__name__)


class DocumentService(BaseTerminusService):
    """
    TerminusDB 문서(인스턴스) 관리 서비스
    
    문서 생성, 수정, 삭제 및 조회 기능을 제공합니다.
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
    
    async def create_document(
        self,
        db_name: str,
        document: Dict[str, Any],
        graph_type: str = "instance",
        branch: str = "main",
        author: Optional[str] = None,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        새 문서 생성
        
        Args:
            db_name: 데이터베이스 이름
            document: 문서 데이터
            graph_type: 그래프 타입 (instance, schema, inference)
            author: 작성자
            message: 커밋 메시지
            
        Returns:
            생성된 문서 정보
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            # 문서 ID 확인
            doc_id = document.get("@id") or document.get("id")
            if not doc_id:
                raise ValueError("문서에 ID가 필요합니다")
            
            # 중복 확인
            if await self.document_exists(db_name, doc_id, graph_type, branch=branch):
                raise DatabaseError(f"문서 '{doc_id}'이(가) 이미 존재합니다")
            
            # 문서 생성 엔드포인트
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {"graph_type": graph_type}
            
            # CLAUDE RULE FIX: Always add author parameter (required by TerminusDB API)
            if author:
                params["author"] = author
            else:
                # Default author if not provided
                params["author"] = "system"
                
            if message:
                params["message"] = message
            
            # Debug logging to verify params
            logger.info(f"Creating document with params: {params}")
            
            # @type 확인 및 추가
            if "@type" not in document and "type" in document:
                document["@type"] = document["type"]
            
            await self._make_request("POST", endpoint, document, params=params)
            
            logger.info(f"Document '{doc_id}' created in database '{db_name}'")
            
            return {
                "id": doc_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "graph_type": graph_type
            }
            
        except Exception as e:
            logger.error(f"Failed to create document: {e}")
            raise DatabaseError(f"문서 생성 실패: {e}")
    
    async def update_document(
        self,
        db_name: str,
        doc_id: str,
        document: Dict[str, Any],
        graph_type: str = "instance",
        branch: str = "main",
        author: Optional[str] = None,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        문서 업데이트
        
        Args:
            db_name: 데이터베이스 이름
            doc_id: 문서 ID
            document: 업데이트할 문서 데이터
            graph_type: 그래프 타입
            author: 작성자
            message: 커밋 메시지
            
        Returns:
            업데이트된 문서 정보
        """
        try:
            # 존재 확인
            if not await self.document_exists(db_name, doc_id, graph_type, branch=branch):
                raise OntologyNotFoundError(f"문서 '{doc_id}'을(를) 찾을 수 없습니다")
            
            # ID 설정
            document["@id"] = doc_id
            
            # 문서 업데이트 엔드포인트
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {
                "graph_type": graph_type,
                "id": doc_id
            }
            
            # CLAUDE RULE FIX: Always add author parameter
            if author:
                params["author"] = author
            else:
                params["author"] = "system"
            if message:
                params["message"] = message
            
            await self._make_request("PUT", endpoint, document, params=params)
            
            logger.info(f"Document '{doc_id}' updated in database '{db_name}'")
            
            return {
                "id": doc_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "graph_type": graph_type
            }
            
        except OntologyNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Failed to update document: {e}")
            raise DatabaseError(f"문서 업데이트 실패: {e}")
    
    async def delete_document(
        self,
        db_name: str,
        doc_id: str,
        graph_type: str = "instance",
        branch: str = "main",
        author: Optional[str] = None,
        message: Optional[str] = None
    ) -> bool:
        """
        문서 삭제
        
        Args:
            db_name: 데이터베이스 이름
            doc_id: 문서 ID
            graph_type: 그래프 타입
            author: 작성자
            message: 커밋 메시지
            
        Returns:
            삭제 성공 여부
        """
        try:
            # 존재 확인
            if not await self.document_exists(db_name, doc_id, graph_type, branch=branch):
                raise OntologyNotFoundError(f"문서 '{doc_id}'을(를) 찾을 수 없습니다")
            
            # 문서 삭제 엔드포인트
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {
                "graph_type": graph_type,
                "id": doc_id
            }
            
            # CLAUDE RULE FIX: Always add author parameter
            if author:
                params["author"] = author
            else:
                params["author"] = "system"
            if message:
                params["message"] = message
            
            await self._make_request("DELETE", endpoint, params=params)
            
            logger.info(f"Document '{doc_id}' deleted from database '{db_name}'")
            return True
            
        except OntologyNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Failed to delete document: {e}")
            raise DatabaseError(f"문서 삭제 실패: {e}")
    
    async def get_document(
        self,
        db_name: str,
        doc_id: str,
        graph_type: str = "instance",
        branch: str = "main",
    ) -> Optional[Dict[str, Any]]:
        """
        특정 문서 조회
        
        Args:
            db_name: 데이터베이스 이름
            doc_id: 문서 ID
            graph_type: 그래프 타입
            
        Returns:
            문서 데이터 또는 None
        """
        try:
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {
                "graph_type": graph_type,
                "id": doc_id
            }
            
            result = await self._make_request("GET", endpoint, params=params)
            
            if not result:
                return None
            
            # 단일 문서 추출
            if isinstance(result, list) and len(result) > 0:
                return result[0]
            elif isinstance(result, dict):
                return result
            else:
                return None
                
        except Exception as e:
            logger.error(f"Failed to get document: {e}")
            return None
    
    async def list_documents(
        self,
        db_name: str,
        graph_type: str = "instance",
        branch: str = "main",
        doc_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        문서 목록 조회
        
        Args:
            db_name: 데이터베이스 이름
            graph_type: 그래프 타입
            doc_type: 문서 타입 (클래스) 필터
            limit: 최대 결과 수
            offset: 시작 위치
            
        Returns:
            문서 목록
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {
                "graph_type": graph_type,
                "limit": limit,
                "offset": offset
            }
            
            if doc_type:
                params["type"] = doc_type
            
            result = await self._make_request("GET", endpoint, params=params)
            
            documents = []
            
            if isinstance(result, list):
                documents = result
            elif isinstance(result, dict):
                if "@graph" in result:
                    documents = result["@graph"]
                elif "documents" in result:
                    documents = result["documents"]
                else:
                    documents = [result]
            
            return documents
            
        except Exception as e:
            logger.error(f"Failed to list documents: {e}")
            raise DatabaseError(f"문서 목록 조회 실패: {e}")
    
    async def document_exists(
        self,
        db_name: str,
        doc_id: str,
        graph_type: str = "instance",
        branch: str = "main",
    ) -> bool:
        """
        문서 존재 여부 확인
        
        Args:
            db_name: 데이터베이스 이름
            doc_id: 문서 ID
            graph_type: 그래프 타입
            
        Returns:
            존재 여부
        """
        try:
            doc = await self.get_document(db_name, doc_id, graph_type, branch=branch)
            return doc is not None
        except Exception:
            return False
    
    async def bulk_create_documents(
        self,
        db_name: str,
        documents: List[Dict[str, Any]],
        graph_type: str = "instance",
        branch: str = "main",
        author: Optional[str] = None,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        여러 문서를 한 번에 생성
        
        Args:
            db_name: 데이터베이스 이름
            documents: 문서 목록
            graph_type: 그래프 타입
            author: 작성자
            message: 커밋 메시지
            
        Returns:
            생성 결과
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            # 문서 ID 검증
            for doc in documents:
                if not doc.get("@id") and not doc.get("id"):
                    raise ValueError("모든 문서에 ID가 필요합니다")
            
            # 벌크 생성 엔드포인트
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {"graph_type": graph_type}
            
            if author:
                params["author"] = author
            if message:
                params["message"] = message
            
            # JSON-LD 그래프 형식으로 래핑
            data = {"@graph": documents}
            
            await self._make_request("POST", endpoint, data, params=params)
            
            logger.info(f"Bulk created {len(documents)} documents in database '{db_name}'")
            
            return {
                "created_count": len(documents),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "graph_type": graph_type
            }
            
        except Exception as e:
            logger.error(f"Failed to bulk create documents: {e}")
            raise DatabaseError(f"벌크 문서 생성 실패: {e}")
    
    async def bulk_update_documents(
        self,
        db_name: str,
        documents: List[Dict[str, Any]],
        graph_type: str = "instance",
        branch: str = "main",
        author: Optional[str] = None,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        여러 문서를 한 번에 업데이트
        
        Args:
            db_name: 데이터베이스 이름
            documents: 업데이트할 문서 목록
            graph_type: 그래프 타입
            author: 작성자
            message: 커밋 메시지
            
        Returns:
            업데이트 결과
        """
        try:
            # 벌크 업데이트 엔드포인트
            endpoint = (
                f"/api/document/{self.connection_info.account}/{db_name}{self._branch_descriptor(branch)}"
            )
            params = {"graph_type": graph_type}
            
            if author:
                params["author"] = author
            if message:
                params["message"] = message
            
            # JSON-LD 그래프 형식으로 래핑
            data = {"@graph": documents}
            
            await self._make_request("PUT", endpoint, data, params=params)
            
            logger.info(f"Bulk updated {len(documents)} documents in database '{db_name}'")
            
            return {
                "updated_count": len(documents),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "graph_type": graph_type
            }
            
        except Exception as e:
            logger.error(f"Failed to bulk update documents: {e}")
            raise DatabaseError(f"벌크 문서 업데이트 실패: {e}")
    
    async def bulk_delete_documents(
        self,
        db_name: str,
        doc_ids: List[str],
        graph_type: str = "instance",
        branch: str = "main",
        author: Optional[str] = None,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        여러 문서를 한 번에 삭제
        
        Args:
            db_name: 데이터베이스 이름
            doc_ids: 삭제할 문서 ID 목록
            graph_type: 그래프 타입
            author: 작성자
            message: 커밋 메시지
            
        Returns:
            삭제 결과
        """
        try:
            deleted_count = 0
            
            # 각 문서를 개별적으로 삭제 (TerminusDB는 벌크 삭제 미지원)
            for doc_id in doc_ids:
                try:
                    await self.delete_document(
                        db_name, doc_id, graph_type, branch, author, message
                    )
                    deleted_count += 1
                except OntologyNotFoundError:
                    logger.warning(f"Document '{doc_id}' not found, skipping")
                    continue
            
            logger.info(f"Bulk deleted {deleted_count} documents from database '{db_name}'")
            
            return {
                "deleted_count": deleted_count,
                "deleted_at": datetime.now(timezone.utc).isoformat(),
                "graph_type": graph_type
            }
            
        except Exception as e:
            logger.error(f"Failed to bulk delete documents: {e}")
            raise DatabaseError(f"벌크 문서 삭제 실패: {e}")
    
    async def search_documents(
        self,
        db_name: str,
        search_query: str,
        graph_type: str = "instance",
        branch: str = "main",
        doc_type: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        문서 검색
        
        Args:
            db_name: 데이터베이스 이름
            search_query: 검색어
            graph_type: 그래프 타입
            doc_type: 문서 타입 필터
            limit: 최대 결과 수
            
        Returns:
            검색 결과
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            # SPARQL 검색 쿼리 구성
            type_filter = f"?doc a <{doc_type}> ." if doc_type else ""
            
            sparql_query = f"""
            SELECT DISTINCT ?doc ?prop ?value
            WHERE {{
                {type_filter}
                ?doc ?prop ?value .
                FILTER(
                    isLiteral(?value) && 
                    regex(str(?value), "{search_query}", "i")
                )
            }}
            LIMIT {limit}
            """
            
            endpoint = f"/api/sparql/{self.connection_info.account}/{db_name}"
            result = await self._make_request("POST", endpoint, {"query": sparql_query})
            
            # 결과를 문서별로 그룹화
            doc_map = {}
            
            if isinstance(result, dict) and "results" in result:
                bindings = result.get("results", {}).get("bindings", [])
                
                for binding in bindings:
                    doc_uri = binding.get("doc", {}).get("value", "")
                    doc_id = doc_uri.split("/")[-1] if "/" in doc_uri else doc_uri
                    
                    if doc_id not in doc_map:
                        doc_map[doc_id] = {"@id": doc_id}
                    
                    prop = binding.get("prop", {}).get("value", "")
                    value = binding.get("value", {}).get("value", "")
                    
                    prop_name = prop.split("/")[-1] if "/" in prop else prop
                    prop_name = prop_name.split("#")[-1] if "#" in prop_name else prop_name
                    
                    doc_map[doc_id][prop_name] = value
            
            return list(doc_map.values())
            
        except Exception as e:
            logger.error(f"Failed to search documents: {e}")
            raise DatabaseError(f"문서 검색 실패: {e}")
    
    async def create_instance(
        self,
        db_name: str,
        class_id: str,
        instance_data: Dict[str, Any],
        branch: str = "main",
    ) -> Dict[str, Any]:
        """
        Create an instance document (alias for create_document)
        
        Args:
            db_name: Database name
            class_id: Class/Type ID
            instance_data: Instance data
            
        Returns:
            Created instance info
        """
        # Ensure @type is set to the class_id
        if "@type" not in instance_data:
            instance_data["@type"] = class_id
        
        # Ensure @id is set
        if "@id" not in instance_data and "instance_id" in instance_data:
            instance_data["@id"] = instance_data["instance_id"]
        
        return await self.create_document(
            db_name=db_name,
            document=instance_data,
            graph_type="instance",
            branch=branch,
            author="system",
            message=f"Created instance of {class_id}"
        )
    
    async def update_instance(
        self,
        db_name: str,
        class_id: str,
        instance_id: str,
        update_data: Dict[str, Any],
        branch: str = "main",
    ) -> Dict[str, Any]:
        """
        Update an instance document (alias for update_document)
        
        Args:
            db_name: Database name
            class_id: Class/Type ID
            instance_id: Instance ID
            update_data: Update data
            
        Returns:
            Updated instance info
        """
        # Ensure @id and @type are set
        update_data["@id"] = instance_id
        update_data["@type"] = class_id
        
        return await self.update_document(
            db_name=db_name,
            doc_id=instance_id,
            document=update_data,
            graph_type="instance",
            branch=branch,
            author="system",
            message=f"Updated instance {instance_id}"
        )
    
    async def delete_instance(
        self,
        db_name: str,
        class_id: str,
        instance_id: str,
        branch: str = "main",
    ) -> bool:
        """
        Delete an instance document (alias for delete_document)
        
        Args:
            db_name: Database name
            class_id: Class/Type ID
            instance_id: Instance ID
            
        Returns:
            True if deleted successfully
        """
        return await self.delete_document(
            db_name=db_name,
            doc_id=instance_id,
            graph_type="instance",
            branch=branch,
            author="system",
            message=f"Deleted instance {instance_id}"
        )
