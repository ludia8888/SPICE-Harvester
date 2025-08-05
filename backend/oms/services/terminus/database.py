"""
Database Service for TerminusDB
데이터베이스 생성, 삭제, 목록 조회 등 데이터베이스 관리 기능
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from .base import BaseTerminusService, async_terminus_retry
from oms.exceptions import (
    DatabaseError,
    DuplicateOntologyError,
    OntologyNotFoundError
)

logger = logging.getLogger(__name__)


class DatabaseService(BaseTerminusService):
    """
    TerminusDB 데이터베이스 관리 서비스
    
    데이터베이스 생성, 삭제, 목록 조회 등의 기능을 제공합니다.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 데이터베이스 캐시
        self._db_cache: set = set()
    
    async def database_exists(self, db_name: str) -> bool:
        """데이터베이스 존재 여부 확인"""
        if db_name in self._db_cache:
            return True
            
        try:
            endpoint = f"/api/db/{self.connection_info.account}/{db_name}"
            await self._make_request("GET", endpoint)
            self._db_cache.add(db_name)
            return True
        except Exception:
            return False
    
    async def ensure_db_exists(self, db_name: str, description: Optional[str] = None) -> None:
        """
        데이터베이스 존재 확인 및 생성
        
        데이터베이스가 없으면 자동으로 생성합니다.
        """
        if await self.database_exists(db_name):
            return
            
        logger.info(f"Database '{db_name}' not found, creating...")
        
        try:
            await self.create_database(db_name, description)
            self._db_cache.add(db_name)
        except Exception as e:
            logger.error(f"Error ensuring database exists: {e}")
            raise DatabaseError(f"데이터베이스 생성/확인 실패: {e}")
    
    async def create_database(
        self, db_name: str, description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        새 데이터베이스 생성
        
        Args:
            db_name: 데이터베이스 이름
            description: 데이터베이스 설명
            
        Returns:
            생성 결과
        """
        # 중복 검사
        if await self.database_exists(db_name):
            raise DuplicateOntologyError(f"데이터베이스 '{db_name}'이(가) 이미 존재합니다")
        
        endpoint = f"/api/db/{self.connection_info.account}/{db_name}"
        
        # TerminusDB 데이터베이스 생성 요청 형식
        data = {
            "label": db_name,
            "comment": description or f"{db_name} database",
            "prefixes": {
                "@base": f"terminusdb:///{self.connection_info.account}/{db_name}/data/",
                "@schema": f"terminusdb:///{self.connection_info.account}/{db_name}/schema#",
            },
        }
        
        try:
            await self._make_request("POST", endpoint, data)
            self._db_cache.add(db_name)
            
            logger.info(f"Database '{db_name}' created successfully")
            
            return {"name": db_name, "created_at": datetime.utcnow().isoformat()}
            
        except Exception as e:
            logger.error(f"Failed to create database: {e}")
            raise DatabaseError(f"데이터베이스 생성 실패: {e}")
    
    async def list_databases(self) -> List[Dict[str, Any]]:
        """
        사용 가능한 데이터베이스 목록 조회
        
        Returns:
            데이터베이스 목록
        """
        try:
            endpoint = f"/api/db/{self.connection_info.account}"
            
            # TerminusDB의 bad descriptor path 에러 처리
            try:
                result = await self._make_request("GET", endpoint)
            except Exception as terminus_error:
                error_msg = str(terminus_error)
                
                # Bad descriptor path 에러인 경우
                if "bad descriptor path" in error_msg.lower():
                    logger.warning(f"TerminusDB has bad descriptor path error: {error_msg}")
                    logger.warning("This indicates stale database references in TerminusDB")
                    logger.info("Returning empty database list due to TerminusDB internal error")
                    return []
                else:
                    raise
            
            # 응답 파싱
            databases = []
            
            # TerminusDB 응답 형식 처리
            if isinstance(result, list):
                db_list = result
            elif isinstance(result, dict):
                # 여러 가능한 키 확인
                if "@graph" in result:
                    db_list = result["@graph"]
                elif "databases" in result:
                    db_list = result["databases"]
                elif "dbs" in result:
                    db_list = result["dbs"]
                else:
                    db_list = []
                    logger.warning(f"Unknown TerminusDB response format: {result}")
            else:
                db_list = []
            
            # 데이터베이스 정보 파싱
            for db_info in db_list:
                db_name = None
                
                if isinstance(db_info, str):
                    db_name = db_info
                elif isinstance(db_info, dict):
                    # 여러 가능한 키 시도
                    db_name = db_info.get("name") or db_info.get("id") or db_info.get("@id")
                    
                    # path 형식 처리
                    if not db_name and "path" in db_info:
                        path = db_info.get("path", "")
                        if "/" in path:
                            _, db_name = path.split("/", 1)
                
                if db_name:
                    databases.append({
                        "name": db_name,
                        "label": (
                            db_info.get("label", db_name)
                            if isinstance(db_info, dict)
                            else db_name
                        ),
                        "comment": (
                            db_info.get("comment", f"Database {db_name}")
                            if isinstance(db_info, dict)
                            else f"Database {db_name}"
                        ),
                        "created": (
                            db_info.get("created") if isinstance(db_info, dict) else None
                        ),
                        "path": (
                            db_info.get("path")
                            if isinstance(db_info, dict)
                            else f"{self.connection_info.account}/{db_name}"
                        ),
                    })
                    self._db_cache.add(db_name)
            
            return databases
            
        except Exception as e:
            logger.error(f"Failed to list databases: {e}")
            raise DatabaseError(f"데이터베이스 목록 조회 실패: {e}")
    
    @async_terminus_retry(max_retries=3)
    async def delete_database(self, db_name: str) -> bool:
        """
        데이터베이스 삭제
        
        Args:
            db_name: 삭제할 데이터베이스 이름
            
        Returns:
            삭제 성공 여부
        """
        try:
            # 데이터베이스 존재 여부 확인
            if not await self.database_exists(db_name):
                raise OntologyNotFoundError(f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다")
            
            # TerminusDB 데이터베이스 삭제 엔드포인트
            endpoint = f"/api/db/{self.connection_info.account}/{db_name}"
            
            await self._make_request("DELETE", endpoint)
            
            # 캐시에서 제거
            self._db_cache.discard(db_name)
            
            logger.info(f"Database '{db_name}' deleted successfully")
            return True
            
        except OntologyNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Failed to delete database: {e}")
            raise DatabaseError(f"데이터베이스 삭제 실패: {e}")
    
    async def get_database_info(self, db_name: str) -> Dict[str, Any]:
        """
        데이터베이스 상세 정보 조회
        
        Args:
            db_name: 데이터베이스 이름
            
        Returns:
            데이터베이스 정보
        """
        try:
            if not await self.database_exists(db_name):
                raise OntologyNotFoundError(f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다")
            
            endpoint = f"/api/db/{self.connection_info.account}/{db_name}"
            result = await self._make_request("GET", endpoint)
            
            return {
                "name": db_name,
                "exists": True,
                "info": result
            }
            
        except OntologyNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            raise DatabaseError(f"데이터베이스 정보 조회 실패: {e}")
    
    def clear_cache(self):
        """데이터베이스 캐시 초기화"""
        self._db_cache.clear()
        logger.info("Database cache cleared")