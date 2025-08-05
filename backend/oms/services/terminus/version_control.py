"""
Version Control Service for TerminusDB
브랜치, 커밋, 머지 등 버전 관리 기능
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from .base import BaseTerminusService
from .database import DatabaseService
from oms.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class VersionControlService(BaseTerminusService):
    """
    TerminusDB 버전 관리 서비스
    
    브랜치 생성/삭제, 커밋, 머지, 이력 조회 등의 기능을 제공합니다.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # DatabaseService 인스턴스 (데이터베이스 존재 확인용)
        self.db_service = DatabaseService(*args, **kwargs)
    
    async def list_branches(self, db_name: str) -> List[Dict[str, Any]]:
        """
        데이터베이스의 모든 브랜치 목록 조회
        
        Args:
            db_name: 데이터베이스 이름
            
        Returns:
            브랜치 목록
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            endpoint = f"/api/branch/{self.connection_info.account}/{db_name}"
            result = await self._make_request("GET", endpoint)
            
            branches = []
            
            # 응답 파싱
            if isinstance(result, list):
                for branch_info in result:
                    if isinstance(branch_info, str):
                        branches.append({
                            "name": branch_info,
                            "head": None,
                            "created": None
                        })
                    elif isinstance(branch_info, dict):
                        branches.append({
                            "name": branch_info.get("name", branch_info.get("@id", "")),
                            "head": branch_info.get("head"),
                            "created": branch_info.get("created")
                        })
            elif isinstance(result, dict) and "@graph" in result:
                # JSON-LD 형식
                for branch_info in result["@graph"]:
                    branches.append({
                        "name": branch_info.get("name", branch_info.get("@id", "")),
                        "head": branch_info.get("head"),
                        "created": branch_info.get("created")
                    })
            
            return branches
            
        except Exception as e:
            logger.error(f"Failed to list branches: {e}")
            raise DatabaseError(f"브랜치 목록 조회 실패: {e}")
    
    async def create_branch(
        self,
        db_name: str,
        branch_name: str,
        source_branch: str = "main",
        empty: bool = False
    ) -> Dict[str, Any]:
        """
        새 브랜치 생성
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 새 브랜치 이름
            source_branch: 소스 브랜치 (기본값: main)
            empty: 빈 브랜치 생성 여부
            
        Returns:
            생성된 브랜치 정보
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            # 브랜치 존재 확인
            branches = await self.list_branches(db_name)
            if any(b["name"] == branch_name for b in branches):
                raise DatabaseError(f"브랜치 '{branch_name}'이(가) 이미 존재합니다")
            
            endpoint = f"/api/branch/{self.connection_info.account}/{db_name}"
            
            data = {
                "origin": f"{self.connection_info.account}/{db_name}/branch/{source_branch}",
                "branch": branch_name
            }
            
            if empty:
                data["empty"] = True
            
            await self._make_request("POST", endpoint, data)
            
            logger.info(f"Branch '{branch_name}' created in database '{db_name}'")
            
            return {
                "name": branch_name,
                "source": source_branch,
                "created_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to create branch: {e}")
            raise DatabaseError(f"브랜치 생성 실패: {e}")
    
    async def delete_branch(
        self,
        db_name: str,
        branch_name: str
    ) -> bool:
        """
        브랜치 삭제
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 삭제할 브랜치 이름
            
        Returns:
            삭제 성공 여부
        """
        try:
            # main 브랜치는 삭제 불가
            if branch_name == "main":
                raise DatabaseError("main 브랜치는 삭제할 수 없습니다")
            
            # 브랜치 존재 확인
            branches = await self.list_branches(db_name)
            if not any(b["name"] == branch_name for b in branches):
                raise DatabaseError(f"브랜치 '{branch_name}'을(를) 찾을 수 없습니다")
            
            endpoint = f"/api/branch/{self.connection_info.account}/{db_name}/branch/{branch_name}"
            await self._make_request("DELETE", endpoint)
            
            logger.info(f"Branch '{branch_name}' deleted from database '{db_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete branch: {e}")
            raise DatabaseError(f"브랜치 삭제 실패: {e}")
    
    async def reset_branch(
        self,
        db_name: str,
        branch_name: str,
        commit_id: str
    ) -> Dict[str, Any]:
        """
        브랜치를 특정 커밋으로 리셋
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 브랜치 이름
            commit_id: 대상 커밋 ID
            
        Returns:
            리셋 결과
        """
        try:
            endpoint = f"/api/reset/{self.connection_info.account}/{db_name}/branch/{branch_name}"
            
            data = {
                "commit": commit_id,
                "author": self.connection_info.user,
                "message": f"Reset branch {branch_name} to commit {commit_id}"
            }
            
            await self._make_request("POST", endpoint, data)
            
            logger.info(f"Branch '{branch_name}' reset to commit '{commit_id}'")
            
            return {
                "branch": branch_name,
                "reset_to": commit_id,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to reset branch: {e}")
            raise DatabaseError(f"브랜치 리셋 실패: {e}")
    
    async def get_commits(
        self,
        db_name: str,
        branch_name: str = "main",
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        브랜치의 커밋 이력 조회
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 브랜치 이름
            limit: 최대 결과 수
            offset: 시작 위치
            
        Returns:
            커밋 목록
        """
        try:
            await self.db_service.ensure_db_exists(db_name)
            
            endpoint = f"/api/log/{self.connection_info.account}/{db_name}/branch/{branch_name}"
            params = {
                "limit": limit,
                "offset": offset
            }
            
            result = await self._make_request("GET", endpoint, params=params)
            
            commits = []
            
            # 응답 파싱
            if isinstance(result, list):
                for commit_info in result:
                    commits.append(self._parse_commit_info(commit_info))
            elif isinstance(result, dict):
                if "@graph" in result:
                    for commit_info in result["@graph"]:
                        commits.append(self._parse_commit_info(commit_info))
                elif "commits" in result:
                    for commit_info in result["commits"]:
                        commits.append(self._parse_commit_info(commit_info))
            
            return commits
            
        except Exception as e:
            logger.error(f"Failed to get commits: {e}")
            raise DatabaseError(f"커밋 이력 조회 실패: {e}")
    
    async def create_commit(
        self,
        db_name: str,
        branch_name: str,
        message: str,
        author: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        새 커밋 생성
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 브랜치 이름
            message: 커밋 메시지
            author: 작성자 (기본값: 연결 사용자)
            
        Returns:
            생성된 커밋 정보
        """
        try:
            endpoint = f"/api/woql/{self.connection_info.account}/{db_name}/branch/{branch_name}"
            
            # 빈 WOQL 쿼리로 커밋 생성
            data = {
                "query": {"@type": "True"},  # 빈 쿼리
                "author": author or self.connection_info.user,
                "message": message
            }
            
            result = await self._make_request("POST", endpoint, data)
            
            logger.info(f"Commit created in branch '{branch_name}' of database '{db_name}'")
            
            return {
                "branch": branch_name,
                "message": message,
                "author": author or self.connection_info.user,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to create commit: {e}")
            raise DatabaseError(f"커밋 생성 실패: {e}")
    
    async def rebase_branch(
        self,
        db_name: str,
        branch_name: str,
        target_branch: str = "main",
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        브랜치 리베이스
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 리베이스할 브랜치
            target_branch: 대상 브랜치
            message: 리베이스 메시지
            
        Returns:
            리베이스 결과
        """
        try:
            endpoint = f"/api/rebase/{self.connection_info.account}/{db_name}"
            
            data = {
                "rebase_from": f"{self.connection_info.account}/{db_name}/branch/{branch_name}",
                "rebase_to": f"{self.connection_info.account}/{db_name}/branch/{target_branch}",
                "author": self.connection_info.user,
                "message": message or f"Rebase {branch_name} onto {target_branch}"
            }
            
            result = await self._make_request("POST", endpoint, data)
            
            logger.info(f"Branch '{branch_name}' rebased onto '{target_branch}'")
            
            return {
                "branch": branch_name,
                "rebased_onto": target_branch,
                "result": result,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to rebase branch: {e}")
            raise DatabaseError(f"브랜치 리베이스 실패: {e}")
    
    async def squash_commits(
        self,
        db_name: str,
        branch_name: str,
        commit_id: str,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        커밋 스쿼시 (여러 커밋을 하나로 합치기)
        
        Args:
            db_name: 데이터베이스 이름
            branch_name: 브랜치 이름
            commit_id: 스쿼시 대상 커밋 ID
            message: 새 커밋 메시지
            
        Returns:
            스쿼시 결과
        """
        try:
            endpoint = f"/api/squash/{self.connection_info.account}/{db_name}/branch/{branch_name}"
            
            data = {
                "commit": commit_id,
                "author": self.connection_info.user,
                "message": message or f"Squashed commits up to {commit_id}"
            }
            
            result = await self._make_request("POST", endpoint, data)
            
            logger.info(f"Commits squashed in branch '{branch_name}'")
            
            return {
                "branch": branch_name,
                "squashed_to": commit_id,
                "result": result,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to squash commits: {e}")
            raise DatabaseError(f"커밋 스쿼시 실패: {e}")
    
    async def get_diff(
        self,
        db_name: str,
        from_ref: str,
        to_ref: str
    ) -> Dict[str, Any]:
        """
        두 참조(브랜치/커밋) 간의 차이점 조회
        
        Args:
            db_name: 데이터베이스 이름
            from_ref: 시작 참조 (브랜치명 또는 커밋 ID)
            to_ref: 대상 참조 (브랜치명 또는 커밋 ID)
            
        Returns:
            차이점 정보
        """
        try:
            endpoint = f"/api/diff/{self.connection_info.account}/{db_name}"
            params = {
                "before": from_ref,
                "after": to_ref
            }
            
            result = await self._make_request("GET", endpoint, params=params)
            
            return {
                "from": from_ref,
                "to": to_ref,
                "changes": result
            }
            
        except Exception as e:
            logger.error(f"Failed to get diff: {e}")
            raise DatabaseError(f"차이점 조회 실패: {e}")
    
    def _parse_commit_info(self, commit_data: Any) -> Dict[str, Any]:
        """커밋 정보 파싱"""
        if isinstance(commit_data, str):
            return {
                "id": commit_data,
                "message": "",
                "author": "",
                "timestamp": None
            }
        elif isinstance(commit_data, dict):
            return {
                "id": commit_data.get("id", commit_data.get("@id", "")),
                "message": commit_data.get("message", commit_data.get("comment", "")),
                "author": commit_data.get("author", ""),
                "timestamp": commit_data.get("timestamp", commit_data.get("created")),
                "parent": commit_data.get("parent"),
                "branch": commit_data.get("branch")
            }
        else:
            return {
                "id": str(commit_data),
                "message": "",
                "author": "",
                "timestamp": None
            }