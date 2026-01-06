"""
Pull Request Service for SPICE HARVESTER
Implements GitHub-like PR workflow using TerminusDB branches and PostgreSQL metadata

SOLID Principles:
- SRP: Only handles Pull Request operations
- OCP: Extends BaseTerminusService without modifying it
- LSP: Can substitute BaseTerminusService anywhere
- ISP: Only exposes PR-related interfaces
- DIP: Depends on abstractions (MVCCTransactionManager, BaseTerminusService)
"""

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import uuid

from oms.services.terminus.base import BaseTerminusService
from oms.services.terminus.version_control import VersionControlService
from oms.database.mvcc import MVCCTransactionManager, IsolationLevel, MVCCError
from oms.database.decorators import with_mvcc_retry, with_optimistic_lock
from shared.models.base import OptimisticLockError
from oms.exceptions import DatabaseError
from shared.utils.commit_utils import coerce_commit_id
from shared.utils.terminus_branch import encode_branch_name
from shared.utils.diff_utils import normalize_diff_response

logger = logging.getLogger(__name__)


class PullRequestStatus:
    """PR status constants"""
    OPEN = "open"
    MERGED = "merged"
    CLOSED = "closed"
    REJECTED = "rejected"


def _maybe_decode_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


class PullRequestService(BaseTerminusService):
    """
    Pull Request management service following SRP
    
    This service handles ONLY Pull Request operations:
    - Creating PRs
    - Getting diffs between branches
    - Merging PRs (rebase + merge)
    - Managing PR metadata in PostgreSQL
    
    It does NOT handle:
    - Branch operations (delegated to VersionControlService)
    - Database operations (delegated to DatabaseService)
    - Direct TerminusDB API calls (inherited from BaseTerminusService)
    """
    
    def __init__(self, mvcc_manager: MVCCTransactionManager, *args, **kwargs):
        """
        Initialize PullRequestService
        
        Args:
            mvcc_manager: MVCC transaction manager for PostgreSQL operations
            *args, **kwargs: Arguments for BaseTerminusService
        """
        super().__init__(*args, **kwargs)
        self.mvcc = mvcc_manager
        # Compose with VersionControlService for branch operations
        self.version_control = VersionControlService(*args, **kwargs)
    
    @with_mvcc_retry()
    async def create_pull_request(
        self,
        db_name: str,
        source_branch: str,
        target_branch: str,
        title: str,
        description: Optional[str] = None,
        author: str = "system"
    ) -> Dict[str, Any]:
        """
        Create a new pull request
        
        Args:
            db_name: Database name
            source_branch: Source branch name
            target_branch: Target branch name
            title: PR title
            description: PR description
            author: PR author
            
        Returns:
            PR information including ID and diff
            
        Raises:
            DatabaseError: If PR creation fails
            MVCCError: If concurrency conflict occurs
        """
        try:
            # First, verify both branches exist
            branches = await self.version_control.list_branches(db_name)
            branch_names = [b["name"] for b in branches]
            
            if source_branch not in branch_names:
                raise DatabaseError(f"Source branch '{source_branch}' does not exist")
            if target_branch not in branch_names:
                raise DatabaseError(f"Target branch '{target_branch}' does not exist")
            
            # Get diff between branches
            diff = await self.get_branch_diff(db_name, source_branch, target_branch)
            
            # Check for conflicts
            conflicts = await self.check_merge_conflicts(db_name, source_branch, target_branch)
            diff_payload = json.dumps(diff) if diff is not None else None
            conflicts_payload = json.dumps(conflicts) if conflicts is not None else None
            
            # Store PR metadata in PostgreSQL with MVCC
            pr_id = None
            source_commit_id = None
            for branch in branches:
                if branch.get("name") == source_branch:
                    source_commit_id = coerce_commit_id(branch.get("head"))
                    break
            async with self.mvcc.transaction(IsolationLevel.REPEATABLE_READ) as conn:
                # Check if an open PR already exists for these branches
                existing = await conn.fetchrow("""
                    SELECT id FROM pull_requests
                    WHERE db_name = $1 AND source_branch = $2 
                    AND target_branch = $3 AND status = $4
                """, db_name, source_branch, target_branch, PullRequestStatus.OPEN)
                
                if existing:
                    raise DatabaseError(
                        f"An open PR already exists for {source_branch} -> {target_branch}"
                    )
                
                # Create new PR
                pr_id = await conn.fetchval("""
                    INSERT INTO pull_requests 
                    (db_name, source_branch, target_branch, title, description, 
                     author, status, version, diff_cache, conflicts, source_commit_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $9, $10)
                    RETURNING id
                """, db_name, source_branch, target_branch, title, description,
                    author, PullRequestStatus.OPEN,
                    diff_payload,
                    conflicts_payload,
                    source_commit_id)
            
            logger.info(f"Created PR {pr_id}: {source_branch} -> {target_branch} in {db_name}")
            
            return {
                "id": str(pr_id),
                "db_name": db_name,
                "source_branch": source_branch,
                "target_branch": target_branch,
                "title": title,
                "description": description,
                "author": author,
                "status": PullRequestStatus.OPEN,
                "diff": diff,
                "conflicts": conflicts,
                "has_conflicts": bool(conflicts),
                "source_commit_id": source_commit_id,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            
        except MVCCError as e:
            logger.error(f"MVCC error creating PR: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to create pull request: {e}")
            raise DatabaseError(f"Failed to create pull request: {e}")
    
    async def get_branch_diff(
        self,
        db_name: str,
        source_branch: str,
        target_branch: str
    ) -> Dict[str, Any]:
        """
        Get diff between two branches using TerminusDB diff API
        
        Args:
            db_name: Database name
            source_branch: Source branch name
            target_branch: Target branch name
            
        Returns:
            Diff information
        """
        try:
            result = await self.version_control.get_diff(db_name, source_branch, target_branch)

            return normalize_diff_response(source_branch, target_branch, result)
            
        except Exception as e:
            logger.error(f"Failed to get branch diff: {e}")
            diff_payload = normalize_diff_response(source_branch, target_branch, None)
            diff_payload["error"] = str(e)
            return diff_payload
    
    async def check_merge_conflicts(
        self,
        db_name: str,
        source_branch: str,
        target_branch: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Check for potential merge conflicts
        
        Args:
            db_name: Database name
            source_branch: Source branch name
            target_branch: Target branch name
            
        Returns:
            List of conflicts if any, None otherwise
        """
        try:
            # Try a dry-run rebase to detect conflicts
            encoded_source = encode_branch_name(source_branch)
            encoded_target = encode_branch_name(target_branch)
            endpoint = (
                f"/api/rebase/{self.connection_info.account}/{db_name}/local/branch/{encoded_target}"
            )
            
            rebase_data = {
                "rebase_from": f"{self.connection_info.account}/{db_name}/local/branch/{encoded_source}",
                "author": "system",
                "dry_run": True  # Just check, don't actually rebase
            }
            
            try:
                result = await self._make_request("POST", endpoint, rebase_data)
                # If dry run succeeds, no conflicts
                return None
            except Exception as e:
                # If dry run fails, there might be conflicts
                if "conflict" in str(e).lower():
                    return [{"type": "merge_conflict", "message": str(e)}]
                # Other errors are not conflicts
                return None
                
        except Exception as e:
            logger.warning(f"Could not check for conflicts: {e}")
            return None
    
    @with_mvcc_retry()
    @with_optimistic_lock(entity_type="PullRequest")
    async def merge_pull_request(
        self,
        pr_id: str,
        merge_message: Optional[str] = None,
        author: str = "system"
    ) -> Dict[str, Any]:
        """
        Merge a pull request using rebase strategy
        
        Args:
            pr_id: Pull request ID
            merge_message: Custom merge commit message
            author: Person performing the merge
            
        Returns:
            Merge result information
            
        Raises:
            DatabaseError: If merge fails
            OptimisticLockError: If PR was modified concurrently
        """
        try:
            pr_data = None
            
            # Get PR data with optimistic locking
            async with self.mvcc.transaction(IsolationLevel.REPEATABLE_READ) as conn:
                pr_data = await conn.fetchrow("""
                    SELECT * FROM pull_requests
                    WHERE id = $1 AND status = $2
                    FOR UPDATE
                """, uuid.UUID(pr_id), PullRequestStatus.OPEN)
                
                if not pr_data:
                    raise DatabaseError(f"PR {pr_id} not found or not open")

                source_commit_id = pr_data.get("source_commit_id")
                if source_commit_id:
                    branches = await self.version_control.list_branches(pr_data["db_name"])
                    head_commit_id = None
                    for branch in branches:
                        if branch.get("name") == pr_data["source_branch"]:
                            head_commit_id = coerce_commit_id(branch.get("head"))
                            break
                    if head_commit_id and head_commit_id != source_commit_id:
                        raise DatabaseError(
                            "Source branch head has moved since proposal creation; re-propose to merge"
                        )
                
                # Perform rebase in TerminusDB
                encoded_source = encode_branch_name(pr_data["source_branch"])
                encoded_target = encode_branch_name(pr_data["target_branch"])
                endpoint = (
                    f"/api/rebase/{self.connection_info.account}/{pr_data['db_name']}/local/branch/{encoded_target}"
                )
                
                rebase_data = {
                    "rebase_from": (
                        f"{self.connection_info.account}/{pr_data['db_name']}/local/branch/{encoded_source}"
                    ),
                    "author": author,
                    "message": merge_message or f"Merge PR: {pr_data['title']}"
                }
                
                # Execute rebase (updates source branch head)
                await self._make_request("POST", endpoint, rebase_data)

                def _find_branch_head(branches, name: str) -> Optional[str]:
                    for branch in branches or []:
                        if branch.get("name") == name:
                            return coerce_commit_id(branch.get("head"))
                    return None

                branches = await self.version_control.list_branches(pr_data["db_name"])
                source_head = _find_branch_head(branches, pr_data["source_branch"])
                merge_commit = _find_branch_head(branches, pr_data["target_branch"]) or source_head
                
                # Update PR status in PostgreSQL
                await conn.execute("""
                    UPDATE pull_requests
                    SET status = $1, version = version + 1,
                        merge_commit_id = $2, merged_at = NOW()
                    WHERE id = $3
                """, PullRequestStatus.MERGED, merge_commit, uuid.UUID(pr_id))
            
            logger.info(f"Merged PR {pr_id}: {pr_data['source_branch']} -> {pr_data['target_branch']}")
            
            return {
                "id": pr_id,
                "status": PullRequestStatus.MERGED,
                "merge_commit": merge_commit,
                "merged_at": datetime.now(timezone.utc).isoformat(),
                "merged_by": author,
                "source_branch": pr_data['source_branch'],
                "target_branch": pr_data['target_branch']
            }
            
        except OptimisticLockError as e:
            logger.error(f"Concurrent modification of PR {pr_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to merge PR {pr_id}: {e}")
            raise DatabaseError(f"Failed to merge pull request: {e}")
    
    async def get_pull_request(self, pr_id: str) -> Optional[Dict[str, Any]]:
        """
        Get pull request details
        
        Args:
            pr_id: Pull request ID
            
        Returns:
            PR information or None if not found
        """
        try:
            async with self.mvcc.transaction(IsolationLevel.READ_COMMITTED, read_only=True) as conn:
                pr_data = await conn.fetchrow("""
                    SELECT * FROM pull_requests WHERE id = $1
                """, uuid.UUID(pr_id))
                
                if not pr_data:
                    return None
                diff_cache = _maybe_decode_json(pr_data['diff_cache'])
                if diff_cache is not None:
                    diff_cache = normalize_diff_response(
                        pr_data['source_branch'],
                        pr_data['target_branch'],
                        diff_cache,
                    )

                return {
                    "id": str(pr_data['id']),
                    "db_name": pr_data['db_name'],
                    "source_branch": pr_data['source_branch'],
                    "target_branch": pr_data['target_branch'],
                    "title": pr_data['title'],
                    "description": pr_data['description'],
                    "author": pr_data['author'],
                    "status": pr_data['status'],
                    "version": pr_data['version'],
                    "diff_cache": diff_cache,
                    "conflicts": _maybe_decode_json(pr_data['conflicts']),
                    "merge_commit_id": pr_data['merge_commit_id'],
                    "source_commit_id": pr_data.get("source_commit_id"),
                    "created_at": pr_data['created_at'].isoformat() if pr_data['created_at'] else None,
                    "updated_at": pr_data['updated_at'].isoformat() if pr_data['updated_at'] else None,
                    "merged_at": pr_data['merged_at'].isoformat() if pr_data['merged_at'] else None
                }
                
        except Exception as e:
            logger.error(f"Failed to get PR {pr_id}: {e}")
            return None
    
    async def list_pull_requests(
        self,
        db_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List pull requests with optional filters
        
        Args:
            db_name: Filter by database name
            status: Filter by status
            limit: Maximum number of results
            
        Returns:
            List of pull requests
        """
        try:
            async with self.mvcc.transaction(IsolationLevel.READ_COMMITTED, read_only=True) as conn:
                query = "SELECT * FROM pull_requests WHERE 1=1"
                params = []
                
                if db_name:
                    params.append(db_name)
                    query += f" AND db_name = ${len(params)}"
                
                if status:
                    params.append(status)
                    query += f" AND status = ${len(params)}"
                
                query += f" ORDER BY created_at DESC LIMIT {limit}"
                
                rows = await conn.fetch(query, *params)
                
                return [
                    {
                        "id": str(row['id']),
                        "db_name": row['db_name'],
                        "source_branch": row['source_branch'],
                        "target_branch": row['target_branch'],
                        "title": row['title'],
                        "author": row['author'],
                        "status": row['status'],
                        "source_commit_id": row.get("source_commit_id"),
                        "created_at": row['created_at'].isoformat() if row['created_at'] else None
                    }
                    for row in rows
                ]
                
        except Exception as e:
            logger.error(f"Failed to list pull requests: {e}")
            return []
    
    async def close_pull_request(
        self,
        pr_id: str,
        reason: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Close a pull request without merging
        
        Args:
            pr_id: Pull request ID
            reason: Reason for closing
            
        Returns:
            Updated PR information
        """
        try:
            async with self.mvcc.transaction(IsolationLevel.REPEATABLE_READ) as conn:
                pr_data = await conn.fetchrow("""
                    SELECT * FROM pull_requests
                    WHERE id = $1 AND status = $2
                    FOR UPDATE
                """, uuid.UUID(pr_id), PullRequestStatus.OPEN)
                
                if not pr_data:
                    raise DatabaseError(f"PR {pr_id} not found or not open")
                
                # Close the PR
                await conn.execute("""
                    UPDATE pull_requests
                    SET status = $1, version = version + 1
                    WHERE id = $2
                """, PullRequestStatus.CLOSED, uuid.UUID(pr_id))
            
            logger.info(f"Closed PR {pr_id}: {reason or 'No reason provided'}")
            
            return {
                "id": pr_id,
                "status": PullRequestStatus.CLOSED,
                "closed_at": datetime.now(timezone.utc).isoformat(),
                "reason": reason
            }
            
        except Exception as e:
            logger.error(f"Failed to close PR {pr_id}: {e}")
            raise DatabaseError(f"Failed to close pull request: {e}")
