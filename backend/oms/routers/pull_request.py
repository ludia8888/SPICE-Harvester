"""
Pull Request Router for OMS
Implements GitHub-like PR REST API endpoints

Following SOLID principles:
- SRP: Only handles HTTP routing for Pull Requests
- DIP: Depends on abstractions (PullRequestService)
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ConfigDict

from oms.dependencies import TerminusServiceDep
from oms.services.pull_request_service import PullRequestService, PullRequestStatus
from oms.database.postgres import db as postgres_db
from shared.models.requests import ApiResponse
from shared.models.base import OptimisticLockError
from shared.security.input_sanitizer import validate_db_name, validate_branch_name, sanitize_input
from oms.exceptions import DatabaseError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}/pull-requests", tags=["Pull Requests"])


class PRCreateRequest(BaseModel):
    """Pull Request creation request model"""
    source_branch: str = Field(..., description="Source branch name")
    target_branch: str = Field(..., description="Target branch name")
    title: str = Field(..., min_length=1, max_length=500, description="PR title")
    description: Optional[str] = Field(None, max_length=5000, description="PR description")
    author: str = Field(default="system", description="PR author")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "source_branch": "feature/new-ontology",
                "target_branch": "main",
                "title": "Add Product ontology",
                "description": "This PR adds a new Product ontology with price and description fields",
                "author": "developer",
            }
        }
    )


class PRMergeRequest(BaseModel):
    """Pull Request merge request model"""
    merge_message: Optional[str] = Field(None, max_length=1000, description="Custom merge message")
    author: str = Field(default="system", description="Person performing the merge")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "merge_message": "Merge PR: Add Product ontology",
                "author": "maintainer",
            }
        }
    )


class PRCloseRequest(BaseModel):
    """Pull Request close request model"""
    reason: Optional[str] = Field(None, max_length=500, description="Reason for closing")


# Dependency to get PullRequestService
async def get_pr_service() -> PullRequestService:
    """Get PullRequestService instance with MVCC support"""
    if not postgres_db.mvcc_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not initialized"
        )
    
    # Create PullRequestService with MVCC manager
    return PullRequestService(postgres_db.mvcc_manager)


@router.post("")
async def create_pull_request(
    db_name: str,
    request: PRCreateRequest,
    pr_service: PullRequestService = Depends(get_pr_service)
):
    """
    Create a new pull request
    
    Creates a pull request to merge changes from source branch to target branch.
    Automatically calculates diff and checks for conflicts.
    """
    try:
        # Validate inputs
        db_name = validate_db_name(db_name)
        request.source_branch = validate_branch_name(request.source_branch)
        request.target_branch = validate_branch_name(request.target_branch)
        
        # Sanitize text fields
        sanitized_data = sanitize_input(request.model_dump(mode="json"))
        
        # Create PR
        result = await pr_service.create_pull_request(
            db_name=db_name,
            source_branch=request.source_branch,
            target_branch=request.target_branch,
            title=sanitized_data['title'],
            description=sanitized_data.get('description'),
            author=sanitized_data.get('author', 'system')
        )
        
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=ApiResponse.created(
                message=f"Pull request created: {request.source_branch} -> {request.target_branch}",
                data=result
            ).to_dict()
        )
        
    except DatabaseError as e:
        logger.error(f"Database error creating PR: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to create pull request: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create pull request: {str(e)}"
        )


@router.get("/{pr_id}")
async def get_pull_request(
    db_name: str,
    pr_id: str,
    pr_service: PullRequestService = Depends(get_pr_service)
):
    """
    Get pull request details
    
    Returns detailed information about a specific pull request.
    """
    try:
        # Validate inputs
        db_name = validate_db_name(db_name)
        
        # Get PR
        pr_data = await pr_service.get_pull_request(pr_id)
        
        if not pr_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pull request {pr_id} not found"
            )
        
        # Verify PR belongs to this database
        if pr_data['db_name'] != db_name:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pull request {pr_id} not found in database {db_name}"
            )
        
        return ApiResponse.success(
            message="Pull request retrieved successfully",
            data=pr_data
        ).to_dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get pull request {pr_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get pull request: {str(e)}"
        )


@router.get("")
async def list_pull_requests(
    db_name: str,
    status: Optional[str] = Query(None, description="Filter by status (open, merged, closed)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    pr_service: PullRequestService = Depends(get_pr_service)
):
    """
    List pull requests for a database
    
    Returns a list of pull requests with optional status filtering.
    """
    try:
        # Validate inputs
        db_name = validate_db_name(db_name)
        
        if status and status not in ["open", "merged", "closed", "rejected"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status: {status}"
            )
        
        # List PRs
        prs = await pr_service.list_pull_requests(
            db_name=db_name,
            status=status,
            limit=limit
        )
        
        return ApiResponse.success(
            message=f"Found {len(prs)} pull requests",
            data={
                "pull_requests": prs,
                "total": len(prs),
                "filters": {
                    "db_name": db_name,
                    "status": status,
                    "limit": limit
                }
            }
        ).to_dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list pull requests: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list pull requests: {str(e)}"
        )


@router.post("/{pr_id}/merge")
async def merge_pull_request(
    db_name: str,
    pr_id: str,
    request: PRMergeRequest,
    pr_service: PullRequestService = Depends(get_pr_service)
):
    """
    Merge a pull request
    
    Merges the source branch into the target branch using rebase strategy.
    The PR must be open and have no conflicts.
    """
    try:
        # Validate inputs
        db_name = validate_db_name(db_name)
        sanitized_data = sanitize_input(request.model_dump(mode="json"))
        
        # Merge PR
        result = await pr_service.merge_pull_request(
            pr_id=pr_id,
            merge_message=sanitized_data.get('merge_message'),
            author=sanitized_data.get('author', 'system')
        )
        
        return ApiResponse.success(
            message=f"Pull request {pr_id} merged successfully",
            data=result
        ).to_dict()
        
    except OptimisticLockError as e:
        logger.warning(f"Concurrent modification of PR {pr_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Pull request was modified by another process. Please retry."
        )
    except DatabaseError as e:
        logger.error(f"Database error merging PR {pr_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to merge pull request {pr_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to merge pull request: {str(e)}"
        )


@router.post("/{pr_id}/close")
async def close_pull_request(
    db_name: str,
    pr_id: str,
    request: PRCloseRequest,
    pr_service: PullRequestService = Depends(get_pr_service)
):
    """
    Close a pull request without merging
    
    Closes the pull request without merging the changes.
    """
    try:
        # Validate inputs
        db_name = validate_db_name(db_name)
        sanitized_data = sanitize_input(request.model_dump(mode="json"))
        
        # Close PR
        result = await pr_service.close_pull_request(
            pr_id=pr_id,
            reason=sanitized_data.get('reason')
        )
        
        return ApiResponse.success(
            message=f"Pull request {pr_id} closed",
            data=result
        ).to_dict()
        
    except DatabaseError as e:
        logger.error(f"Database error closing PR {pr_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Failed to close pull request {pr_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to close pull request: {str(e)}"
        )


@router.get("/{pr_id}/diff")
async def get_pull_request_diff(
    db_name: str,
    pr_id: str,
    refresh: bool = Query(False, description="Refresh diff from TerminusDB"),
    pr_service: PullRequestService = Depends(get_pr_service)
):
    """
    Get diff for a pull request
    
    Returns the diff between source and target branches.
    Can optionally refresh the diff from TerminusDB.
    """
    try:
        # Validate inputs
        db_name = validate_db_name(db_name)
        
        # Get PR details
        pr_data = await pr_service.get_pull_request(pr_id)
        
        if not pr_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pull request {pr_id} not found"
            )
        
        # Get diff (from cache or fresh)
        if refresh or not pr_data.get('diff_cache'):
            diff = await pr_service.get_branch_diff(
                db_name=pr_data['db_name'],
                source_branch=pr_data['source_branch'],
                target_branch=pr_data['target_branch']
            )
        else:
            diff = pr_data['diff_cache']
        
        return ApiResponse.success(
            message="Diff retrieved successfully",
            data={
                "pr_id": pr_id,
                "source_branch": pr_data['source_branch'],
                "target_branch": pr_data['target_branch'],
                "diff": diff,
                "from_cache": not refresh and bool(pr_data.get('diff_cache'))
            }
        ).to_dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get diff for PR {pr_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get diff: {str(e)}"
        )
