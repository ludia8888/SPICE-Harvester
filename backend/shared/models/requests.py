"""
Request models for SPICE HARVESTER services
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# Backwards compatibility: `ApiResponse` is defined in `responses.py` but historically
# imported from this module across services.
from .responses import ApiResponse  # noqa: F401


class BranchCreateRequest(BaseModel):
    """Request model for creating a branch"""

    branch_name: str = Field(..., description="Branch name")
    description: Optional[str] = Field(None, description="Branch description")
    from_branch: Optional[str] = Field(None, description="Create from specific branch")
    from_commit: Optional[str] = Field(None, description="Create from specific commit")


class CheckoutRequest(BaseModel):
    """Request model for checking out a branch or commit"""

    target: str = Field(..., description="Branch name or commit ID to checkout")
    target_type: str = Field(default="branch", description="Target type: 'branch' or 'commit'")


class CommitRequest(BaseModel):
    """Request model for creating a commit"""

    message: str = Field(..., description="Commit message")
    author: Optional[str] = Field(None, description="Commit author")
    changes: Optional[List[Dict[str, Any]]] = Field(None, description="List of changes")


class MergeRequest(BaseModel):
    """Request model for merging branches"""

    source_branch: str = Field(..., description="Source branch name")
    target_branch: str = Field(..., description="Target branch name")
    message: Optional[str] = Field(None, description="Merge message")
    strategy: str = Field(default="merge", description="Merge strategy")


class RollbackRequest(BaseModel):
    """Request model for rolling back changes"""

    target_commit: str = Field(..., description="Target commit to rollback to")
    reason: Optional[str] = Field(None, description="Rollback reason")


class DatabaseCreateRequest(BaseModel):
    """Request model for creating a database"""

    name: str = Field(..., description="Database name")
    description: Optional[str] = Field(None, description="Database description")
    options: Optional[Dict[str, Any]] = Field(None, description="Database options")


class MappingImportRequest(BaseModel):
    """Request model for importing mappings"""

    file_path: str = Field(..., description="Path to mapping file")
    format: str = Field(default="json", description="File format (json, csv, etc.)")
    overwrite: bool = Field(default=False, description="Overwrite existing mappings")
    should_validate: bool = Field(
        default=True, description="Validate mappings before import", alias="validate"
    )
