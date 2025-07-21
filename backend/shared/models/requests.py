"""
Request models for SPICE HARVESTER services
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from .responses import ApiResponse


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

    @classmethod
    def accepted(cls, message: str, data: Optional[Dict[str, Any]] = None) -> "ApiResponse":
        """Create accepted response (202 Accepted)"""
        return cls(status="accepted", message=message, data=data)

    @classmethod
    def no_content(cls, message: str) -> "ApiResponse":
        """Create no content response (204 No Content)"""
        return cls(status="success", message=message)

    @classmethod
    def error(cls, message: str, errors: Optional[List[str]] = None) -> "ApiResponse":
        """Create error response (4xx/5xx status codes)"""
        return cls(status="error", message=message, errors=errors)

    @classmethod
    def warning(cls, message: str, data: Optional[Dict[str, Any]] = None) -> "ApiResponse":
        """Create warning response (successful but with warnings)"""
        return cls(status="warning", message=message, data=data)

    @classmethod
    def partial(
        cls, message: str, data: Optional[Dict[str, Any]] = None, errors: Optional[List[str]] = None
    ) -> "ApiResponse":
        """Create partial success response (some operations succeeded, some failed)"""
        return cls(status="partial", message=message, data=data, errors=errors)

    @classmethod
    def health_check(
        cls, service_name: str, version: str, description: Optional[str] = None
    ) -> "ApiResponse":
        """Create standardized health check response"""
        health_data = {"service": service_name, "version": version, "status": "healthy"}
        if description:
            health_data["description"] = description

        return cls(status="success", message="Service is healthy", data=health_data)

    def is_success(self) -> bool:
        """Check if response indicates success"""
        return self.status in ["success", "created", "accepted"]

    def is_error(self) -> bool:
        """Check if response indicates error"""
        return self.status == "error"

    def is_warning(self) -> bool:
        """Check if response has warnings"""
        return self.status in ["warning", "partial"]
