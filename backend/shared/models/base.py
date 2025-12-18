"""
Base models with MVCC support for SPICE HARVESTER
Implements Optimistic Locking through version field management
"""

from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class VersionedModelMixin(BaseModel):
    """
    Mixin for adding optimistic locking version support to models.
    
    Following SOLID principles:
    - SRP: Only responsible for version management
    - OCP: Can be extended without modification
    - LSP: Can substitute any BaseModel
    """

    model_config = ConfigDict(validate_assignment=True, use_enum_values=True)

    version: int = Field(
        default=1,
        ge=1,
        description="Optimistic locking version number. Incremented on each update."
    )
    
    def increment_version(self) -> None:
        """
        Increment the version number for optimistic locking.
        Should be called before any update operation.
        """
        self.version += 1
    
    def check_version_conflict(self, expected_version: int) -> bool:
        """
        Check if there's a version conflict for optimistic locking.
        
        Args:
            expected_version: The version that was expected (from client/previous read)
            
        Returns:
            True if versions match (no conflict), False otherwise
        """
        return self.version == expected_version
    
    def get_version_for_update(self) -> int:
        """
        Get the current version for use in update operations.
        The actual update should increment this.
        
        Returns:
            Current version number
        """
        return self.version


class OptimisticLockError(Exception):
    """
    Exception raised when optimistic locking version conflict is detected.
    
    Attributes:
        entity_type: Type of entity that had the conflict
        entity_id: ID of the entity
        expected_version: Version that was expected
        actual_version: Actual current version
    """
    
    def __init__(
        self,
        entity_type: str,
        entity_id: str,
        expected_version: int,
        actual_version: int
    ):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        
        message = (
            f"Optimistic lock conflict on {entity_type} '{entity_id}': "
            f"expected version {expected_version}, but current version is {actual_version}. "
            f"Please refresh and retry your operation."
        )
        super().__init__(message)


class ConcurrencyControl:
    """
    Utility class for concurrency control operations.
    Follows SRP - only handles concurrency control logic.
    """
    
    @staticmethod
    def validate_version_for_update(
        current_version: int,
        provided_version: Optional[int]
    ) -> None:
        """
        Validate version for update operation.
        
        Args:
            current_version: Current version in database
            provided_version: Version provided by client
            
        Raises:
            OptimisticLockError: If versions don't match
        """
        if provided_version is not None and provided_version != current_version:
            raise OptimisticLockError(
                entity_type="Entity",
                entity_id="unknown",
                expected_version=provided_version,
                actual_version=current_version
            )
    
    @staticmethod
    def get_next_version(current_version: int) -> int:
        """
        Calculate next version number.
        
        Args:
            current_version: Current version number
            
        Returns:
            Next version number
        """
        return current_version + 1
