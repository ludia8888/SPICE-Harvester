"""
Schema Versioning Service
THINK ULTRA³ - Managing schema evolution with backward compatibility

This service provides schema versioning capabilities for events and entities,
ensuring smooth migrations and backward compatibility.
"""

import json
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SchemaVersion:
    """
    Represents a schema version with comparison capabilities.
    Format: MAJOR.MINOR.PATCH (semantic versioning)
    """
    
    def __init__(self, version_string: str):
        """
        Initialize schema version from string.
        
        Args:
            version_string: Version in format "1.2.3"
        """
        parts = version_string.split('.')
        if len(parts) != 3:
            raise ValueError(f"Invalid version format: {version_string}")
        
        self.major = int(parts[0])
        self.minor = int(parts[1])
        self.patch = int(parts[2])
        self.version_string = version_string
    
    def __str__(self) -> str:
        return self.version_string
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, SchemaVersion):
            return False
        return (self.major == other.major and 
                self.minor == other.minor and 
                self.patch == other.patch)
    
    def __lt__(self, other) -> bool:
        if not isinstance(other, SchemaVersion):
            return False
        if self.major != other.major:
            return self.major < other.major
        if self.minor != other.minor:
            return self.minor < other.minor
        return self.patch < other.patch
    
    def __le__(self, other) -> bool:
        return self == other or self < other
    
    def is_backward_compatible(self, other: 'SchemaVersion') -> bool:
        """
        Check if this version is backward compatible with another.
        
        Following semantic versioning:
        - Major version changes are breaking
        - Minor version changes add functionality (backward compatible)
        - Patch versions are bug fixes (backward compatible)
        
        Args:
            other: Version to compare with
            
        Returns:
            True if backward compatible
        """
        # Same major version = backward compatible
        return self.major == other.major and self >= other


class MigrationStrategy(Enum):
    """Migration strategies for schema changes"""
    ADD_FIELD = "add_field"
    REMOVE_FIELD = "remove_field"
    RENAME_FIELD = "rename_field"
    CHANGE_TYPE = "change_type"
    TRANSFORM_VALUE = "transform_value"
    CUSTOM = "custom"


class SchemaMigration:
    """
    Represents a migration from one schema version to another.
    """
    
    def __init__(
        self,
        from_version: str,
        to_version: str,
        entity_type: str,
        migration_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        description: str = ""
    ):
        """
        Initialize schema migration.
        
        Args:
            from_version: Source schema version
            to_version: Target schema version
            entity_type: Type of entity (Event, Command, etc.)
            migration_func: Function to transform data
            description: Human-readable description
        """
        self.from_version = SchemaVersion(from_version)
        self.to_version = SchemaVersion(to_version)
        self.entity_type = entity_type
        self.migration_func = migration_func
        self.description = description
    
    def apply(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply migration to data.
        
        Args:
            data: Data to migrate
            
        Returns:
            Migrated data
        """
        try:
            migrated = self.migration_func(data.copy())
            migrated['schema_version'] = str(self.to_version)
            return migrated
        except Exception as e:
            logger.error(f"Migration failed from {self.from_version} to {self.to_version}: {e}")
            raise


class SchemaRegistry:
    """
    Central registry for schema versions and migrations.
    """
    
    def __init__(self):
        """Initialize schema registry"""
        self.current_versions: Dict[str, SchemaVersion] = {}
        self.migrations: Dict[str, List[SchemaMigration]] = {}
        self.schemas: Dict[str, Dict[str, Any]] = {}
    
    def register_schema(
        self,
        entity_type: str,
        version: str,
        schema: Dict[str, Any]
    ):
        """
        Register a schema version.
        
        Args:
            entity_type: Type of entity
            version: Schema version
            schema: Schema definition
        """
        version_obj = SchemaVersion(version)
        
        if entity_type not in self.schemas:
            self.schemas[entity_type] = {}
        
        self.schemas[entity_type][version] = schema
        
        # Update current version if newer
        if (entity_type not in self.current_versions or 
            version_obj > self.current_versions[entity_type]):
            self.current_versions[entity_type] = version_obj
            logger.info(f"Updated current version for {entity_type} to {version}")
    
    def register_migration(self, migration: SchemaMigration):
        """
        Register a migration between versions.
        
        Args:
            migration: Migration to register
        """
        key = f"{migration.entity_type}:{migration.from_version}"
        
        if key not in self.migrations:
            self.migrations[key] = []
        
        self.migrations[key].append(migration)
        logger.info(
            f"Registered migration for {migration.entity_type} "
            f"from {migration.from_version} to {migration.to_version}"
        )
    
    def get_migration_path(
        self,
        entity_type: str,
        from_version: str,
        to_version: Optional[str] = None
    ) -> List[SchemaMigration]:
        """
        Find migration path between versions.
        
        Args:
            entity_type: Type of entity
            from_version: Source version
            to_version: Target version (current if None)
            
        Returns:
            List of migrations to apply in order
        """
        from_v = SchemaVersion(from_version)
        to_v = (SchemaVersion(to_version) if to_version 
                else self.current_versions.get(entity_type))
        
        if not to_v:
            raise ValueError(f"No current version for {entity_type}")
        
        if from_v == to_v:
            return []
        
        if from_v > to_v:
            raise ValueError(f"Cannot migrate backwards from {from_v} to {to_v}")
        
        # Build migration path (simple linear for now)
        path = []
        current_v = from_v
        
        while current_v < to_v:
            key = f"{entity_type}:{current_v}"
            migrations = self.migrations.get(key, [])
            
            if not migrations:
                raise ValueError(f"No migration found from {current_v}")
            
            # Find best migration (closest to target)
            best_migration = None
            for migration in migrations:
                if migration.to_version <= to_v:
                    if (not best_migration or 
                        migration.to_version > best_migration.to_version):
                        best_migration = migration
            
            if not best_migration:
                raise ValueError(f"No migration path from {current_v} to {to_v}")
            
            path.append(best_migration)
            current_v = best_migration.to_version
        
        return path
    
    def migrate_data(
        self,
        data: Dict[str, Any],
        entity_type: str,
        target_version: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Migrate data to target version.
        
        Args:
            data: Data to migrate
            entity_type: Type of entity
            target_version: Target version (current if None)
            
        Returns:
            Migrated data
        """
        current_version = data.get('schema_version', '1.0.0')
        
        if not target_version:
            target_v = self.current_versions.get(entity_type)
            if not target_v:
                logger.warning(f"No current version for {entity_type}, using {current_version}")
                return data
            target_version = str(target_v)
        
        if current_version == target_version:
            return data
        
        # Get migration path
        migrations = self.get_migration_path(entity_type, current_version, target_version)
        
        # Apply migrations in order
        migrated_data = data
        for migration in migrations:
            logger.debug(f"Applying migration from {migration.from_version} to {migration.to_version}")
            migrated_data = migration.apply(migrated_data)
        
        return migrated_data


class SchemaVersioningService:
    """
    High-level service for schema versioning operations.
    """
    
    def __init__(self, registry: Optional[SchemaRegistry] = None):
        """
        Initialize schema versioning service.
        
        Args:
            registry: Schema registry to use (creates new if None)
        """
        self.registry = registry or SchemaRegistry()
        self._initialize_default_schemas()
    
    def _initialize_default_schemas(self):
        """Initialize default schemas and migrations"""
        
        # Register default Event schema v1.0.0
        self.registry.register_schema(
            "Event",
            "1.0.0",
            {
                "event_id": {"type": "string", "required": True},
                "event_type": {"type": "string", "required": True},
                "occurred_at": {"type": "datetime", "required": True},
                "data": {"type": "object", "required": True}
            }
        )
        
        # Register Event schema v1.1.0 (added sequence_number)
        self.registry.register_schema(
            "Event",
            "1.1.0",
            {
                "event_id": {"type": "string", "required": True},
                "event_type": {"type": "string", "required": True},
                "occurred_at": {"type": "datetime", "required": True},
                "sequence_number": {"type": "integer", "required": False},
                "data": {"type": "object", "required": True}
            }
        )
        
        # Register migration from 1.0.0 to 1.1.0
        self.registry.register_migration(
            SchemaMigration(
                from_version="1.0.0",
                to_version="1.1.0",
                entity_type="Event",
                migration_func=lambda data: {
                    **data,
                    "sequence_number": data.get("sequence_number", 0)
                },
                description="Add sequence_number field with default 0"
            )
        )
        
        # Register Event schema v1.2.0 (added aggregate_id)
        self.registry.register_schema(
            "Event",
            "1.2.0",
            {
                "event_id": {"type": "string", "required": True},
                "event_type": {"type": "string", "required": True},
                "occurred_at": {"type": "datetime", "required": True},
                "sequence_number": {"type": "integer", "required": False},
                "aggregate_id": {"type": "string", "required": False},
                "data": {"type": "object", "required": True}
            }
        )
        
        # Register migration from 1.1.0 to 1.2.0
        self.registry.register_migration(
            SchemaMigration(
                from_version="1.1.0",
                to_version="1.2.0",
                entity_type="Event",
                migration_func=lambda data: {
                    **data,
                    "aggregate_id": data.get("aggregate_id", data.get("instance_id"))
                },
                description="Add aggregate_id field from instance_id"
            )
        )
    
    def version_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add or update schema version for an event.
        
        Args:
            event: Event data
            
        Returns:
            Event with schema version
        """
        entity_type = "Event"
        
        if 'schema_version' not in event:
            current_version = self.registry.current_versions.get(entity_type)
            if current_version:
                event['schema_version'] = str(current_version)
            else:
                event['schema_version'] = "1.0.0"
        
        return event
    
    def migrate_event(
        self,
        event: Dict[str, Any],
        target_version: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Migrate event to target version.
        
        Args:
            event: Event data
            target_version: Target version (current if None)
            
        Returns:
            Migrated event
        """
        return self.registry.migrate_data(event, "Event", target_version)
    
    def is_compatible(
        self,
        data: Dict[str, Any],
        entity_type: str,
        required_version: Optional[str] = None
    ) -> bool:
        """
        Check if data version is compatible with required version.
        
        Args:
            data: Data with schema_version
            entity_type: Type of entity
            required_version: Required version (current if None)
            
        Returns:
            True if compatible
        """
        data_version = SchemaVersion(data.get('schema_version', '1.0.0'))
        
        if not required_version:
            required_v = self.registry.current_versions.get(entity_type)
            if not required_v:
                return True
        else:
            required_v = SchemaVersion(required_version)
        
        return data_version.is_backward_compatible(required_v)