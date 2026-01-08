# Changelog

> Status: Historical snapshot. This file is not actively maintained; rely on git history/PRs for current changes.

All notable changes to SPICE HARVESTER will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive enterprise-level documentation suite
  - Architecture documentation with microservices overview
  - Complete API reference for all services
  - Developer guide with onboarding instructions
  - Operations manual for deployment and maintenance
  - Security documentation with best practices
- E2E testing framework simulating real user scenarios
- Performance optimization guidelines
- Security monitoring and incident response procedures

### Changed
- Updated README.md to English with enterprise focus
- Improved documentation structure and organization

## [2.1.1] - 2025-07-23

### Fixed
- MONEY type mapping to xsd:decimal for TerminusDB compatibility
- URL type mapping to xsd:anyURI
- ARRAY<STRING> parsing with case-insensitive handling
- SQL injection pattern false positives
- NoSQL injection detection for @type patterns
- PATCH endpoint references (changed to PUT for API compatibility)

### Security
- Improved input sanitization patterns
- Reduced false positives in security validation
- Enhanced SQL/NoSQL injection prevention

## [2.1.0] - 2025-07-22

### Added
- **Property-to-Relationship Automatic Conversion**: Class properties referencing other classes automatically convert to relationships
- **Advanced Constraint System**:
  - Value ranges (min/max), string length, patterns, format constraints
  - Array/collection constraints (min_items, max_items, unique_items)
  - Relationship cardinality constraints
  - Default value support (static, computed, timestamp, uuid, sequence, reference)
- **Full TerminusDB v11.x Support**:
  - OneOfType (Union types)
  - Foreign keys
  - GeoPoint, GeoTemporalPoint
  - Enum types
  - Set, List, Array (with dimensions support)
  - Optional types

### Changed
- Changed `sys:JSON` type to `xsd:string` (TerminusDB v11.x compatibility)
- Added `is_class_reference()` method to Property model
- Support for `type="link"` for explicit class references

### Added Files
- `backend/oms/services/property_to_relationship_converter.py`
- `backend/oms/utils/constraint_extractor.py`
- `backend/oms/utils/terminus_schema_types.py`

## [2.0.0] - 2025-07-20

### Changed
- Documentation update: All documents updated to match current code structure
- BFF port correction: Updated from 8001 to 8002 in DEPLOYMENT_GUIDE.md
- New documentation: Created CURRENT_ARCHITECTURE.md for clarity
- Documentation cleanup: Moved old migration documents to archive folder

### Current Structure
- **Service Ports**:
  - OMS: 8000
  - BFF: 8002
  - Funnel: 8003
  - TerminusDB: 6363

## [1.5.0] - 2025-07-18

### Added
- HTTPS support implementation
- Automatic CORS configuration
- Centralized port configuration (ServiceConfig)

### Changed
- Simplified directory structure: `backend/spice_harvester/*` → `backend/*`
- Import path changes: `from spice_harvester.shared...` → `from shared...`
- Service name simplification:
  - `ontology-management-service` → `oms`
  - `backend-for-frontend` → `bff`

## [1.0.0] - 2025-07-17

### Initial Release
- Removed sys.path.insert completely
- Adopted standard Python package structure
- Implemented microservices architecture:
  - OMS (Ontology Management Service)
  - BFF (Backend for Frontend)
  - Funnel (Type Inference Service)
  - Shared Components

### Key Features
- Ontology management
- Complex type system
- Relationship management
- Multi-language support
- Google Sheets integration

## [0.1.0] - 2025-07-01

### Project Inception
- Initial prototype development
- TerminusDB integration
- Basic CRUD operations implementation

---

## Version Management Policy

- **Major (X.0.0)**: Structural changes, breaking compatibility
- **Minor (0.X.0)**: New features, backward compatibility maintained
- **Patch (0.0.X)**: Bug fixes, documentation updates

## Upgrade Notes

### From 2.1.0 to 2.1.1
1. Update type mappings in configuration
2. Change PATCH endpoints to PUT in client code
3. Review and update security patterns if customized

### From 2.0.0 to 2.1.0
1. Database schema migration may be required
2. Update API clients for new constraint features
3. Review relationship configurations

### From 1.x to 2.0.0
1. Update service port configurations
2. Review documentation for new structure
3. Update deployment scripts

## Related Links

- [Architecture Overview](./docs/ARCHITECTURE.md)
- [API Reference](./docs/API_REFERENCE.md)
- [Operations Manual](./docs/OPERATIONS.md)
- [Security Documentation](./docs/SECURITY.md)
