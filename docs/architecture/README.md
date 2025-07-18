# SPICE HARVESTER Architecture

> Auto-generated on 2025-07-18 10:41:34

## Overview

This document contains automatically generated architecture diagrams for the SPICE HARVESTER project.

## Class Diagrams

### Backend Classes

```mermaid
classDiagram
    %% SPICE HARVESTER Comprehensive Backend Architecture
    
    %% Core Application Layer
    class FastAPIApplication {
        +FastAPI app
        +List~Router~ routers
        +List~Middleware~ middleware
        +Config config
        +startup() Task
        +shutdown() Task
        +include_routers() None
        +add_middleware() None
    }
    
    %% Domain Models
    class Production {
        +String id
        +String name
        +DateTime created_at
        +DateTime updated_at
        +Dict metadata
        +List~Relationship~ relationships
        +validate() bool
        +to_dict() Dict
        +from_dict() Production
    }
    
    class Ontology {
        +String id
        +String name
        +String version
        +Dict schema
        +List~Class~ classes
        +List~Property~ properties
        +validate_schema() bool
        +get_class() Class
        +add_property() None
    }
    
    class Relationship {
        +String id
        +String source_id
        +String target_id
        +String relationship_type
        +Dict properties
        +DateTime created_at
        +validate_circular() bool
        +get_path() List
    }
    
    %% Service Layer
    class OntologyManagementService {
        +AsyncTerminusClient terminus_client
        +RelationshipManager relationship_manager
        +create_ontology() Ontology
        +update_ontology() Ontology
        +delete_ontology() bool
        +get_ontology() Ontology
        +list_ontologies() List~Ontology~
        +validate_relationships() bool
    }
    
    class BackendForFrontend {
        +OMSClient oms_client
        +LabelMapper label_mapper
        +ResponseFormatter formatter
        +aggregate_data() Dict
        +format_response() Response
        +handle_error() ErrorResponse
        +map_labels() Dict
    }
    
    class AsyncTerminusService {
        +String terminus_url
        +HTTPXClient http_client
        +ConnectionPool pool
        +execute_woql() Result
        +create_database() bool
        +get_document() Document
        +update_document() bool
        +delete_document() bool
    }
    
    %% Complex Type System
    class ComplexTypeValidator {
        +Dict type_registry
        +List validation_rules
        +validate() bool
        +validate_nested() bool
        +register_type() None
        +get_validator() Validator
    }
    
    class ComplexTypeSerializer {
        +Dict serializers
        +serialize() str
        +deserialize() Any
        +register_serializer() None
        +handle_circular_refs() Dict
    }
    
    %% Utils and Helpers
    class RelationshipManager {
        +CircularReferenceDetector detector
        +RelationshipPathTracker tracker
        +create_relationship() Relationship
        +validate_relationship() bool
        +find_paths() List~Path~
        +detect_cycles() bool
    }
    
    class MultilingualText {
        +Dict~str, str~ translations
        +String default_language
        +get_text() str
        +set_text() None
        +get_languages() List~str~
        +validate() bool
    }
    
    %% Middleware
    class RBACMiddleware {
        +PermissionChecker checker
        +UserContext context
        +check_permission() bool
        +get_user_roles() List~Role~
        +enforce_policy() None
    }
    
    class ValidationMiddleware {
        +RequestValidator validator
        +validate_request() bool
        +validate_response() bool
        +sanitize_input() Dict
    }
    
    %% Relationships
    FastAPIApplication --> OntologyManagementService : uses
    FastAPIApplication --> BackendForFrontend : uses
    OntologyManagementService --> AsyncTerminusService : uses
    OntologyManagementService --> RelationshipManager : uses
    BackendForFrontend --> ComplexTypeSerializer : uses
    BackendForFrontend --> ComplexTypeValidator : uses
    Production --> Relationship : has many
    Ontology --> Production : defines
    RelationshipManager --> Relationship : manages
    FastAPIApplication --> RBACMiddleware : applies
    FastAPIApplication --> ValidationMiddleware : applies
    MultilingualText --> ComplexTypeValidator : validated by
```

### Classes Spice Harvester

*Diagram generation pending for Classes Spice Harvester*

### Data Flow

```mermaid
graph TB
    %% Data Flow Architecture
    
    subgraph "Client Layer"
        Web[Web Application]
        Mobile[Mobile App]
        API[External API]
    end
    
    subgraph "API Gateway Layer"
        BFF[Backend for Frontend]
        Auth[Authentication]
        RateLimit[Rate Limiter]
    end
    
    subgraph "Service Layer"
        OMS[Ontology Management]
        Query[Query Service]
        Validator[Validation Service]
    end
    
    subgraph "Data Layer"
        Terminus[TerminusDB]
        Cache[Redis Cache]
        Search[Search Index]
    end
    
    %% Connections
    Web --> BFF
    Mobile --> BFF
    API --> BFF
    
    BFF --> Auth
    BFF --> RateLimit
    
    Auth --> OMS
    Auth --> Query
    
    OMS --> Validator
    Query --> Validator
    
    OMS --> Terminus
    Query --> Terminus
    Query --> Cache
    
    Terminus --> Search
```

### Service Interactions

```mermaid
sequenceDiagram
    %% Service Interaction Flow
    
    participant Client
    participant BFF as Backend for Frontend
    participant OMS as Ontology Management Service
    participant DB as TerminusDB
    participant Cache as Cache Layer
    
    %% Standard Request Flow
    Client->>BFF: HTTP Request
    BFF->>BFF: Validate Request
    BFF->>BFF: Check Permissions
    
    alt Cached Response Available
        BFF->>Cache: Get Cached Data
        Cache-->>BFF: Return Data
        BFF-->>Client: Return Response
    else Fresh Data Needed
        BFF->>OMS: Forward Request
        OMS->>OMS: Business Logic
        OMS->>DB: Query/Update
        DB-->>OMS: Result
        OMS-->>BFF: Response
        BFF->>Cache: Update Cache
        BFF-->>Client: Formatted Response
    end
    
    %% Error Handling
    alt Error Occurs
        OMS-->>BFF: Error Response
        BFF->>BFF: Format Error
        BFF-->>Client: Error Details
    end
```

