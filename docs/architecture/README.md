# SPICE HARVESTER Architecture

> Auto-generated on 2025-07-18 10:41:34
> Updated on 2025-08-05 - Outbox Pattern Implementation

## Overview

This document contains automatically generated architecture diagrams for the SPICE HARVESTER project. The architecture now includes the Outbox Pattern for reliable event publishing and asynchronous processing.

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
    %% Data Flow Architecture with Outbox Pattern
    
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
        Relay[Message Relay Service]
    end
    
    subgraph "Data Layer"
        Terminus[TerminusDB]
        Postgres[(PostgreSQL<br/>Outbox Table)]
        Cache[Redis Cache]
        Search[Search Index]
    end
    
    subgraph "Message Layer"
        Kafka[Apache Kafka]
        Consumers[Event Consumers]
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
    OMS --> Postgres
    Query --> Terminus
    Query --> Cache
    
    Postgres --> Relay
    Relay --> Kafka
    Kafka --> Consumers
    
    Terminus --> Search
```

### Service Interactions

```mermaid
sequenceDiagram
    %% Service Interaction Flow with Outbox Pattern
    
    participant Client
    participant BFF as Backend for Frontend
    participant OMS as Ontology Management Service
    participant DB as TerminusDB
    participant PG as PostgreSQL
    participant Relay as Message Relay
    participant Kafka
    participant Consumer as Event Consumer
    
    %% Create/Update Request Flow with Outbox Pattern
    Client->>BFF: HTTP Request (Create/Update)
    BFF->>BFF: Validate Request
    BFF->>BFF: Check Permissions
    
    BFF->>OMS: Forward Request
    OMS->>OMS: Business Logic
    
    rect rgb(240, 240, 240)
        Note over OMS,PG: Atomic Transaction
        OMS->>DB: Update Ontology
        DB-->>OMS: Success
        OMS->>PG: Insert Outbox Event
        PG-->>OMS: Event Stored
    end
    
    OMS-->>BFF: Response
    BFF-->>Client: Success Response
    
    %% Asynchronous Event Processing
    rect rgb(230, 250, 230)
        Note over Relay,Consumer: Asynchronous Flow
        Relay->>PG: Poll Unprocessed Events
        PG-->>Relay: Events List
        Relay->>Kafka: Publish Events
        Kafka-->>Relay: Ack
        Relay->>PG: Mark as Processed
        
        Consumer->>Kafka: Subscribe to Events
        Kafka-->>Consumer: New Event
        Consumer->>Consumer: Process Event
    end
    
    %% Read Request Flow (unchanged)
    Client->>BFF: HTTP Request (Read)
    BFF->>OMS: Forward Request
    OMS->>DB: Query
    DB-->>OMS: Result
    OMS-->>BFF: Response
    BFF-->>Client: Formatted Response
```

## Outbox Pattern Implementation

### Overview

The Outbox Pattern ensures reliable event publishing by storing events in a database table within the same transaction as the business operation. A separate Message Relay service then reads these events and publishes them to Kafka.

### Components

1. **PostgreSQL Outbox Table**: Stores events with metadata
2. **OMS Service**: Writes to both TerminusDB and Outbox table in a single transaction
3. **Message Relay Service**: Polls the outbox table and publishes to Kafka
4. **Kafka**: Message broker for event distribution
5. **Event Consumers**: Services that subscribe to and process events

### Event Types

- `ONTOLOGY_CLASS_CREATED`: New ontology class created
- `ONTOLOGY_CLASS_UPDATED`: Existing ontology class modified
- `ONTOLOGY_CLASS_DELETED`: Ontology class removed

### Benefits

- **Atomicity**: Events are guaranteed to be published if the business transaction succeeds
- **Reliability**: No events are lost even if Kafka is temporarily unavailable
- **Scalability**: Multiple Message Relay instances can run concurrently
- **Decoupling**: Services communicate asynchronously through events

### Configuration

Key environment variables:
- `POSTGRES_HOST/PORT/USER/PASSWORD/DB`: PostgreSQL connection
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `MESSAGE_RELAY_BATCH_SIZE`: Events per batch (default: 100)
- `MESSAGE_RELAY_POLL_INTERVAL`: Polling interval in seconds (default: 5)

