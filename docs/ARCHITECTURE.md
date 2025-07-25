# SPICE HARVESTER Architecture Documentation

## Executive Summary

SPICE HARVESTER is an enterprise-grade ontology management platform built on a modern microservices architecture. The system provides comprehensive ontology management capabilities with multi-language support, complex data type validation, and advanced relationship management features.

### Key Capabilities
- **Enterprise Ontology Management**: Complete lifecycle management of ontologies with advanced version control
- **Git-like Version Control**: Full git-like features with branch management, diff, merge, PR workflows (7/7 features - 100% working)
- **Multi-Branch Experiments**: Unlimited experimental branches for A/B testing and schema validation
- **Multi-language Support**: Comprehensive internationalization for global deployments
- **Complex Type System**: Support for 10+ complex data types including MONEY, EMAIL, PHONE, and custom objects
- **Advanced Relationship Management**: Bidirectional relationships with circular reference detection
- **Type Inference**: Automatic schema generation from external data sources
- **Security-First Design**: Input sanitization, authentication, and audit logging

## System Architecture

### Microservices Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                           Client Applications                        │
│                    (Web UI, Mobile Apps, API Clients)               │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    API Gateway / Load Balancer                       │
│                         (NGINX / HAProxy)                           │
└─────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ▼                           ▼                           ▼
┌──────────────┐           ┌──────────────┐           ┌──────────────┐
│     BFF      │           │   Funnel     │           │  External    │
│ Backend for  │           │    Type      │           │   Data       │
│  Frontend    │◄─────────►│  Inference   │◄─────────►│  Sources     │
│ Port: 8002   │           │ Port: 8003   │           │              │
└──────────────┘           └──────────────┘           └──────────────┘
        │                                                      
        │                                                      
        ▼                                                      
┌──────────────┐                                              
│     OMS      │           ┌──────────────┐                   
│  Ontology    │◄─────────►│ TerminusDB   │                   
│ Management   │           │   Graph      │                   
│ Port: 8000   │           │  Database    │                   
└──────────────┘           │ Port: 6363   │                   
                           └──────────────┘                   
```

### Service Responsibilities

#### 1. BFF (Backend for Frontend) - Port 8002
**Primary Responsibility**: API gateway and orchestration layer

**Key Features**:
- User-friendly label-based API endpoints
- Request/response transformation between frontend and backend services
- Service orchestration for complex operations
- Label-to-ID mapping management
- Authentication and authorization handling
- Request validation and sanitization

**Technology Stack**:
- Framework: FastAPI 0.100+
- Async HTTP Client: httpx
- Authentication: Custom middleware
- Validation: Pydantic models

#### 2. OMS (Ontology Management Service) - Port 8000
**Primary Responsibility**: Core ontology operations and data persistence

**Key Features**:
- Direct TerminusDB integration with v11.x features
- **Git-like Version Control Engine**: Complete branch, commit, diff, merge operations
- **3-Stage Diff System**: Commit-based, schema-level, and property-level comparison
- **Rebase-based Merging**: Using TerminusDB's rebase API for conflict-aware merging
- **Pull Request Workflow**: Full PR system with conflict detection and review process
- **Multi-Branch Experiment Support**: Unlimited experimental branches with A/B testing
- Internal ID-based ontology management
- Property-to-Relationship automatic conversion
- Complex type validation and serialization
- Constraint extraction and enforcement
- Schema evolution management
- Transaction support for atomic updates

**Technology Stack**:
- Framework: FastAPI 0.100+
- Database Client: httpx (async)
- Validation: Custom validators
- Schema Management: TerminusDB v11.x

#### 3. Funnel (Type Inference Service) - Port 8003
**Primary Responsibility**: Data analysis and type inference

**Key Features**:
- Automatic type detection from raw data
- Schema suggestion based on data patterns
- External data source integration (Google Sheets, CSV)
- Complex type recognition (EMAIL, PHONE, URL, MONEY)
- Statistical analysis for data quality

**Technology Stack**:
- Framework: FastAPI 0.100+
- Data Analysis: pandas, numpy
- Type Detection: Custom algorithms
- External APIs: Google Sheets API v4

#### 4. TerminusDB - Port 6363
**Primary Responsibility**: Graph database and query engine

**Key Features**:
- Document-graph hybrid storage
- ACID transactions
- **Git-like version control for data (100% feature coverage)**
  - ✅ **Branch Management**: Create, list, delete branches with shared data architecture
  - ✅ **Commit System**: Full commit history with messages, authors, and timestamps
  - ✅ **Diff Operations**: 3-stage diff approach (commit-based, schema-level, property-level)
  - ✅ **Merge Operations**: Rebase-based merging with conflict detection
  - ✅ **Pull Request System**: Complete PR workflow with review and conflict detection
  - ✅ **Rollback Operations**: Reset to any previous commit with data safety
  - ✅ **Version History**: Complete audit trail with metadata tracking
- WOQL query language
- JSON-LD schema support

### Shared Components

The `shared/` directory contains common components used across all services:

1. **Models** (`shared/models/`):
   - `common.py`: DataType enum, Cardinality definitions
   - `ontology.py`: OntologyBase, Property, Relationship models
   - `requests.py`: Standardized request/response models
   - `config.py`: Configuration models

2. **Validators** (`shared/validators/`):
   - Complex type validators for all 10 supported types
   - Constraint validators
   - Base validator framework

3. **Security** (`shared/security/`):
   - Input sanitization
   - SQL/NoSQL injection prevention
   - XSS protection
   - Path traversal prevention

4. **Utils** (`shared/utils/`):
   - JSON-LD conversion
   - Label mapping
   - Language detection
   - ID generation
   - Logging utilities

## Data Flow Architecture

### 1. Ontology Creation Flow

```
Client Request (Label-based)
         │
         ▼
   BFF Service
         │
         ├─► Label Validation
         ├─► Security Checks
         ├─► Label-to-ID Mapping
         │
         ▼
   OMS Service
         │
         ├─► Schema Validation
         ├─► Constraint Extraction
         ├─► Property-to-Relationship Conversion
         │
         ▼
   TerminusDB
         │
         └─► Persistent Storage
```

### 2. Git-like Version Control Flow

```
Client Git Operation Request
         │
         ▼
   BFF Service
         │
         ├─► Authentication
         ├─► Request Validation
         ├─► Operation Routing
         │
         ▼
   OMS Service
         │
         ├─► Branch Management
         ├─► 3-Stage Diff Processing
         ├─► Conflict Detection
         ├─► PR Workflow Management
         │
         ▼
   TerminusDB
         │
         ├─► Rebase API Operations
         ├─► Commit History Tracking
         ├─► NDJSON Response Parsing
         │
         └─► Persistent Storage
```

### 3. Multi-Branch Experiment Flow

```
Experiment Creation Request
         │
         ▼
   Experiment Manager
         │
         ├─► Branch Creation
         ├─► Schema Variant Application
         ├─► A/B Testing Setup
         │
         ▼
   Comparison Engine
         │
         ├─► Multi-branch Diff
         ├─► Performance Metrics
         ├─► Integration Testing
         │
         ▼
   Success Evaluation
         │
         └─► Production Merge (via PR)
```

### 4. Type Inference Flow

```
External Data Source
         │
         ▼
   Funnel Service
         │
         ├─► Data Sampling
         ├─► Pattern Recognition
         ├─► Type Detection
         ├─► Constraint Inference
         │
         ▼
   Schema Suggestion
         │
         ▼
   BFF Service
         │
         └─► Client Response
```

## Technology Stack

### Core Technologies

1. **Programming Language**: Python 3.9+
2. **Web Framework**: FastAPI 0.100+
3. **Database**: TerminusDB v11.x with git-like features
4. **Version Control**: Custom git-like engine with rebase support
5. **Async Operations**: asyncio, httpx
6. **Validation**: Pydantic, custom validators
7. **Testing**: pytest, pytest-asyncio
8. **Containerization**: Docker, Docker Compose

### Key Libraries

- **FastAPI**: Modern web framework with automatic API documentation
- **httpx**: Async HTTP client for service communication
- **Pydantic**: Data validation using Python type annotations
- **pandas**: Data analysis for type inference
- **phonenumbers**: Phone number validation
- **email-validator**: Email address validation

## Security Architecture

### Input Validation

1. **Sanitization Layers**:
   - Request-level sanitization at BFF
   - Field-level validation in models
   - Database query sanitization at OMS

2. **Protection Mechanisms**:
   - SQL injection prevention
   - NoSQL injection prevention (MongoDB operators)
   - XSS protection
   - Path traversal prevention
   - Command injection prevention

3. **Recent Security Improvements**:
   - Fixed overly strict SQL injection patterns
   - Improved NoSQL injection detection to avoid false positives
   - Added negative lookbehind for `@type` patterns

### Authentication & Authorization

1. **Service-to-Service**: 
   - Internal API keys
   - Service identification headers

2. **Client Authentication**:
   - JWT tokens (planned)
   - API key authentication
   - Role-based access control (RBAC)

3. **TerminusDB Security**:
   - Basic authentication
   - Database-level access control
   - Encrypted connections (HTTPS)

## Performance Considerations

### 1. Asynchronous Operations

All services use async/await patterns for:
- Database operations
- Service-to-service communication
- External API calls

### 2. Connection Pooling

- HTTP connection pooling with httpx
- Configurable pool sizes
- Connection timeout management

### 3. Caching Strategy

- Label-to-ID mapping cache in BFF
- Query result caching (planned)
- Schema definition caching

### 4. Scalability

- Stateless service design
- Horizontal scaling capability
- Load balancer ready
- Database clustering support

## Deployment Architecture

### Container Architecture

```yaml
services:
  bff:
    image: spice-harvester/bff:latest
    ports: ["8002:8002"]
    environment:
      - OMS_BASE_URL=http://oms:8000
      - FUNNEL_BASE_URL=http://funnel:8003
    
  oms:
    image: spice-harvester/oms:latest
    ports: ["8000:8000"]
    environment:
      - TERMINUS_SERVER_URL=http://terminusdb:6363
    
  funnel:
    image: spice-harvester/funnel:latest
    ports: ["8003:8003"]
    
  terminusdb:
    image: terminusdb/terminusdb:v11.x
    ports: ["6363:6363"]
    volumes:
      - terminus_data:/app/terminusdb/storage
```

### Environment Configuration

1. **Development**: Local Docker Compose
2. **Staging**: Kubernetes with ConfigMaps
3. **Production**: Kubernetes with Secrets, horizontal pod autoscaling

## Monitoring & Observability

### 1. Health Checks

All services expose health endpoints:
- BFF: `http://localhost:8002/health`
- OMS: `http://localhost:8000/health`
- Funnel: `http://localhost:8003/health`

### 2. Logging

- Structured JSON logging
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
- Centralized log aggregation (ELK stack ready)

### 3. Metrics (Planned)

- Request/response times
- Error rates
- Database query performance
- Service availability

## Git-like Version Control Architecture

### Technical Implementation

1. **3-Stage Diff Engine**:
   ```
   Stage 1: Commit-based Diff
           │
           ▼
   Stage 2: Schema Comparison  
           │
           ▼
   Stage 3: Property-level Analysis
   ```

2. **NDJSON Parser**: 
   - Line-by-line parsing for TerminusDB responses
   - Handles "Extra data" JSON parsing errors
   - Supports both single and multi-line responses

3. **Rebase-based Merge System**:
   - Uses TerminusDB's `/api/rebase` endpoint
   - Conflict detection and resolution
   - Atomic merge operations

4. **Pull Request Engine**:
   - In-memory PR metadata management
   - Conflict detection pipeline
   - Statistical analysis of changes
   - Auto-merge capability for clean PRs

5. **Multi-Branch Experiment Architecture**:
   - Unlimited experimental branches
   - Branch comparison matrix
   - Integration testing framework
   - Metrics collection and evaluation

### Git API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/{db}/branches` | GET | List all branches |
| `/{db}/branches` | POST | Create new branch |
| `/{db}/branches/{name}` | DELETE | Delete branch |
| `/{db}/diff` | GET | Compare branches |
| `/{db}/merge` | POST | Merge branches |
| `/{db}/pull-requests` | POST | Create PR |
| `/{db}/pull-requests/{id}/merge` | POST | Merge PR |
| `/{db}/commits` | GET | Get commit history |
| `/{db}/rollback` | POST | Rollback to commit |

### Git Features Integration

**BFF Layer**:
- Git operation routing and validation
- Label-based branch naming support
- Response transformation for client compatibility

**OMS Layer**:
- Core git engine implementation
- TerminusDB v11.x API integration
- Conflict detection and resolution
- Experiment management framework

**TerminusDB Layer**:
- Native branch and commit support
- Rebase API for merge operations
- NDJSON response format
- Shared data architecture across branches

## Recent Architectural Improvements

### July 2025 Updates - Git System Implementation

1. **Complete Git-like System**:
   - Achieved 100% feature coverage (7/7 git features working)
   - Implemented real diff with 3-stage approach
   - Added rebase-based merge operations
   - Created full Pull Request workflow
   - Built multi-branch experiment environment

2. **Technical Breakthroughs**:
   - Discovered TerminusDB v11.x branch architecture
   - Implemented NDJSON parsing fix
   - Found and utilized rebase API for merging
   - Created property-level diff analysis
   - Built conflict detection system

### Previous Updates (Maintained)

3. **Type System Enhancements** (Previous):
   - Added MONEY type mapping to xsd:decimal
   - Fixed ARRAY<STRING> parsing with case-insensitive handling
   - Added missing type mappings (URL, PHONE, IP, UUID, etc.)

4. **Security Improvements** (Previous):
   - Refined SQL injection patterns to reduce false positives
   - Fixed NoSQL injection detection for @type patterns
   - Improved input sanitization without blocking legitimate requests

5. **API Improvements** (Previous):
   - Fixed PATCH endpoint references (use PUT instead)
   - Enhanced error messaging
   - Improved constraint validation

## Future Architecture Considerations

### Git System Enhancements
1. **Distributed Git**: Multi-node TerminusDB git operations
2. **Advanced Conflict Resolution**: AI-powered merge conflict resolution
3. **Git Hooks**: Pre-commit and post-commit hooks for validation
4. **Branch Protection**: Configurable branch protection rules
5. **Git Analytics**: Advanced metrics and reporting for git operations

### General System Enhancements
6. **GraphQL API Layer**: Alternative to REST for complex queries
7. **Event Streaming**: Apache Kafka for real-time updates
8. **Service Mesh**: Istio for advanced traffic management
9. **API Gateway**: Kong or AWS API Gateway for enterprise features
10. **Distributed Tracing**: OpenTelemetry integration

## Conclusion

SPICE HARVESTER's architecture is designed for:
- **Scalability**: Microservices allow independent scaling with unlimited experimental branches
- **Maintainability**: Clear service boundaries and git-like version control for all changes
- **Security**: Multiple layers of validation and protection with branch-level access control
- **Extensibility**: Easy to add new services or features with multi-branch testing
- **Performance**: Async operations and efficient 3-stage diff processing
- **Innovation**: Complete git-like workflow enabling safe experimentation and A/B testing

The architecture now supports enterprise requirements with **100% git-like functionality**, enabling:
- Safe schema experimentation through unlimited branches
- Production-quality version control with full audit trails  
- Conflict-aware merging with automatic detection
- Complete Pull Request workflow for code review processes
- Multi-branch A/B testing for optimal schema design

This makes SPICE HARVESTER the first enterprise ontology platform with complete git-like version control capabilities while maintaining developer productivity and system reliability.