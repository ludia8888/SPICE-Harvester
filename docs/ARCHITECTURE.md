# SPICE HARVESTER Architecture Documentation

## Executive Summary

SPICE HARVESTER is an enterprise-grade ontology management platform built on a modern microservices architecture. The system provides comprehensive ontology management capabilities with multi-language support, complex data type validation, and advanced relationship management features.

### Key Capabilities
- **Enterprise Ontology Management**: Complete lifecycle management of ontologies with version control
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
- Direct TerminusDB integration
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
- Version control for data
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

### 2. Type Inference Flow

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
3. **Database**: TerminusDB v11.x
4. **Async Operations**: asyncio, httpx
5. **Validation**: Pydantic, custom validators
6. **Testing**: pytest, pytest-asyncio
7. **Containerization**: Docker, Docker Compose

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

## Recent Architectural Improvements

### July 2025 Updates

1. **Type System Enhancements**:
   - Added MONEY type mapping to xsd:decimal
   - Fixed ARRAY<STRING> parsing with case-insensitive handling
   - Added missing type mappings (URL, PHONE, IP, UUID, etc.)

2. **Security Improvements**:
   - Refined SQL injection patterns to reduce false positives
   - Fixed NoSQL injection detection for @type patterns
   - Improved input sanitization without blocking legitimate requests

3. **API Improvements**:
   - Fixed PATCH endpoint references (use PUT instead)
   - Enhanced error messaging
   - Improved constraint validation

## Future Architecture Considerations

1. **GraphQL API Layer**: Alternative to REST for complex queries
2. **Event Streaming**: Apache Kafka for real-time updates
3. **Service Mesh**: Istio for advanced traffic management
4. **API Gateway**: Kong or AWS API Gateway for enterprise features
5. **Distributed Tracing**: OpenTelemetry integration

## Conclusion

SPICE HARVESTER's architecture is designed for:
- **Scalability**: Microservices allow independent scaling
- **Maintainability**: Clear service boundaries and responsibilities
- **Security**: Multiple layers of validation and protection
- **Extensibility**: Easy to add new services or features
- **Performance**: Async operations and efficient data flow

The architecture supports enterprise requirements while maintaining developer productivity and system reliability.