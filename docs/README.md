# SPICE HARVESTER Documentation

Welcome to the SPICE HARVESTER project documentation. This directory contains all project documentation organized by category.

## Documentation Overview

### Core Documentation

- **[Architecture Overview](ARCHITECTURE.md)** - Comprehensive system architecture with microservices design
- **[API Reference](API_REFERENCE.md)** - Complete API documentation for all services
- **[Developer Guide](DEVELOPER_GUIDE.md)** - Development setup, guidelines, and best practices
- **[Operations Manual](OPERATIONS.md)** - Deployment, maintenance, and operational procedures
- **[Security Documentation](SECURITY.md)** - Security architecture, guidelines, and compliance

### Quick Start Guides

#### For Developers
1. Start with the [Architecture Overview](ARCHITECTURE.md) to understand the system
2. Follow the [Developer Guide](DEVELOPER_GUIDE.md) for setup instructions
3. Review the [API Reference](API_REFERENCE.md) for endpoint details
4. Check [Security Documentation](SECURITY.md) for secure coding practices

#### For DevOps Engineers
1. Follow the [Operations Manual](OPERATIONS.md) for deployment
2. Review [Security Documentation](SECURITY.md) for security configuration
3. Check monitoring and maintenance procedures

#### For QA Engineers
1. Review the test structure in [Developer Guide](DEVELOPER_GUIDE.md#testing-guidelines)
2. Check API endpoints in [API Reference](API_REFERENCE.md)
3. Follow testing procedures in relevant sections

## Documentation Structure

```
docs/
├── ARCHITECTURE.md          # System architecture overview
├── API_REFERENCE.md         # Complete API documentation
├── DEVELOPER_GUIDE.md       # Developer onboarding and guidelines
├── OPERATIONS.md           # Operations and deployment manual
├── SECURITY.md             # Security documentation
├── README.md               # This file - documentation index
├── architecture/           # Architecture diagrams and details
│   ├── service_interactions.mmd
│   ├── data_flow.mmd
│   └── backend_classes.mmd
├── api/                    # API-specific documentation
│   └── OMS_DATABASE_ENDPOINTS.md
├── deployment/             # Deployment guides
│   └── DEPLOYMENT_GUIDE.md
├── development/            # Development guides
│   ├── FRONTEND_DEVELOPMENT_GUIDE.md
│   └── CORS_CONFIGURATION_GUIDE.md
├── requirements/           # Dependency requirements
│   ├── requirements.txt
│   ├── bff-requirements.txt
│   ├── oms-requirements.txt
│   └── funnel-requirements.txt
├── security/               # Security-specific guides
│   └── SECURITY.md
└── testing/                # Testing documentation
    ├── COMPLEX_TYPES_TEST_README.md
    └── OMS_PRODUCTION_TEST_README.md
```

## Key Features Documented

### Microservices Architecture
- **OMS (Ontology Management Service)** - Core ontology operations
- **BFF (Backend for Frontend)** - API gateway and orchestration
- **Funnel (Type Inference Service)** - Data analysis and type detection
- **Shared Components** - Common utilities and models

### Technical Capabilities
- **Complex Type System** - Support for 10+ data types (MONEY, EMAIL, PHONE, etc.)
- **Relationship Management** - Automatic Property-to-Relationship conversion
- **Multi-language Support** - Internationalization for global deployments
- **Security Features** - Input sanitization, authentication, audit logging
- **TerminusDB v11.x Integration** - Full schema type support

### Service Configuration
- **Port Assignments**:
  - OMS: 8000
  - BFF: 8002
  - Funnel: 8003
  - TerminusDB: 6363

## Documentation Standards

### File Naming Conventions
- Main guides use UPPERCASE (e.g., `ARCHITECTURE.md`)
- Descriptive names with underscores
- Language suffixes for translations (e.g., `_ES.md`)

### Content Structure
- Table of contents for long documents
- Clear section headers with proper hierarchy
- Code examples where applicable
- Diagrams for complex concepts

### Writing Guidelines
- Use present tense
- Be concise but comprehensive
- Include practical examples
- Keep technical accuracy
- Update with code changes

## Contributing to Documentation

When adding new documentation:
1. Place in the appropriate category directory
2. Update this README.md with document links
3. Follow existing format standards
4. Include creation and update dates
5. Review for technical accuracy

### Documentation Review Process
1. Technical review by development team
2. Editorial review for clarity
3. Security review for sensitive information
4. Final approval by project lead

## Recent Updates

### Version 2.1.1 (2025-07-23)
- Created comprehensive enterprise documentation suite
- Updated all documentation to English
- Added security and operations documentation
- Improved structure and organization

### Version 2.1.0 (2025-07-22)
- Added Property-to-Relationship conversion documentation
- Updated for TerminusDB v11.x support
- Enhanced constraint system documentation

### Version 2.0.0 (2025-07-20)
- Restructured documentation hierarchy
- Updated port configurations
- Added current architecture documentation

## External Resources

- [TerminusDB Documentation](https://terminusdb.com/docs/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Pydantic Documentation](https://pydantic-docs.helpmanual.io/)
- [Docker Documentation](https://docs.docker.com/)

## Support

For documentation issues or questions:
- Open an issue in the GitHub repository
- Contact the documentation team
- Submit a pull request with improvements

---
*Last updated: 2025-07-23*