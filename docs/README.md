# SPICE HARVESTER Documentation

Welcome to the SPICE HARVESTER project documentation. This directory contains all project documentation organized by category and thoroughly reorganized for clarity and maintainability.

## Documentation Overview

### Core Documentation

- **[System Architecture](ARCHITECTURE.md)** - Comprehensive system architecture with microservices design, implementation status, and performance metrics
- **[Frontend Development Guide](FRONTEND_GUIDE.md)** - Complete frontend development guide with React, TypeScript, and Blueprint.js
- **[API Reference](API_REFERENCE.md)** - Complete API documentation for all services
- **[Developer Guide](DEVELOPER_GUIDE.md)** - Development setup, guidelines, and best practices
- **[Operations Manual](OPERATIONS.md)** - Deployment, maintenance, and operational procedures
- **[Security Documentation](SECURITY.md)** - Security architecture, guidelines, and compliance
- **[UI/UX Guidelines](UIUX.md)** - User interface and experience design standards
- **[Design System](DesignSystem.md)** - Design system documentation and guidelines

### Quick Start Guides

#### For Developers
1. Start with the [System Architecture](ARCHITECTURE.md) to understand the system (90-95% complete backend)
2. Follow the [Developer Guide](DEVELOPER_GUIDE.md) for backend setup instructions
3. Use the [Frontend Development Guide](FRONTEND_GUIDE.md) for frontend development
4. Review the [API Reference](API_REFERENCE.md) for endpoint details
5. Check [Security Documentation](SECURITY.md) for secure coding practices

#### For Frontend Developers
1. Read the [Frontend Development Guide](FRONTEND_GUIDE.md) for comprehensive setup and patterns
2. Check the [System Architecture](ARCHITECTURE.md) to understand backend services
3. Review [Design System](DesignSystem.md) for UI component guidelines
4. Follow [UI/UX Guidelines](UIUX.md) for design standards

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
├── README.md               # This file - documentation index
├── ARCHITECTURE.md         # 🔄 Unified system architecture (3 docs merged)
├── FRONTEND_GUIDE.md       # 🆕 Unified frontend guide (7 docs merged)
├── API_REFERENCE.md        # Complete API documentation
├── DEVELOPER_GUIDE.md      # Developer onboarding and guidelines
├── OPERATIONS.md          # Operations and deployment manual
├── SECURITY.md            # Security documentation
├── UIUX.md               # UI/UX guidelines
├── DesignSystem.md       # 🔄 Design system (renamed from DesignSysyem.md)
└── architecture/         # Architecture diagrams and details
    └── README.md

backend/docs/              # Backend-specific documentation
├── api/
│   └── OMS_DATABASE_ENDPOINTS.md
├── deployment/
│   ├── DEPLOYMENT_GUIDE.md
│   └── PORT_CONFIGURATION.md     # 📁 Moved from backend root
├── development/
│   ├── FRONTEND_DEVELOPMENT_GUIDE.md  # Backend-specific frontend guide
│   ├── CORS_CONFIGURATION_GUIDE.md
│   ├── CORS_QUICK_START.md       # 📁 Moved from backend root
│   └── DATABASE_LIST_FIX_SUMMARY.md
├── security/
│   └── SECURITY.md
└── testing/
    ├── COMPLEX_TYPES_TEST_README.md
    └── OMS_PRODUCTION_TEST_README.md
```

## Key Features Documented

### Enterprise Microservices Architecture
- **BFF (Backend for Frontend)** - Port 8002: User-friendly API gateway with Service Factory pattern
- **OMS (Ontology Management Service)** - Port 8000: Core ontology operations with 18+ validators
- **Funnel (Type Inference Service)** - Port 8004: AI-powered data analysis with 1,048 lines of algorithms
- **Shared Components** - Service Factory, validators, and utilities

### Technical Capabilities
- **Complex Type System** - Support for 18+ data types (MONEY, EMAIL, PHONE, COORDINATE, ADDRESS, etc.)
- **Advanced AI Type Inference** - Multilingual pattern recognition (Korean, Japanese, Chinese)
- **Git-like Version Control** - 7/7 git features working (100% complete)
  - Branch management, commits, diff, merge, PR, rollback, history
  - 3-stage diff engine and rebase-based merging
- **Relationship Management** - Automatic Property-to-Relationship conversion
- **Multi-language Support** - Comprehensive internationalization
- **Performance Optimization** - 95%+ success rate, <5s response time
- **Security Features** - Input sanitization, authentication, audit logging
- **TerminusDB v11.x Integration** - Full schema type support

### Frontend Technology Stack
- **React 18 + TypeScript 5** - Modern UI framework with strict typing
- **Blueprint.js 5** - Palantir's enterprise UI toolkit
- **Vite 5** - Fast build tool and development server
- **State Management** - Zustand with immer, Relay for GraphQL
- **Visualization** - Cytoscape.js, D3.js, React Flow, Three.js
- **Real-time Collaboration** - Yjs CRDT, Socket.io
- **Testing** - Vitest, React Testing Library, MSW v2
- **Accessibility** - WCAG 2.0 compliant with comprehensive keyboard navigation

### Current Implementation Status
- **Backend Services**: ✅ 90-95% Complete (Production-ready)
- **Frontend Infrastructure**: ✅ 100% Complete (Development-ready)
- **Frontend Components**: ⚠️ 30-40% Complete (GlobalSidebar done, core features needed)

### Service Configuration
- **Port Assignments**:
  - BFF: 8002 (Frontend API gateway)
  - OMS: 8000 (Core ontology service)
  - Funnel: 8004 (Type inference service)
  - TerminusDB: 6364 (Graph database)

## Documentation Standards

### File Naming Conventions
- Main guides use UPPERCASE (e.g., `ARCHITECTURE.md`)
- Descriptive names with underscores for multi-word files
- No language suffixes (English is the standard)

### Content Structure
- Table of contents for documents >50 lines
- Clear section headers with proper hierarchy (H1-H4)
- Code examples with proper syntax highlighting
- Mermaid diagrams for complex architecture concepts
- Performance metrics and implementation status clearly marked

### Writing Guidelines
- Use present tense for current features
- Be concise but comprehensive
- Include practical examples and code snippets
- Maintain technical accuracy with regular updates
- Use emojis sparingly for status indicators (✅ ⚠️ 🆕 🔄 📁)

## Contributing to Documentation

When adding new documentation:
1. Place in the appropriate category (docs/ for project-wide, backend/docs/ for backend-specific)
2. Update this README.md with document links
3. Follow existing format standards and naming conventions
4. Include creation and update dates
5. Review for technical accuracy and completeness

### Documentation Review Process
1. Technical review by development team
2. Editorial review for clarity and consistency
3. Security review for sensitive information
4. Final approval by project lead

## Recent Major Reorganization (2025-07-26)

### ✅ Completed Reorganization
- **Deleted 22 duplicate/obsolete files**: Removed redundant documentation and temporary files
- **Merged 3 architecture documents** into unified [ARCHITECTURE.md](ARCHITECTURE.md)
- **Merged 7 frontend documents** into comprehensive [FRONTEND_GUIDE.md](FRONTEND_GUIDE.md)
- **Moved 2 files** to appropriate backend documentation folders
- **Fixed 1 filename typo**: DesignSysyem.md → DesignSystem.md
- **Reorganized structure** for clarity and maintainability

### Document Reduction Summary
- **Before**: 47 documentation files (with many duplicates)
- **After**: 25 well-organized files (47% reduction)
- **Eliminated**: All redundant and temporary documentation
- **Improved**: Clear hierarchy and easy navigation

### Key Improvements
- **No More Duplicates**: Single source of truth for each topic
- **Logical Organization**: Clear separation between project-wide and backend-specific docs
- **Updated Content**: All documentation reflects current implementation status
- **Better Navigation**: Simplified structure with clear entry points

## External Resources

- [TerminusDB Documentation](https://terminusdb.com/docs/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Pydantic Documentation](https://pydantic-docs.helpmanual.io/)
- [Blueprint.js Documentation](https://blueprintjs.com/docs/)
- [React Documentation](https://react.dev/)
- [TypeScript Documentation](https://www.typescriptlang.org/docs/)
- [Docker Documentation](https://docs.docker.com/)

## Support

For documentation issues or questions:
- Open an issue in the GitHub repository
- Contact the documentation team
- Submit a pull request with improvements

---
*Last updated: 2025-07-26*
*Documentation reorganization: Complete*
*Version: 3.0 (Major reorganization)*