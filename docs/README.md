# SPICE HARVESTER Documentation

Welcome to the SPICE HARVESTER project documentation. This directory contains all project documentation organized by category and thoroughly reorganized for clarity and maintainability.

## Documentation Overview

### Core Documentation

- **[System Architecture](ARCHITECTURE.md)** - Comprehensive system architecture with microservices design, implementation status, and performance metrics
- **[Frontend UI/UX Spec](frontend.md)** - BFF-aligned frontend implementation spec (Blueprint.js + Palantir 3-pane)
- **[Frontend Policies](FRONTEND_POLICIES.md)** - Frontend state/auth/query/command tracking policies
- **[API Reference](API_REFERENCE.md)** - Complete API documentation for all services
- **[Operations Manual](OPERATIONS.md)** - Deployment, maintenance, and operational procedures
- **[DevOps Risk & Cost Report](DEVOPS_MSA_RISK_COST_REPORT.md)** - Senior DevOps 관점 운영 리스크/코스트 분석 및 우선순위 개선안
- **[LLM Integration Blueprint](LLM_INTEGRATION.md)** - LLM을 Funnel/OMS/Graph/Lineage에 안전하게 결합하는 설계(도메인 중립)
- **[Security Documentation](SECURITY.md)** - Security architecture, guidelines, and compliance
- **[Data Lineage](DATA_LINEAGE.md)** - Provenance/lineage 그래프 설계 및 운영(백필/지표 포함)
- **[Audit Logs](AUDIT_LOGS.md)** - 감사 로그 스키마/보장(guarantees) 및 운영 가이드
- **[Idempotency Contract](IDEMPOTENCY_CONTRACT.md)** - 재시도/중복 처리(At-least-once) 안전성 계약
- **[UI/UX Guidelines](UIUX.md)** - User interface and experience design standards
- **[Design System](DesignSystem.md)** - Design system documentation and guidelines

### Quick Start Guides

#### For Developers
1. Start with the [System Architecture](ARCHITECTURE.md) to understand the system topology and data/control planes
2. Follow the root [README](../README.md) for backend setup instructions
3. Use the [Frontend UI/UX Spec](frontend.md) for frontend development
4. Review the [API Reference](API_REFERENCE.md) for endpoint details
5. Check [Security Documentation](SECURITY.md) for secure coding practices

#### For Frontend Developers
1. Read the [Frontend UI/UX Spec](frontend.md) for comprehensive setup and patterns
2. Check the [System Architecture](ARCHITECTURE.md) to understand backend services
3. Review [Design System](DesignSystem.md) for UI component guidelines
4. Follow [UI/UX Guidelines](UIUX.md) for design standards
5. Follow [Frontend Policies](FRONTEND_POLICIES.md) for state/auth/query rules

#### For DevOps Engineers
1. Follow the [Operations Manual](OPERATIONS.md) for deployment
2. Review [Security Documentation](SECURITY.md) for security configuration
3. Read the [DevOps Risk & Cost Report](DEVOPS_MSA_RISK_COST_REPORT.md) for 운영 리스크/코스트 우선순위
4. Check monitoring and maintenance procedures (incl. [Data Lineage](DATA_LINEAGE.md), [Audit Logs](AUDIT_LOGS.md))

#### For QA Engineers
1. Review the test structure in [Backend testing docs](../backend/docs/testing/COMPLEX_TYPES_TEST_README.md)
2. Check API endpoints in [API Reference](API_REFERENCE.md)
3. Follow testing procedures in relevant sections

## Documentation Structure

```
docs/
├── README.md               # This file - documentation index
├── ARCHITECTURE.md         # 🔄 Unified system architecture (3 docs merged)
├── frontend.md             # ✅ Frontend UI/UX spec (BFF-aligned)
├── FRONTEND_POLICIES.md    # Frontend policies (URL SSoT, auth, command tracking)
├── API_REFERENCE.md        # Complete API documentation
├── OPERATIONS.md          # Operations and deployment manual
├── DEVOPS_MSA_RISK_COST_REPORT.md  # 🆕 DevOps 운영 리스크/코스트 보고서
├── LLM_INTEGRATION.md      # 🆕 LLM 결합 설계(도메인 중립/엔터프라이즈 안전)
├── SECURITY.md            # Security documentation
├── DATA_LINEAGE.md        # Data lineage / provenance 운영 가이드
├── AUDIT_LOGS.md          # Audit logs 스키마/보장 및 운영
├── IDEMPOTENCY_CONTRACT.md  # Idempotency(멱등성) 계약/가이드
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
- **BFF (Backend for Frontend)** - Port 8002: API gateway, routing, async command tracking
- **OMS (Ontology Management Service)** - Port 8000: Ontology + graph operations on TerminusDB
- **Funnel (Type Inference Service)** - Port 8003: schema/type inference utilities
- **Pipeline/Objectify Workers** - ETL transforms + dataset → ontology instance mapping
- **Connector Services** - Google Sheets ingest/preview/polling
- **Shared Components** - registries, validators, security, observability

### Technical Capabilities
- **Event sourcing + idempotency** - processed_event registry, expected_seq OCC, replayable projections
- **Ontology + relationship management** - schema validation, link types, link edits overlay
- **Data plane** - ingest, pipeline transforms (filter/join/cast/dedupe), schema contracts, expectations
- **Objectify** - mapping spec → bulk instance creation
- **Branching/versions** - TerminusDB-backed branches, diffs, merges
- **Lineage + audit** - lineage graph, audit logs, gate results
- **Connectors** - Google Sheets OAuth + ingest flow
- **Security/observability** - input sanitization, rate limiting, metrics, tracing

### Frontend Technology Stack
- **React 18 + TypeScript 5** - Modern UI framework with strict typing
- **Blueprint.js 6** - Palantir's enterprise UI toolkit
- **Vite 7** - Fast build tool and development server
- **State Management** - Zustand + TanStack Query
- **UI Icons** - @blueprintjs/icons

### Current Implementation Status
- **Backend services/workers**: runnable via `docker-compose.full.yml`; production readiness depends on environment hardening
- **Frontend**: React + Blueprint app scaffold with BFF-aligned API routes

### Service Configuration
- **Port Assignments**:
  - BFF: 8002 (Frontend API gateway)
  - OMS: 8000 (Core ontology service)
  - Funnel: 8003 (Type inference service)
  - TerminusDB: 6363 (Graph database)

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
- **Frontend spec** consolidated in [frontend.md](frontend.md) with BFF-aligned UI/UX details
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
*Last updated: 2025-12-17*
*Documentation reorganization: Complete*
*Version: 3.0 (Major reorganization)*
