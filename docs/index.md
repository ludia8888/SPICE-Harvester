# SPICE Harvester Documentation

This site is built with Sphinx + MyST (Markdown) and is intended to stay in lockstep with the codebase.

Some pages are auto-managed from code (OpenAPI, architecture inventory, module index, MCP tool catalogs). If a generated
section looks out of date, run `sphinx-build` (it runs the generators during build) or run the scripts listed in each page.

```{toctree}
:maxdepth: 2
:caption: Core

README
ARCHITECTURE
architecture/README
API_REFERENCE
BACKEND_METHODS
ERROR_SPEC
ERROR_TAXONOMY
OPERATIONS
SECURITY
```

```{toctree}
:maxdepth: 1
:caption: Historical Reports

DEVOPS_MSA_RISK_COST_REPORT
```

```{toctree}
:maxdepth: 2
:caption: Data & Agent

PIPELINE_AGENT
AGENT_PRD
AGENT_PRD_CHECKLIST
PipelineBuilder_checklist
DATA_LINEAGE
AUDIT_LOGS
IDEMPOTENCY_CONTRACT
ACTION_WRITEBACK_DESIGN
```

```{toctree}
:maxdepth: 2
:caption: Ontology

ONTOLOGY_AGENT
Ontology
ontology_resource_design
OntologyDeploy_runbook
ActionType_Reference
```

```{toctree}
:maxdepth: 2
:caption: Frontend

frontend
FRONTEND_POLICIES
DesignSystem
UIUX
```

```{toctree}
:maxdepth: 2
:caption: LLM

LLM_INTEGRATION
```

```{toctree}
:maxdepth: 2
:caption: Reference

reference/index
reference/autoapi/index
```

```{toctree}
:maxdepth: 2
:caption: Platform Checklists

platform_checklist/PIPELINE_ARTIFACT_E2E
platform_checklist/VERIFICATION_REPORT
```
