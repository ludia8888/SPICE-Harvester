import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebars: SidebarsConfig = {
  docsSidebar: [
    {
      type: "category",
      label: "Getting Started",
      collapsed: false,
      items: [
        "getting-started/quick-start",
        "getting-started/installation",
        "getting-started/concepts",
      ],
    },
    {
      type: "category",
      label: "Guides",
      items: [
        {
          type: "category",
          label: "Data Engineer",
          items: [
            "guides/data-engineer/schema-config",
            "guides/data-engineer/pipeline-builder",
            "guides/data-engineer/import-templates",
          ],
        },
        {
          type: "category",
          label: "Platform Admin",
          items: [
            "guides/platform-admin/service-monitoring",
            "guides/platform-admin/rbac",
            "guides/platform-admin/foundry-migration",
          ],
        },
        {
          type: "category",
          label: "Integration Developer",
          items: [
            "guides/integration-developer/rest-api",
            "guides/integration-developer/auth-flow",
            "guides/integration-developer/sdk-guide",
          ],
        },
        {
          type: "category",
          label: "Business Analyst",
          items: [
            "guides/business-analyst/action-templates",
            "guides/business-analyst/search-query",
            "guides/business-analyst/dashboard",
          ],
        },
        {
          type: "category",
          label: "Onboarding",
          items: [
            "guides/onboarding/overview",
            "guides/onboarding/architecture-tour",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Architecture",
      items: [
        "architecture/overview",
        "architecture/data-flow",
        "architecture/service-topology",
        "architecture/class-diagrams",
        "architecture/event-sourcing",
        {
          type: "category",
          label: "Auto-Generated Diagrams",
          items: [
            "architecture/auto-data-flow",
            "architecture/auto-service-interactions",
            "architecture/auto-class-diagrams",
            "architecture/auto-system-classes",
          ],
        },
      ],
    },
    {
      type: "category",
      label: "Reference",
      items: [
        "reference/error-codes",
        "reference/config",
        "reference/auto-config-reference",
        "reference/pipeline-tools",
        "reference/glossary",
      ],
    },
    {
      type: "category",
      label: "Operations",
      items: [
        "operations/monitoring",
        "operations/troubleshooting",
        "operations/projection-consistency",
      ],
    },
    {
      type: "category",
      label: "Contributing",
      items: [
        "contributing/dev-setup",
        "contributing/code-standards",
        "contributing/release-process",
      ],
    },
  ],

  apiSidebar: [
    "api/overview",
    {
      type: "category",
      label: "Ontology v2",
      items: [
        "api/v2-object-types",
        "api/v2-objects",
        "api/v2-search",
        "api/v2-actions",
      ],
    },
    {
      type: "category",
      label: "Complete Reference (Auto-Generated)",
      items: [
        "api/auto-v2-reference",
      ],
    },
    "api/error-codes",
  ],
};

export default sidebars;
