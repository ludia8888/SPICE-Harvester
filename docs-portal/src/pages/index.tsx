import React from "react";
import Layout from "@theme/Layout";
import Translate, { translate } from "@docusaurus/Translate";
import HeroSection from "@site/src/components/layout/HeroSection";
import FeatureGrid from "@site/src/components/layout/FeatureGrid";
import type { Feature } from "@site/src/components/layout/FeatureGrid";
import styles from "./index.module.css";

/* ------------------------------------------------------------------ */
/* Data                                                                */
/* ------------------------------------------------------------------ */

const STATS = [
  {
    value: "275+",
    label: translate({
      id: "homepage.stats.apiEndpoints",
      message: "API Endpoints",
    }),
  },
  {
    value: "33+",
    label: translate({
      id: "homepage.stats.services",
      message: "Services",
    }),
  },
  {
    value: "290+",
    label: translate({
      id: "homepage.stats.errorCodes",
      message: "Error Codes",
    }),
  },
  {
    value: "6",
    label: translate({
      id: "homepage.stats.runtimePaths",
      message: "Runtime Paths",
    }),
  },
] as const;

const FEATURES: Feature[] = [
  {
    icon: "\u{1F9E9}",
    title: translate({
      id: "homepage.features.ontology.title",
      message: "Ontology Management",
    }),
    description:
      translate({
        id: "homepage.features.ontology.description",
        message:
          "Define and evolve your data model with type-safe schemas, properties, and relationships.",
      }),
    link: "/docs/api/overview",
  },
  {
    icon: "\u{1F504}",
    title: translate({
      id: "homepage.features.pipeline.title",
      message: "Pipeline Builder",
    }),
    description:
      translate({
        id: "homepage.features.pipeline.description",
        message:
          "Build data transformation pipelines with 22+ transforms and AI-assisted composition.",
      }),
    link: "/docs/guides/data-engineer/schema-config",
  },
  {
    icon: "\u26A1",
    title: translate({
      id: "homepage.features.action.title",
      message: "Action System",
    }),
    description:
      translate({
        id: "homepage.features.action.description",
        message:
          "Execute business logic through templated actions with undo support and conflict resolution.",
      }),
    link: "/docs/api/overview",
  },
  {
    icon: "\u{1F50D}",
    title: translate({
      id: "homepage.features.lineage.title",
      message: "Data Lineage",
    }),
    description:
      translate({
        id: "homepage.features.lineage.description",
        message:
          "Track data provenance across 15 edge types with SHA256 audit trails.",
      }),
    link: "/docs/architecture/overview",
  },
  {
    icon: "\u{1F50E}",
    title: translate({
      id: "homepage.features.search.title",
      message: "Search & Query",
    }),
    description:
      translate({
        id: "homepage.features.search.description",
        message:
          "Foundry-compatible search with 13 operators, faceted results, and full-text indexing.",
      }),
    link: "/docs/api/overview",
  },
  {
    icon: "\u{1F4CA}",
    title: translate({
      id: "homepage.features.projections.title",
      message: "Real-time Projections",
    }),
    description:
      translate({
        id: "homepage.features.projections.description",
        message:
          "Materialize computed views with eventual consistency and projection replay.",
      }),
    link: "/docs/architecture/overview",
  },
];

const ARCHITECTURE_DIAGRAM = `graph LR
    A[Client SDK] --> B[API Gateway]
    B --> C[Ontology Service]
    B --> D[Pipeline Engine]
    B --> E[Action Executor]
    C --> F[(Object Storage)]
    D --> F
    E --> G[Event Bus]
    G --> H[Projection Engine]
    H --> F
    C --> I[Search Index]
    D --> I`;

const STEPS = [
  {
    title: translate({
      id: "homepage.steps.install.title",
      message: "Install",
    }),
    description: translate({
      id: "homepage.steps.install.description",
      message: "Add Spice OS to your project with a single command.",
    }),
    code: [
      {
        type: "comment",
        text: translate({
          id: "homepage.steps.install.codeComment",
          message: "# Install the CLI and SDK",
        }),
      },
      { type: "keyword", text: "npm install " },
      { type: "string", text: "@spice/harvester-sdk" },
    ],
  },
  {
    title: translate({
      id: "homepage.steps.configure.title",
      message: "Configure",
    }),
    description: translate({
      id: "homepage.steps.configure.description",
      message: "Connect to your data sources and define your ontology schema.",
    }),
    code: [
      { type: "comment", text: "# spice.config.yaml" },
      { type: "keyword", text: "ontology" },
      { type: "plain", text: ":" },
      { type: "string", text: "\n  namespace: production" },
      { type: "string", text: "\n  version: v2" },
    ],
  },
  {
    title: translate({
      id: "homepage.steps.deploy.title",
      message: "Deploy",
    }),
    description: translate({
      id: "homepage.steps.deploy.description",
      message: "Ship to production with built-in observability and scaling.",
    }),
    code: [
      {
        type: "comment",
        text: translate({
          id: "homepage.steps.deploy.codeComment",
          message: "# Deploy to your cluster",
        }),
      },
      { type: "keyword", text: "spice deploy " },
      { type: "string", text: "--env production" },
      { type: "plain", text: "\n" },
      { type: "comment", text: "# \u2713 6 services running" },
    ],
  },
] as const;

/* ------------------------------------------------------------------ */
/* Components                                                          */
/* ------------------------------------------------------------------ */

function StatsBar(): React.ReactElement {
  return (
    <div className={styles.statsBar}>
      {STATS.map((stat) => (
        <div key={stat.label} className={styles.stat}>
          <span className={styles.statValue}>{stat.value}</span>
          <span className={styles.statLabel}>{stat.label}</span>
        </div>
      ))}
    </div>
  );
}

function ArchitectureSection(): React.ReactElement {
  return (
    <div className={styles.sectionAlt}>
      <div className={styles.sectionInner}>
        <div className={styles.sectionHeader}>
          <h2 className={styles.sectionTitle}>
            <Translate id="homepage.architecture.title">
              Built for Enterprise Scale
            </Translate>
          </h2>
          <p className={styles.sectionSubtitle}>
            <Translate id="homepage.architecture.subtitle">
              A microservices architecture with 33 independently deployable
              services, event-driven communication, and built-in observability
              across every runtime path.
            </Translate>
          </p>
        </div>
        <div className={styles.architectureContent}>
          <div className={styles.architectureDiagram}>
            <pre>
              <code>{ARCHITECTURE_DIAGRAM}</code>
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}

function GettingStartedSection(): React.ReactElement {
  return (
    <div className={styles.section}>
      <div className={styles.sectionHeader}>
        <h2 className={styles.sectionTitle}>
          <Translate id="homepage.gettingStarted.title">
            Get Started in Minutes
          </Translate>
        </h2>
        <p className={styles.sectionSubtitle}>
          <Translate id="homepage.gettingStarted.subtitle">
            From zero to a running platform in three steps. Full documentation
            covers every configuration option.
          </Translate>
        </p>
      </div>
      <div className={styles.stepsContainer}>
        {STEPS.map((step, index) => (
          <div key={step.title} className={styles.step}>
            <div className={styles.stepNumber}>{index + 1}</div>
            <h3 className={styles.stepTitle}>{step.title}</h3>
            <p className={styles.stepDescription}>{step.description}</p>
            <div className={styles.stepCode}>
              <code>
                {step.code.map((segment, i) => {
                  const key = `${step.title}-${i}`;
                  switch (segment.type) {
                    case "comment":
                      return (
                        <span key={key} className={styles.codeComment}>
                          {segment.text}
                          {"\n"}
                        </span>
                      );
                    case "keyword":
                      return (
                        <span key={key} className={styles.codeKeyword}>
                          {segment.text}
                        </span>
                      );
                    case "string":
                      return (
                        <span key={key} className={styles.codeString}>
                          {segment.text}
                        </span>
                      );
                    default:
                      return <span key={key}>{segment.text}</span>;
                  }
                })}
              </code>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Page                                                                */
/* ------------------------------------------------------------------ */

export default function Home(): React.ReactElement {
  return (
    <Layout
      title={translate({
        id: "homepage.meta.title",
        message: "Enterprise Ontology & Data Platform",
      })}
      description={translate({
        id: "homepage.meta.description",
        message:
          "Spice OS documentation portal — enterprise ontology management, data pipelines, actions, lineage, search, and real-time projections.",
      })}
    >
      <main className={styles.landing}>
        <HeroSection
          title="Spice OS"
          tagline={translate({
            id: "homepage.hero.tagline",
            message: "Enterprise Ontology & Data Platform",
          })}
          primaryAction={{
            label: translate({
              id: "homepage.hero.primaryAction",
              message: "Quick Start",
            }),
            href: "/docs/getting-started/quick-start",
          }}
          secondaryAction={{
            label: translate({
              id: "homepage.hero.secondaryAction",
              message: "API Reference",
            }),
            href: "/docs/api/overview",
          }}
        />

        <StatsBar />

        <div className={styles.section}>
          <div className={styles.sectionHeader}>
            <h2 className={styles.sectionTitle}>
              <Translate id="homepage.capabilities.title">
                Platform Capabilities
              </Translate>
            </h2>
            <p className={styles.sectionSubtitle}>
              <Translate id="homepage.capabilities.subtitle">
                Everything you need to model, transform, query, and operate
                your enterprise data at scale.
              </Translate>
            </p>
          </div>
          <FeatureGrid features={FEATURES} />
        </div>

        <ArchitectureSection />

        <GettingStartedSection />
      </main>
    </Layout>
  );
}
