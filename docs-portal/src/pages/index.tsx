import React from "react";
import Layout from "@theme/Layout";
import HeroSection from "@site/src/components/layout/HeroSection";
import FeatureGrid from "@site/src/components/layout/FeatureGrid";
import type { Feature } from "@site/src/components/layout/FeatureGrid";
import styles from "./index.module.css";

/* ------------------------------------------------------------------ */
/* Data                                                                */
/* ------------------------------------------------------------------ */

const STATS = [
  { value: "275+", label: "API Endpoints" },
  { value: "33+", label: "Services" },
  { value: "290+", label: "Error Codes" },
  { value: "6", label: "Runtime Paths" },
] as const;

const FEATURES: Feature[] = [
  {
    icon: "\u{1F9E9}",
    title: "Ontology Management",
    description:
      "Define and evolve your data model with type-safe schemas, properties, and relationships.",
    link: "/docs/api/overview",
  },
  {
    icon: "\u{1F504}",
    title: "Pipeline Builder",
    description:
      "Build data transformation pipelines with 22+ transforms and AI-assisted composition.",
    link: "/docs/guides/data-engineer/schema-config",
  },
  {
    icon: "\u26A1",
    title: "Action System",
    description:
      "Execute business logic through templated actions with undo support and conflict resolution.",
    link: "/docs/api/overview",
  },
  {
    icon: "\u{1F50D}",
    title: "Data Lineage",
    description:
      "Track data provenance across 15 edge types with SHA256 audit trails.",
    link: "/docs/architecture/overview",
  },
  {
    icon: "\u{1F50E}",
    title: "Search & Query",
    description:
      "Foundry-compatible search with 13 operators, faceted results, and full-text indexing.",
    link: "/docs/api/overview",
  },
  {
    icon: "\u{1F4CA}",
    title: "Real-time Projections",
    description:
      "Materialize computed views with eventual consistency and projection replay.",
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
    title: "Install",
    description: "Add SPICE Harvester to your project with a single command.",
    code: [
      { type: "comment", text: "# Install the CLI and SDK" },
      { type: "keyword", text: "npm install " },
      { type: "string", text: "@spice/harvester-sdk" },
    ],
  },
  {
    title: "Configure",
    description: "Connect to your data sources and define your ontology schema.",
    code: [
      { type: "comment", text: "# spice.config.yaml" },
      { type: "keyword", text: "ontology" },
      { type: "plain", text: ":" },
      { type: "string", text: "\n  namespace: production" },
      { type: "string", text: "\n  version: v2" },
    ],
  },
  {
    title: "Deploy",
    description: "Ship to production with built-in observability and scaling.",
    code: [
      { type: "comment", text: "# Deploy to your cluster" },
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
            Built for Enterprise Scale
          </h2>
          <p className={styles.sectionSubtitle}>
            A microservices architecture with 33 independently deployable
            services, event-driven communication, and built-in observability
            across every runtime path.
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
        <h2 className={styles.sectionTitle}>Get Started in Minutes</h2>
        <p className={styles.sectionSubtitle}>
          From zero to a running platform in three steps. Full documentation
          covers every configuration option.
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
      title="Enterprise Ontology & Data Platform"
      description="SPICE Harvester documentation portal — enterprise ontology management, data pipelines, actions, lineage, search, and real-time projections."
    >
      <main className={styles.landing}>
        <HeroSection
          title="SPICE Harvester"
          tagline="Enterprise Ontology & Data Platform"
          primaryAction={{
            label: "Quick Start",
            href: "/docs/getting-started/quick-start",
          }}
          secondaryAction={{
            label: "API Reference",
            href: "/docs/api/overview",
          }}
        />

        <StatsBar />

        <div className={styles.section}>
          <div className={styles.sectionHeader}>
            <h2 className={styles.sectionTitle}>Platform Capabilities</h2>
            <p className={styles.sectionSubtitle}>
              Everything you need to model, transform, query, and operate
              your enterprise data at scale.
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
