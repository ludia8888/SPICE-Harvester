import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

const config: Config = {
  title: "SPICE Harvester",
  tagline: "Enterprise Ontology & Data Platform",
  favicon: "img/favicon.ico",

  future: {
    v4: true,
  },

  url: "https://docs.spice-harvester.io",
  baseUrl: "/",

  organizationName: "spice-harvester",
  projectName: "spice-harvester",

  onBrokenLinks: "throw",

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: "throw",
    },
  },

  themes: ["@docusaurus/theme-mermaid"],

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      {
        docs: {
          sidebarPath: "./sidebars.ts",
        },
        blog: false,
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    [
      "@cmfcmf/docusaurus-search-local",
      {
        indexDocs: true,
        indexBlog: false,
        language: "en",
      },
    ],
  ],

  themeConfig: {
    image: "img/docusaurus-social-card.jpg",
    colorMode: {
      defaultMode: "light",
      respectPrefersColorScheme: true,
    },
    docs: {
      sidebar: {
        hideable: true,
        autoCollapseCategories: true,
      },
    },
    navbar: {
      title: "SPICE Harvester",
      logo: {
        alt: "SPICE Harvester",
        src: "img/logo.svg",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "docsSidebar",
          position: "left",
          label: "Docs",
        },
        {
          type: "docSidebar",
          sidebarId: "apiSidebar",
          position: "left",
          label: "API Reference",
        },
        {
          to: "/docs/architecture/overview",
          label: "Architecture",
          position: "left",
        },
        {
          href: "https://github.com/ludia8888/SPICE-Harvester",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Documentation",
          items: [
            { label: "Quick Start", to: "/docs/getting-started/quick-start" },
            { label: "API Reference", to: "/docs/api/overview" },
            { label: "Architecture", to: "/docs/architecture/overview" },
          ],
        },
        {
          title: "Guides",
          items: [
            {
              label: "Data Engineer",
              to: "/docs/guides/data-engineer/schema-config",
            },
            {
              label: "Platform Admin",
              to: "/docs/guides/platform-admin/service-monitoring",
            },
            {
              label: "Integration Developer",
              to: "/docs/guides/integration-developer/rest-api",
            },
          ],
        },
        {
          title: "Operations",
          items: [
            { label: "Monitoring", to: "/docs/operations/monitoring" },
            {
              label: "Troubleshooting",
              to: "/docs/operations/troubleshooting",
            },
            { label: "Error Codes", to: "/docs/reference/error-codes" },
          ],
        },
        {
          title: "Resources",
          items: [
            {
              label: "GitHub",
              href: "https://github.com/ludia8888/SPICE-Harvester",
            },
            { label: "Contributing", to: "/docs/contributing/dev-setup" },
          ],
        },
      ],
      copyright: `Copyright \u00a9 ${new Date().getFullYear()} SPICE Harvester. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: [
        "bash",
        "python",
        "yaml",
        "json",
        "sql",
        "docker",
        "http",
      ],
    },
    mermaid: {
      theme: { light: "neutral", dark: "dark" },
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
