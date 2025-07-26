/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_GRAPHQL_ENDPOINT: string;
  readonly VITE_WEBSOCKET_ENDPOINT: string;
  readonly VITE_API_BASE_URL: string;
  readonly VITE_APP_VERSION: string;
  readonly VITE_ENVIRONMENT: 'development' | 'staging' | 'production';
  readonly VITE_ENABLE_MOCK_DATA: string;
  readonly VITE_ENABLE_DEV_TOOLS: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}