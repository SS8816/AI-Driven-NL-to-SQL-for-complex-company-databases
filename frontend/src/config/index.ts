export const config = {
  // API Configuration
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000',
  wsBaseUrl: import.meta.env.VITE_WS_BASE_URL || 'ws://localhost:8000',
  apiPrefix: '/api/v1',

  // Mapbox Configuration
  mapbox: {
    accessToken: import.meta.env.VITE_MAPBOX_ACCESS_TOKEN || '',
    defaultStyle: 'mapbox://styles/mapbox/dark-v11',
    defaultCenter: [-98.5795, 39.8283] as [number, number], // US center
    defaultZoom: 3,
  },

  // Application Settings
  app: {
    name: 'AI-Driven NL-to-SQL',
    description: 'Violation Detection System',
    maxPreviewRows: 1000,
    queryHistoryPageSize: 20,
  },

  // Storage Keys
  storage: {
    authToken: 'auth_token',
    user: 'user',
  },

  // Query Execution Modes
  executionModes: {
    sync: 'sync',
    async: 'async',
  },

  // Default Values
  defaults: {
    executionMode: 'sync' as const,
    ruleCategory: 'geospatial_violations',
  },
} as const;

// API Endpoints
export const endpoints = {
  auth: {
    login: '/auth/login',
    me: '/auth/me',
    logout: '/auth/logout',
  },
  schemas: {
    list: '/schemas',
    detail: (name: string) => `/schemas/${name}`,
    summary: (name: string) => `/schemas/${name}/summary`,
    analyze: '/schemas/analyze',
    redactedDDL: '/schemas/redacted-ddl',
  },
  queries: {
    execute: '/queries/execute',
    history: '/queries/history',
    bookmark: (id: number) => `/queries/${id}/bookmark`,
    websocket: '/ws/execute',
  },
  results: {
    export: (ctasTableName: string) => `/results/${ctasTableName}/export`,
    schema: (ctasTableName: string) => `/results/${ctasTableName}/schema`,
    countries: (ctasTableName: string) => `/results/${ctasTableName}/countries`,
    query: (ctasTableName: string) => `/results/${ctasTableName}/query`,
  },
  cache: {
    stats: '/cache/stats',
    clearExpired: '/cache/expired',
    invalidate: (rule: string, database: string) => `/cache/${rule}/${database}`,
    rules: '/cache/rules',
  },
} as const;
