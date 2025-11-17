// Authentication Types
export interface User {
  username: string;
  email: string;
  full_name?: string;
  department?: string;
}

export interface LoginRequest {
  username: string;
  password: string;
}

export interface LoginResponse {
  access_token: string;
  token_type: string;
  user: User;
}

// Schema Types
export interface ColumnInfo {
  name: string;
  data_type: string;
  comment?: string;
}

export interface TableInfo {
  table_name: string;
  columns: ColumnInfo[];
  row_count?: number;
  description?: string;
}

export interface SchemaInfo {
  name: string;
  database: string;
  tables: TableInfo[];
  total_tables: number;
  total_columns: number;
}

export interface SchemaListResponse {
  schemas: Array<{
    name: string;
    database: string;
    table_count: number;
    description?: string;
  }>;
}

export interface EntityExtractionResult {
  tables: Record<string, string[]>; // table_name -> column_names[]
  reasoning?: string;
  confidence?: number;
}

// Query Types
export interface ExecuteQueryRequest {
  rule_category: string;
  nl_query: string;
  schema_name: string;
  selected_tables: Record<string, string[]>;
  execution_mode: 'normal' | 'reexecute' | 'force';
  guardrails?: string;
}

export interface QueryProgress {
  stage: string;
  message: string;
  progress_percent?: number;
  timestamp: string;
}

export interface QueryResult {
  ctas_table_name: string;
  database: string;
  row_count: number;
  execution_time_seconds: number;
  sql: string;
  preview_data?: Array<Record<string, any>>;
  column_names?: string[];
  has_geometry: boolean;
  error?: string;
  status: 'success' | 'failed' | 'running';
}

export interface QueryHistoryItem {
  id: number;
  rule_category: string;
  nl_query: string;
  sql: string;
  ctas_table_name: string;
  database: string;
  status: string;
  row_count?: number;
  execution_time_seconds?: number;
  created_at: string;
  is_bookmarked: boolean;
}

// WebSocket Message Types
export interface WSMessage {
  type: 'progress' | 'result' | 'error';
  data: QueryProgress | QueryResult | { message: string };
}

// Cache Types
export interface CacheStats {
  total_entries: number;
  active_entries: number;
  expired_entries: number;
  total_size_mb: number;
  hit_rate?: number;
  cached_rules: Array<{
    rule_category: string;
    database: string;
    entry_count: number;
    last_accessed: string;
  }>;
}

// Export Types
export type ExportFormat = 'csv' | 'json' | 'geojson';

export interface ExportRequest {
  ctas_table_name: string;
  database: string;
  format: ExportFormat;
  filter_sql?: string;
}

// API Response Types
export interface ApiResponse<T = any> {
  message?: string;
  data?: T;
  error_code?: string;
  details?: Record<string, any>;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

// UI State Types
export interface ToastMessage {
  id: string;
  type: 'success' | 'error' | 'warning' | 'info';
  message: string;
  duration?: number;
}

export interface AppError {
  message: string;
  code?: string;
  details?: Record<string, any>;
}
