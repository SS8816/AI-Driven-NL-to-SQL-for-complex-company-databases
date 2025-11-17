import { api } from './client';
import { endpoints } from '@/config';
import { SchemaListResponse, SchemaInfo, EntityExtractionResult } from '@/types';

export const schemasApi = {
  /**
   * List all available schemas
   */
  list: async (): Promise<SchemaListResponse> => {
    return api.get<SchemaListResponse>(endpoints.schemas.list);
  },

  /**
   * Get detailed schema information
   */
  getDetail: async (name: string): Promise<SchemaInfo> => {
    return api.get<SchemaInfo>(endpoints.schemas.detail(name));
  },

  /**
   * Get schema summary (LLM-optimized)
   */
  getSummary: async (name: string): Promise<{ summary: string }> => {
    return api.get<{ summary: string }>(endpoints.schemas.summary(name));
  },

  /**
   * Analyze query and extract relevant tables/columns
   */
  analyze: async (
    schemaName: string,
    nlQuery: string
  ): Promise<EntityExtractionResult> => {
    return api.post<EntityExtractionResult>(endpoints.schemas.analyze, {
      schema_name: schemaName,
      nl_query: nlQuery,
    });
  },

  /**
   * Get redacted DDL for selected tables and columns
   */
  getRedactedDDL: async (
    schemaName: string,
    selectedTables: Record<string, string[]>
  ): Promise<{ ddl: string; table_count: number; total_columns: number }> => {
    return api.post<{ ddl: string; table_count: number; total_columns: number }>(
      endpoints.schemas.redactedDDL,
      {
        schema_name: schemaName,
        selected_tables: selectedTables,
      }
    );
  },
};
