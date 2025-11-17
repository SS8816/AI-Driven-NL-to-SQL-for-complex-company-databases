import { api, downloadFile } from './client';
import { endpoints } from '@/config';
import {
  ExportFormat,
  CTASSchemaResponse,
  CTASCountriesResponse,
  CTASQueryRequest,
  CTASQueryResponse,
} from '@/types';

export const resultsApi = {
  /**
   * Export query results to specified format
   */
  export: async (
    ctasTableName: string,
    database: string,
    format: ExportFormat,
    filterSql?: string
  ): Promise<void> => {
    const filename = `${ctasTableName}.${format}`;
    const params: Record<string, any> = {
      database,
      format,
    };

    if (filterSql) {
      params.filter = filterSql;
    }

    await downloadFile(endpoints.results.export(ctasTableName), filename, params);
  },

  /**
   * Get CTAS table schema
   */
  getSchema: async (
    ctasTableName: string,
    database: string
  ): Promise<CTASSchemaResponse> => {
    const response = await api.get(endpoints.results.schema(ctasTableName), {
      params: { database },
    });
    return response.data;
  },

  /**
   * Get distinct countries from CTAS table
   */
  getCountries: async (
    ctasTableName: string,
    database: string
  ): Promise<CTASCountriesResponse> => {
    const response = await api.get(endpoints.results.countries(ctasTableName), {
      params: { database },
    });
    return response.data;
  },

  /**
   * Execute custom SQL query on CTAS table
   */
  executeQuery: async (
    ctasTableName: string,
    database: string,
    queryRequest: CTASQueryRequest
  ): Promise<CTASQueryResponse> => {
    const response = await api.post(
      endpoints.results.query(ctasTableName),
      queryRequest,
      {
        params: { database },
      }
    );
    return response.data;
  },
};
