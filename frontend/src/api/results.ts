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
    // api.get already returns response.data, no need to extract again
    return await api.get(endpoints.results.schema(ctasTableName), {
      params: { database },
    });
  },

  /**
   * Get distinct countries from CTAS table
   */
  getCountries: async (
    ctasTableName: string,
    database: string
  ): Promise<CTASCountriesResponse> => {
    // api.get already returns response.data, no need to extract again
    return await api.get(endpoints.results.countries(ctasTableName), {
      params: { database },
    });
  },

  /**
   * Execute custom SQL query on CTAS table
   */
  executeQuery: async (
    ctasTableName: string,
    database: string,
    queryRequest: CTASQueryRequest
  ): Promise<CTASQueryResponse> => {
    // api.post already returns response.data, no need to extract again
    return await api.post(
      endpoints.results.query(ctasTableName),
      queryRequest,
      {
        params: { database },
      }
    );
  },
};
