import { downloadFile } from './client';
import { endpoints } from '@/config';
import { ExportFormat } from '@/types';

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
};
