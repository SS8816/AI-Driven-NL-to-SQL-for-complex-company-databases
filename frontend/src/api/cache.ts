import { api } from './client';
import { endpoints } from '@/config';
import { CacheStats } from '@/types';

export const cacheApi = {
  /**
   * Get cache statistics
   */
  getStats: async (): Promise<CacheStats> => {
    return api.get<CacheStats>(endpoints.cache.stats);
  },

  /**
   * Clear expired cache entries
   */
  clearExpired: async (): Promise<{ deleted_count: number }> => {
    return api.delete<{ deleted_count: number }>(endpoints.cache.clearExpired);
  },

  /**
   * Invalidate cache for specific rule and database
   */
  invalidate: async (
    ruleCategory: string,
    database: string
  ): Promise<{ deleted_count: number }> => {
    return api.delete<{ deleted_count: number }>(
      endpoints.cache.invalidate(ruleCategory, database)
    );
  },

  /**
   * List all cached rules
   */
  listRules: async (): Promise<{ rules: string[] }> => {
    return api.get<{ rules: string[] }>(endpoints.cache.rules);
  },
};
