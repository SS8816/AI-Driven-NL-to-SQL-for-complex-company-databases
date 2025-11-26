import { api } from './client';

export interface CountryMapping {
  code: string;
  name: string;
  display: string;
}

export const metadataApi = {
  /**
   * Get formatted country list
   */
  getCountries: async (): Promise<CountryMapping[]> => {
    return await api.get<CountryMapping[]>('/metadata/countries/formatted');
  },

  /**
   * Get country code to name mapping
   */
  getCountryMappings: async (): Promise<Record<string, string>> => {
    return await api.get<Record<string, string>>('/metadata/countries');
  },
};
