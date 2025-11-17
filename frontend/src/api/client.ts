import axios, { AxiosError, AxiosInstance, AxiosRequestConfig, AxiosResponse } from 'axios';
import { config as appConfig } from '@/config';
import { storage } from '@/utils/storage';
import { ApiResponse } from '@/types';

/**
 * Create axios instance with base configuration
 */
const createApiClient = (): AxiosInstance => {
  const client = axios.create({
    baseURL: `${appConfig.apiBaseUrl}${appConfig.apiPrefix}`,
    timeout: 300000, // 5 minutes for long queries
    headers: {
      'Content-Type': 'application/json',
    },
  });

  // Request interceptor - add auth token
  client.interceptors.request.use(
    (config) => {
      const token = storage.get<string>(appConfig.storage.authToken);
      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
      return config;
    },
    (error) => {
      return Promise.reject(error);
    }
  );

  // Response interceptor - handle errors
  client.interceptors.response.use(
    (response: AxiosResponse) => response,
    (error: AxiosError<ApiResponse>) => {
      // Handle 401 Unauthorized - clear auth and redirect to login
      if (error.response?.status === 401) {
        storage.remove(appConfig.storage.authToken);
        storage.remove(appConfig.storage.user);
        window.location.href = '/login';
      }

      // Extract error message
      const message =
        error.response?.data?.message ||
        error.message ||
        'An unexpected error occurred';

      return Promise.reject({
        message,
        code: error.response?.data?.error_code,
        details: error.response?.data?.details,
        status: error.response?.status,
      });
    }
  );

  return client;
};

export const apiClient = createApiClient();

/**
 * Generic request wrapper with error handling
 */
export async function request<T = any>(
  config: AxiosRequestConfig
): Promise<T> {
  try {
    const response = await apiClient.request<T>(config);
    return response.data;
  } catch (error) {
    throw error;
  }
}

/**
 * HTTP method helpers
 */
export const api = {
  get: <T = any>(url: string, config?: AxiosRequestConfig) =>
    request<T>({ ...config, method: 'GET', url }),

  post: <T = any>(url: string, data?: any, config?: AxiosRequestConfig) =>
    request<T>({ ...config, method: 'POST', url, data }),

  put: <T = any>(url: string, data?: any, config?: AxiosRequestConfig) =>
    request<T>({ ...config, method: 'PUT', url, data }),

  patch: <T = any>(url: string, data?: any, config?: AxiosRequestConfig) =>
    request<T>({ ...config, method: 'PATCH', url, data }),

  delete: <T = any>(url: string, config?: AxiosRequestConfig) =>
    request<T>({ ...config, method: 'DELETE', url }),
};

/**
 * Download file helper
 */
export async function downloadFile(
  url: string,
  filename: string,
  params?: Record<string, any>
): Promise<void> {
  try {
    const response = await apiClient.get(url, {
      params,
      responseType: 'blob',
    });

    // Create blob link to download
    const blob = new Blob([response.data]);
    const link = document.createElement('a');
    link.href = window.URL.createObjectURL(blob);
    link.download = filename;
    link.click();
    window.URL.revokeObjectURL(link.href);
  } catch (error) {
    console.error('Download failed:', error);
    throw error;
  }
}
