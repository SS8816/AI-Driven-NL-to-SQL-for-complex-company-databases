import { api } from './client';
import { endpoints } from '@/config';
import { LoginRequest, LoginResponse, User } from '@/types';

export const authApi = {
  /**
   * Login with username and password
   */
  login: async (credentials: LoginRequest): Promise<LoginResponse> => {
    return api.post<LoginResponse>(endpoints.auth.login, credentials);
  },

  /**
   * Get current user info
   */
  me: async (): Promise<User> => {
    return api.get<User>(endpoints.auth.me);
  },

  /**
   * Logout
   */
  logout: async (): Promise<void> => {
    return api.post(endpoints.auth.logout);
  },
};
