import { create } from 'zustand';
import { User, LoginRequest } from '@/types';
import { authApi } from '@/api';
import { storage } from '@/utils/storage';
import { config } from '@/config';

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;

  // Actions
  login: (credentials: LoginRequest) => Promise<void>;
  logout: () => void;
  checkAuth: () => Promise<void>;
  clearError: () => void;
}

export const useAuthStore = create<AuthState>((set, get) => ({
  user: storage.get<User>(config.storage.user),
  token: storage.get<string>(config.storage.authToken),
  isAuthenticated: !!storage.get<string>(config.storage.authToken),
  isLoading: false,
  error: null,

  login: async (credentials: LoginRequest) => {
    set({ isLoading: true, error: null });
    try {
      const response = await authApi.login(credentials);

      // Store token and user
      storage.set(config.storage.authToken, response.access_token);
      storage.set(config.storage.user, response.user);

      set({
        user: response.user,
        token: response.access_token,
        isAuthenticated: true,
        isLoading: false,
        error: null,
      });
    } catch (error: any) {
      set({
        error: error.message || 'Login failed',
        isLoading: false,
        isAuthenticated: false,
      });
      throw error;
    }
  },

  logout: () => {
    // Call logout API (fire and forget)
    authApi.logout().catch(console.error);

    // Clear storage
    storage.remove(config.storage.authToken);
    storage.remove(config.storage.user);

    set({
      user: null,
      token: null,
      isAuthenticated: false,
      error: null,
    });
  },

  checkAuth: async () => {
    const token = storage.get<string>(config.storage.authToken);
    if (!token) {
      set({ isAuthenticated: false, user: null, token: null });
      return;
    }

    try {
      const user = await authApi.me();
      storage.set(config.storage.user, user);

      set({
        user,
        token,
        isAuthenticated: true,
        error: null,
      });
    } catch (error) {
      // Token is invalid, clear auth
      storage.remove(config.storage.authToken);
      storage.remove(config.storage.user);

      set({
        user: null,
        token: null,
        isAuthenticated: false,
      });
    }
  },

  clearError: () => set({ error: null }),
}));
