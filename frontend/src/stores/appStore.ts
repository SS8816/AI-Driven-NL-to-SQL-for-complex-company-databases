import { create } from 'zustand';

interface AppState {
  // Sidebar
  sidebarCollapsed: boolean;
  toggleSidebar: () => void;

  // Current view
  currentView: 'query' | 'history' | 'cache';
  setView: (view: 'query' | 'history' | 'cache') => void;

  // Selected schema
  selectedSchema: string | null;
  setSelectedSchema: (schema: string | null) => void;

  // Loading states
  isExecutingQuery: boolean;
  setExecutingQuery: (isExecuting: boolean) => void;
}

export const useAppStore = create<AppState>((set) => ({
  // Sidebar state
  sidebarCollapsed: false,
  toggleSidebar: () => set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),

  // Current view
  currentView: 'query',
  setView: (view) => set({ currentView: view }),

  // Selected schema
  selectedSchema: null,
  setSelectedSchema: (schema) => set({ selectedSchema: schema }),

  // Loading states
  isExecutingQuery: false,
  setExecutingQuery: (isExecuting) => set({ isExecutingQuery: isExecuting }),
}));
