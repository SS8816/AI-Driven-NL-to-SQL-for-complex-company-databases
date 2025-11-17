import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Toaster } from 'react-hot-toast';
import { MainLayout } from '@/components/layout/MainLayout';
import { ProtectedRoute } from '@/components/auth/ProtectedRoute';
import { LoginPage } from '@/pages/LoginPage';
import { QueryBuilderPage } from '@/pages/QueryBuilderPage';
import { QueryHistoryPage } from '@/pages/QueryHistoryPage';
import { CacheManagementPage } from '@/pages/CacheManagementPage';
import { useAppStore } from '@/stores/appStore';

// Create React Query client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
      staleTime: 5 * 60 * 1000, // 5 minutes
    },
  },
});

function AppContent() {
  const { currentView } = useAppStore();

  return (
    <MainLayout>
      {currentView === 'query' && <QueryBuilderPage />}
      {currentView === 'history' && <QueryHistoryPage />}
      {currentView === 'cache' && <CacheManagementPage />}
    </MainLayout>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          {/* Login Route */}
          <Route path="/login" element={<LoginPage />} />

          {/* Protected Routes */}
          <Route
            path="/"
            element={
              <ProtectedRoute>
                <AppContent />
              </ProtectedRoute>
            }
          />

          {/* Catch all - redirect to home */}
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>

        {/* Toast Notifications */}
        <Toaster
          position="top-right"
          toastOptions={{
            duration: 4000,
            style: {
              background: '#242936',
              color: '#F3F4F6',
              border: '1px solid #2D3548',
              maxWidth: '400px',
              wordWrap: 'break-word',
              wordBreak: 'break-word',
            },
            success: {
              iconTheme: {
                primary: '#00C853',
                secondary: '#F3F4F6',
              },
            },
            error: {
              iconTheme: {
                primary: '#FF3D00',
                secondary: '#F3F4F6',
              },
            },
          }}
        />
      </BrowserRouter>
    </QueryClientProvider>
  );
}
