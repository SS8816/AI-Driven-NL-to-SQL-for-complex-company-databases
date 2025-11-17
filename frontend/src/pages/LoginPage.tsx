import { useState, FormEvent } from 'react';
import { Navigate } from 'react-router-dom';
import { Database } from 'lucide-react';
import { useAuthStore } from '@/stores/authStore';
import { Button, Input } from '@/components/common';
import { config } from '@/config';

export function LoginPage() {
  const { login, isAuthenticated, isLoading, error } = useAuthStore();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');

  // If already authenticated, redirect to app
  if (isAuthenticated) {
    return <Navigate to="/" replace />;
  }

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    try {
      await login({ username, password });
    } catch (error) {
      // Error is already set in store
      console.error('Login failed:', error);
    }
  };

  return (
    <div className="min-h-screen bg-dark-bg flex items-center justify-center p-4">
      <div className="w-full max-w-md">
        {/* Logo & Title */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-primary-600 mb-4">
            <Database className="w-8 h-8 text-white" />
          </div>
          <h1 className="text-2xl font-bold text-gray-100 mb-2">
            {config.app.name}
          </h1>
          <p className="text-gray-400">{config.app.description}</p>
        </div>

        {/* Login Form */}
        <div className="card">
          <form onSubmit={handleSubmit} className="space-y-4">
            <Input
              label="Username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Enter your username"
              required
              autoFocus
            />

            <Input
              label="Password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Enter your password"
              required
            />

            {error && (
              <div className="bg-error/10 border border-error/30 rounded-lg p-3 text-sm text-error">
                {error}
              </div>
            )}

            <Button
              type="submit"
              variant="primary"
              className="w-full"
              isLoading={isLoading}
              loadingText="Signing in..."
            >
              Sign In
            </Button>
          </form>
        </div>

        <p className="text-center text-sm text-gray-500 mt-6">
          Powered by Azure OpenAI • AWS Athena • LangGraph
        </p>
      </div>
    </div>
  );
}
