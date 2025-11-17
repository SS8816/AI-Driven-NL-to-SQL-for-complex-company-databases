import {
  Database,
  History,
  HardDrive,
  ChevronLeft,
  ChevronRight,
  LogOut,
  User,
} from 'lucide-react';
import { cn } from '@/utils/cn';
import { useAppStore } from '@/stores/appStore';
import { useAuthStore } from '@/stores/authStore';
import { config } from '@/config';

export function Sidebar() {
  const { sidebarCollapsed, toggleSidebar, currentView, setView } = useAppStore();
  const { user, logout } = useAuthStore();

  const menuItems = [
    {
      id: 'query' as const,
      label: 'Query Builder',
      icon: Database,
      description: 'Create and execute NL queries',
    },
    {
      id: 'history' as const,
      label: 'Query History',
      icon: History,
      description: 'View past queries and results',
    },
    {
      id: 'cache' as const,
      label: 'Cache Management',
      icon: HardDrive,
      description: 'Manage query cache',
    },
  ];

  return (
    <div
      className={cn(
        'bg-dark-sidebar border-r border-dark-border h-full flex flex-col transition-all duration-300',
        sidebarCollapsed ? 'w-20' : 'w-64'
      )}
    >
      {/* Header */}
      <div className="p-4 border-b border-dark-border flex items-center justify-between">
        {!sidebarCollapsed && (
          <div>
            <h1 className="text-lg font-bold text-gray-100">{config.app.name}</h1>
            <p className="text-xs text-gray-400">{config.app.description}</p>
          </div>
        )}
        <button
          onClick={toggleSidebar}
          className="p-2 hover:bg-dark-hover rounded-lg transition-colors"
          title={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          {sidebarCollapsed ? (
            <ChevronRight className="w-5 h-5 text-gray-400" />
          ) : (
            <ChevronLeft className="w-5 h-5 text-gray-400" />
          )}
        </button>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-4 space-y-2">
        {menuItems.map((item) => {
          const Icon = item.icon;
          const isActive = currentView === item.id;

          return (
            <button
              key={item.id}
              onClick={() => setView(item.id)}
              className={cn(
                'sidebar-link w-full',
                isActive && 'sidebar-link-active'
              )}
              title={sidebarCollapsed ? item.label : undefined}
            >
              <Icon className="w-5 h-5 flex-shrink-0" />
              {!sidebarCollapsed && (
                <div className="flex-1 text-left">
                  <div className="font-medium">{item.label}</div>
                  <div className="text-xs text-gray-500">{item.description}</div>
                </div>
              )}
            </button>
          );
        })}
      </nav>

      {/* User Info & Logout */}
      <div className="p-4 border-t border-dark-border">
        {!sidebarCollapsed ? (
          <div className="space-y-3">
            <div className="flex items-center gap-3 px-3 py-2 bg-dark-card rounded-lg">
              <div className="w-8 h-8 rounded-full bg-primary-600 flex items-center justify-center">
                <User className="w-4 h-4 text-white" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-gray-100 truncate">
                  {user?.full_name || user?.username}
                </p>
                <p className="text-xs text-gray-500 truncate">{user?.email}</p>
              </div>
            </div>
            <button
              onClick={logout}
              className="sidebar-link w-full text-error hover:bg-error/10"
            >
              <LogOut className="w-5 h-5" />
              <span>Logout</span>
            </button>
          </div>
        ) : (
          <button
            onClick={logout}
            className="w-full p-2 hover:bg-dark-hover rounded-lg transition-colors flex items-center justify-center text-error"
            title="Logout"
          >
            <LogOut className="w-5 h-5" />
          </button>
        )}
      </div>
    </div>
  );
}
