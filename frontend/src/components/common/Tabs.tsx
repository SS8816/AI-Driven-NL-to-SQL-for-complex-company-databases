import { ReactNode } from 'react';
import { cn } from '@/utils';

export interface Tab {
  id: string;
  label: string;
  icon?: ReactNode;
  disabled?: boolean;
}

interface TabsProps {
  tabs: Tab[];
  activeTab: string;
  onChange: (tabId: string) => void;
  className?: string;
}

export function Tabs({ tabs, activeTab, onChange, className }: TabsProps) {
  return (
    <div className={cn('border-b border-dark-border', className)}>
      <nav className="flex space-x-1" aria-label="Tabs">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => !tab.disabled && onChange(tab.id)}
            disabled={tab.disabled}
            className={cn(
              'px-4 py-2.5 text-sm font-medium rounded-t-lg transition-colors',
              'focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 focus:ring-offset-dark-bg',
              activeTab === tab.id
                ? 'bg-dark-sidebar text-gray-100 border-b-2 border-blue-500'
                : 'text-gray-400 hover:text-gray-300 hover:bg-dark-hover',
              tab.disabled && 'opacity-50 cursor-not-allowed hover:bg-transparent hover:text-gray-400'
            )}
            aria-current={activeTab === tab.id ? 'page' : undefined}
          >
            <div className="flex items-center gap-2">
              {tab.icon}
              {tab.label}
            </div>
          </button>
        ))}
      </nav>
    </div>
  );
}

interface TabPanelProps {
  activeTab: string;
  tabId: string;
  children: ReactNode;
  className?: string;
}

export function TabPanel({ activeTab, tabId, children, className }: TabPanelProps) {
  if (activeTab !== tabId) return null;

  return (
    <div className={cn('py-4', className)} role="tabpanel">
      {children}
    </div>
  );
}
