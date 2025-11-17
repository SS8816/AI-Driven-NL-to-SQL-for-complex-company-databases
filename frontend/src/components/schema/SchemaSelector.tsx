import { useQuery } from '@tanstack/react-query';
import { Database, Loader2 } from 'lucide-react';
import { schemasApi } from '@/api';
import { Select } from '@/components/common';
import { useAppStore } from '@/stores/appStore';

export function SchemaSelector() {
  const { selectedSchema, setSelectedSchema } = useAppStore();

  const { data, isLoading, error } = useQuery({
    queryKey: ['schemas'],
    queryFn: () => schemasApi.list(),
  });

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 text-gray-400">
        <Loader2 className="w-4 h-4 animate-spin" />
        <span className="text-sm">Loading schemas...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-sm text-error">
        Failed to load schemas
      </div>
    );
  }

  const options = data?.schemas.map((schema) => ({
    value: schema.name,
    label: `${schema.name} (${schema.table_count} tables)`,
  })) || [];

  return (
    <div className="flex items-center gap-3">
      <Database className="w-5 h-5 text-gray-400 flex-shrink-0" />
      <Select
        value={selectedSchema || ''}
        onChange={(e) => setSelectedSchema(e.target.value || null)}
        options={options}
        placeholder="Select a schema..."
        className="flex-1"
      />
    </div>
  );
}
