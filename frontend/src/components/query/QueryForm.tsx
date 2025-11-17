import { useState } from 'react';
import { Play, Zap } from 'lucide-react';
import { Button, Textarea, Select, Card } from '@/components/common';
import { EntityExtractionResult, ExecuteQueryRequest } from '@/types';
import { config } from '@/config';

interface QueryFormProps {
  schemaName: string;
  onExecute: (request: ExecuteQueryRequest) => void;
  isExecuting: boolean;
  extractedEntities: EntityExtractionResult | null;
}

export function QueryForm({
  schemaName,
  onExecute,
  isExecuting,
  extractedEntities,
}: QueryFormProps) {
  const [nlQuery, setNlQuery] = useState('');
  const [ruleCategory, setRuleCategory] = useState('geospatial_violations');
  const [executionMode, setExecutionMode] = useState<'sync' | 'async'>('sync');

  const handleExecute = () => {
    if (!nlQuery.trim() || !extractedEntities) return;

    const request: ExecuteQueryRequest = {
      rule_category: ruleCategory,
      nl_query: nlQuery,
      schema_name: schemaName,
      selected_tables: extractedEntities.tables,
      execution_mode: executionMode,
    };

    onExecute(request);
  };

  const canExecute = nlQuery.trim() && extractedEntities && !isExecuting;

  return (
    <Card title="Query Builder" subtitle="Describe your query in natural language">
      <div className="space-y-4">
        <Textarea
          label="Natural Language Query"
          placeholder="e.g., Find all violations where speed exceeds 80 mph in school zones"
          rows={4}
          value={nlQuery}
          onChange={(e) => setNlQuery(e.target.value)}
          helperText="Describe what data you want to find using plain English"
        />

        <div className="grid grid-cols-2 gap-4">
          <Select
            label="Rule Category"
            value={ruleCategory}
            onChange={(e) => setRuleCategory(e.target.value)}
            options={[
              { value: 'geospatial_violations', label: 'Geospatial Violations' },
              { value: 'speed_violations', label: 'Speed Violations' },
              { value: 'zone_violations', label: 'Zone Violations' },
              { value: 'custom', label: 'Custom Query' },
            ]}
          />

          <Select
            label="Execution Mode"
            value={executionMode}
            onChange={(e) => setExecutionMode(e.target.value as 'sync' | 'async')}
            options={[
              { value: 'sync', label: 'Synchronous' },
              { value: 'async', label: 'Asynchronous' },
            ]}
            helperText={
              executionMode === 'sync'
                ? 'Wait for results'
                : 'Run in background'
            }
          />
        </div>

        <div className="flex gap-3">
          <Button
            onClick={handleExecute}
            variant="primary"
            disabled={!canExecute}
            isLoading={isExecuting}
            loadingText="Executing query..."
            className="flex-1"
          >
            <Play className="w-4 h-4 mr-2" />
            Execute Query
          </Button>

          {extractedEntities && (
            <Button variant="secondary" disabled>
              <Zap className="w-4 h-4 mr-2" />
              {Object.keys(extractedEntities.tables).length} Tables Selected
            </Button>
          )}
        </div>
      </div>
    </Card>
  );
}
