import { useState } from 'react';
import { SchemaSelector } from '@/components/schema/SchemaSelector';
import { EntityExtraction } from '@/components/schema/EntityExtraction';
import { QueryProgress, ProgressStep } from '@/components/query/QueryProgress';
import { ResultsDisplay } from '@/components/query/ResultsDisplay';
import { MapView } from '@/components/map/MapView';
import { Card, EmptyState, Textarea, Input, Select, Button } from '@/components/common';
import { useAppStore } from '@/stores/appStore';
import { useAuthStore } from '@/stores/authStore';
import { QueryWebSocket } from '@/api';
import { ExecuteQueryRequest, QueryResult, EntityExtractionResult } from '@/types';
import { Database, Play, Zap } from 'lucide-react';
import toast from 'react-hot-toast';

export function QueryBuilderPage() {
  const { selectedSchema } = useAppStore();
  const { token } = useAuthStore();

  // Step 1: Query input
  const [nlQuery, setNlQuery] = useState('');
  const [ruleCategory, setRuleCategory] = useState('');

  // Step 2: Entity extraction
  const [extractedEntities, setExtractedEntities] =
    useState<EntityExtractionResult | null>(null);

  // Step 3: Execution
  const [cacheOption, setCacheOption] = useState<'use_cache' | 'use_sql' | 'regenerate'>('use_cache');
  const [isExecuting, setIsExecuting] = useState(false);
  const [progressSteps, setProgressSteps] = useState<ProgressStep[]>([]);
  const [currentStep, setCurrentStep] = useState<string | undefined>();

  // Step 4: Results
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [showMap, setShowMap] = useState(false);
  const [geojsonData, setGeojsonData] = useState<GeoJSON.FeatureCollection | null>(
    null
  );

  const handleEntityExtraction = (extracted: EntityExtractionResult) => {
    setExtractedEntities(extracted);
    // Reset previous results
    setQueryResult(null);
    setProgressSteps([]);
    setShowMap(false);
  };

  const handleExecuteQuery = async () => {
    if (!token || !extractedEntities || !nlQuery.trim() || !ruleCategory.trim()) {
      toast.error('Please complete all required fields');
      return;
    }

    const request: ExecuteQueryRequest = {
      rule_category: ruleCategory.toUpperCase(), // Make case-insensitive for caching
      nl_query: nlQuery,
      schema_name: selectedSchema!,
      selected_tables: extractedEntities.tables,
      execution_mode: 'sync', // Always sync for now
      // Cache handling will be done backend based on rule_category
    };

    setIsExecuting(true);
    setQueryResult(null);
    setProgressSteps([]);
    setCurrentStep(undefined);
    setShowMap(false);

    const ws = new QueryWebSocket(token);

    try {
      await ws.execute(
        request,
        // onProgress
        (stage, message, percent) => {
          const step: ProgressStep = {
            stage,
            message,
            progress_percent: percent,
            timestamp: new Date().toISOString(),
            status: 'running',
          };

          setCurrentStep(stage);
          setProgressSteps((prev) => {
            const existing = prev.findIndex((s) => s.stage === stage);
            if (existing >= 0) {
              const updated = [...prev];
              updated[existing] = step;
              return updated;
            }
            return [...prev, step];
          });
        },
        // onResult
        (result) => {
          setQueryResult(result);
          setIsExecuting(false);
          setCurrentStep(undefined);

          // Mark last step as completed
          setProgressSteps((prev) =>
            prev.map((s, idx) =>
              idx === prev.length - 1 ? { ...s, status: 'completed' } : s
            )
          );

          toast.success('Query executed successfully!');
        },
        // onError
        (error) => {
          setIsExecuting(false);
          setCurrentStep(undefined);

          // Mark last step as failed
          setProgressSteps((prev) =>
            prev.map((s, idx) =>
              idx === prev.length - 1
                ? { ...s, status: 'failed', message: error }
                : s
            )
          );

          toast.error(error);
        }
      );
    } catch (error: any) {
      console.error('Query execution failed:', error);
      toast.error(error.message || 'Query execution failed');
      setIsExecuting(false);
    }
  };

  const handleViewOnMap = async () => {
    if (!queryResult) return;

    try {
      // Fetch GeoJSON export
      const response = await fetch(
        `/api/v1/results/${queryResult.ctas_table_name}/export?database=${queryResult.database}&format=geojson`
      );
      const data = await response.json();
      setGeojsonData(data);
      setShowMap(true);
      toast.success('Loading map visualization...');
    } catch (error) {
      toast.error('Failed to load map data');
    }
  };

  if (!selectedSchema) {
    return (
      <div className="space-y-6">
        <Card title="Query Builder" subtitle="Build and execute natural language queries">
          <div className="mb-6">
            <SchemaSelector />
          </div>

          <EmptyState
            icon={Database}
            title="Select a schema to begin"
            description="Choose a schema from the dropdown above to start building your query"
          />
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Schema Selection */}
      <Card title="Schema Selection">
        <SchemaSelector />
      </Card>

      {/* Step 1: Query Input */}
      <Card
        title="Query Input"
        subtitle="Enter your natural language query and rule category"
      >
        <div className="space-y-4">
          <Textarea
            label="Natural Language Query"
            placeholder="e.g., Find all violations where speed exceeds 80 mph in school zones"
            rows={4}
            value={nlQuery}
            onChange={(e) => setNlQuery(e.target.value)}
            helperText="Describe what data you want to find using plain English"
          />

          <Input
            label="Rule Category"
            type="text"
            value={ruleCategory}
            onChange={(e) => setRuleCategory(e.target.value)}
            placeholder="e.g., WBL039, SPEED001, ZONE002"
            helperText="Rule category is used for caching (case-insensitive: WBL039 = wbl039)"
          />
        </div>
      </Card>

      {/* Step 2: Entity Extraction */}
      {nlQuery.trim() && ruleCategory.trim() && (
        <EntityExtraction
          schemaName={selectedSchema}
          nlQuery={nlQuery}
          onExtracted={handleEntityExtraction}
        />
      )}

      {/* Step 3: Query Execution */}
      {extractedEntities && (
        <Card title="Execute Query" subtitle="Choose cache strategy and execute">
          <div className="space-y-4">
            <Select
              label="Cache Strategy"
              value={cacheOption}
              onChange={(e) => setCacheOption(e.target.value as 'use_cache' | 'use_sql' | 'regenerate')}
              options={[
                {
                  value: 'use_cache',
                  label: '1. Use Cached Result (if available) - Fastest'
                },
                {
                  value: 'use_sql',
                  label: '2. Use Cached SQL & Re-run on Latest Data'
                },
                {
                  value: 'regenerate',
                  label: '3. Regenerate SQL from Scratch - Slowest'
                },
              ]}
              helperText={
                cacheOption === 'use_cache'
                  ? 'Uses existing CTAS table if cache hit for this rule category'
                  : cacheOption === 'use_sql'
                  ? 'Reuses cached SQL query but runs on latest data'
                  : 'Full regeneration: LLM generates new SQL, runs, and caches'
              }
            />

            <div className="flex gap-3">
              <Button
                onClick={handleExecuteQuery}
                variant="primary"
                disabled={isExecuting}
                isLoading={isExecuting}
                loadingText="Executing query..."
                className="flex-1"
              >
                <Play className="w-4 h-4 mr-2" />
                Execute Query
              </Button>

              <Button variant="secondary" disabled>
                <Zap className="w-4 h-4 mr-2" />
                {Object.keys(extractedEntities.tables).length} Tables Selected
              </Button>
            </div>
          </div>
        </Card>
      )}

      {/* Progress */}
      {progressSteps.length > 0 && (
        <QueryProgress steps={progressSteps} currentStep={currentStep} />
      )}

      {/* Results */}
      {queryResult && !showMap && (
        <ResultsDisplay result={queryResult} onViewOnMap={handleViewOnMap} />
      )}

      {/* Map */}
      {showMap && geojsonData && <MapView geojsonData={geojsonData} />}
    </div>
  );
}
