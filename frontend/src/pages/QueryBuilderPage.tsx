import { useState } from 'react';
import { SchemaSelector } from '@/components/schema/SchemaSelector';
import { EntityExtraction } from '@/components/schema/EntityExtraction';
import { QueryForm } from '@/components/query/QueryForm';
import { QueryProgress, ProgressStep } from '@/components/query/QueryProgress';
import { ResultsDisplay } from '@/components/query/ResultsDisplay';
import { MapView } from '@/components/map/MapView';
import { Card, EmptyState } from '@/components/common';
import { useAppStore } from '@/stores/appStore';
import { useAuthStore } from '@/stores/authStore';
import { QueryWebSocket } from '@/api';
import { ExecuteQueryRequest, QueryResult, EntityExtractionResult } from '@/types';
import { Database } from 'lucide-react';
import toast from 'react-hot-toast';

export function QueryBuilderPage() {
  const { selectedSchema } = useAppStore();
  const { token } = useAuthStore();
  const [nlQuery, setNlQuery] = useState('');
  const [extractedEntities, setExtractedEntities] =
    useState<EntityExtractionResult | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [progressSteps, setProgressSteps] = useState<ProgressStep[]>([]);
  const [currentStep, setCurrentStep] = useState<string | undefined>();
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

  const handleExecuteQuery = async (request: ExecuteQueryRequest) => {
    if (!token) {
      toast.error('Authentication required');
      return;
    }

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

      {/* Entity Extraction */}
      <EntityExtraction
        schemaName={selectedSchema}
        nlQuery={nlQuery}
        onExtracted={handleEntityExtraction}
      />

      {/* Query Form */}
      {extractedEntities && (
        <QueryForm
          schemaName={selectedSchema}
          onExecute={handleExecuteQuery}
          isExecuting={isExecuting}
          extractedEntities={extractedEntities}
        />
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
