import { useState } from 'react';
import { Sparkles, Check } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { schemasApi } from '@/api';
import { Button, Card } from '@/components/common';
import { EntityExtractionResult } from '@/types';

interface EntityExtractionProps {
  schemaName: string;
  nlQuery: string;
  onExtracted: (extracted: EntityExtractionResult) => void;
}

export function EntityExtraction({
  schemaName,
  nlQuery,
  onExtracted,
}: EntityExtractionProps) {
  const [isExtracting, setIsExtracting] = useState(false);
  const [extracted, setExtracted] = useState<EntityExtractionResult | null>(null);

  const handleExtract = async () => {
    if (!nlQuery.trim()) return;

    setIsExtracting(true);
    try {
      const result = await schemasApi.analyze(schemaName, nlQuery);
      setExtracted(result);
      onExtracted(result);
    } catch (error) {
      console.error('Entity extraction failed:', error);
    } finally {
      setIsExtracting(false);
    }
  };

  return (
    <Card title="Entity Extraction" subtitle="AI-powered table and column detection">
      <div className="space-y-4">
        <Button
          onClick={handleExtract}
          variant="secondary"
          isLoading={isExtracting}
          loadingText="Analyzing query..."
          disabled={!nlQuery.trim()}
          className="w-full"
        >
          <Sparkles className="w-4 h-4 mr-2" />
          Extract Tables & Columns
        </Button>

        {extracted && (
          <div className="space-y-3">
            <div className="flex items-start gap-2 text-sm text-success">
              <Check className="w-4 h-4 mt-0.5 flex-shrink-0" />
              <span>
                Identified {Object.keys(extracted.tables).length} relevant table(s)
              </span>
            </div>

            {extracted.reasoning && (
              <div className="p-3 bg-dark-sidebar rounded-lg text-sm text-gray-400">
                <span className="font-medium text-gray-300">Analysis: </span>
                {extracted.reasoning}
              </div>
            )}

            <div className="space-y-2">
              {Object.entries(extracted.tables).map(([tableName, columns]) => (
                <div
                  key={tableName}
                  className="p-3 bg-dark-sidebar rounded-lg border border-dark-border"
                >
                  <div className="font-medium text-gray-100 mb-2">
                    {tableName}
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {columns.map((col) => (
                      <span
                        key={col}
                        className="px-2 py-1 bg-primary-500/20 text-primary-400 rounded text-xs font-mono"
                      >
                        {col}
                      </span>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}
