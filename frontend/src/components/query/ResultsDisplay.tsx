import { useState } from 'react';
import {
  Download,
  Code,
  Database,
  Clock,
  MapPin,
  FileJson,
  FileSpreadsheet,
  Search,
} from 'lucide-react';
import { QueryResult } from '@/types';
import { Card, Button, Badge } from '@/components/common';
import { formatNumber, formatExecutionTime, formatSQL } from '@/utils/format';
import { resultsApi } from '@/api';
import { CTASQueryInterface } from './CTASQueryInterface';
import toast from 'react-hot-toast';

interface ResultsDisplayProps {
  result: QueryResult;
  onViewOnMap?: () => void;
}

export function ResultsDisplay({ result, onViewOnMap }: ResultsDisplayProps) {
  const [isExporting, setIsExporting] = useState(false);
  const [showCTASQuery, setShowCTASQuery] = useState(false);

  // Extract database from CTAS table name
  const database = result.ctas_table_name?.split('.')[0] || 'unknown';

  // Check if result has geometry/WKT columns
  const hasGeometry = result.columns?.some(col =>
    col.toLowerCase().includes('wkt') ||
    col.toLowerCase().includes('geometry') ||
    col.toLowerCase().includes('geom')
  ) || false;

  const handleExport = async (format: 'csv' | 'json' | 'geojson') => {
    if (!result.ctas_table_name) return;

    setIsExporting(true);
    try {
      await resultsApi.export(result.ctas_table_name, database, format);
      toast.success(`Exported as ${format.toUpperCase()}`);
    } catch (error: any) {
      toast.error(error.message || 'Export failed');
    } finally {
      setIsExporting(false);
    }
  };

  return (
    <div className="space-y-4">
      {/* Summary Card */}
      <Card title="Query Results" subtitle="Execution completed successfully">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="p-3 bg-dark-sidebar rounded-lg">
            <div className="flex items-center gap-2 text-gray-400 mb-1">
              <Database className="w-4 h-4" />
              <span className="text-xs">Row Count</span>
            </div>
            <div className="text-2xl font-bold text-gray-100">
              {formatNumber(result.row_count)}
            </div>
          </div>

          <div className="p-3 bg-dark-sidebar rounded-lg">
            <div className="flex items-center gap-2 text-gray-400 mb-1">
              <Clock className="w-4 h-4" />
              <span className="text-xs">Execution Time</span>
            </div>
            <div className="text-2xl font-bold text-gray-100">
              {formatExecutionTime(result.execution_time_ms / 1000)}
            </div>
          </div>

          <div className="p-3 bg-dark-sidebar rounded-lg">
            <div className="flex items-center gap-2 text-gray-400 mb-1">
              <Database className="w-4 h-4" />
              <span className="text-xs">Result Table</span>
            </div>
            <div className="text-sm font-mono text-gray-100 truncate">
              {result.ctas_table_name}
            </div>
          </div>

          <div className="p-3 bg-dark-sidebar rounded-lg">
            <div className="flex items-center gap-2 text-gray-400 mb-1">
              <MapPin className="w-4 h-4" />
              <span className="text-xs">Geometry</span>
            </div>
            <div className="text-lg font-bold">
              <Badge variant={hasGeometry ? 'success' : 'default'}>
                {hasGeometry ? 'Yes' : 'No'}
              </Badge>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="flex flex-wrap gap-3 mt-4">
          <Button
            onClick={() => handleExport('csv')}
            variant="secondary"
            size="sm"
            disabled={isExporting}
          >
            <FileSpreadsheet className="w-4 h-4 mr-2" />
            Export CSV
          </Button>

          <Button
            onClick={() => handleExport('json')}
            variant="secondary"
            size="sm"
            disabled={isExporting}
          >
            <FileJson className="w-4 h-4 mr-2" />
            Export JSON
          </Button>

          {hasGeometry && (
            <>
              <Button
                onClick={() => handleExport('geojson')}
                variant="secondary"
                size="sm"
                disabled={isExporting}
              >
                <MapPin className="w-4 h-4 mr-2" />
                Export GeoJSON
              </Button>

              {onViewOnMap && (
                <Button onClick={onViewOnMap} variant="primary" size="sm">
                  <MapPin className="w-4 h-4 mr-2" />
                  View on Map
                </Button>
              )}
            </>
          )}

          {result.ctas_table_name && (
            <Button
              onClick={() => setShowCTASQuery(!showCTASQuery)}
              variant={showCTASQuery ? 'primary' : 'secondary'}
              size="sm"
            >
              <Search className="w-4 h-4 mr-2" />
              {showCTASQuery ? 'Hide' : 'Query'} CTAS Table
            </Button>
          )}
        </div>
      </Card>

      {/* SQL Display */}
      <Card
        title="Generated SQL"
        headerAction={
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              navigator.clipboard.writeText(result.sql);
              toast.success('SQL copied to clipboard');
            }}
          >
            <Code className="w-4 h-4 mr-2" />
            Copy
          </Button>
        }
      >
        <pre className="bg-dark-sidebar p-4 rounded-lg overflow-x-auto text-sm font-mono text-gray-300">
          {formatSQL(result.sql)}
        </pre>
      </Card>

      {/* Data Preview */}
      {result.preview_data && result.preview_data.length > 0 && (
        <Card title="Data Preview" subtitle={`First ${result.preview_data.length} rows`}>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-dark-border">
                  {result.columns?.map((col) => (
                    <th
                      key={col}
                      className="text-left p-3 font-medium text-gray-300"
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.preview_data.map((row, idx) => (
                  <tr
                    key={idx}
                    className="border-b border-dark-border hover:bg-dark-hover"
                  >
                    {result.columns?.map((col) => (
                      <td key={col} className="p-3 text-gray-400 font-mono text-xs">
                        {String(row[col] ?? 'NULL')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {/* CTAS Query Interface */}
      {showCTASQuery && result.ctas_table_name && (
        <CTASQueryInterface
          ctasTableName={result.ctas_table_name}
          database={database}
        />
      )}
    </div>
  );
}
