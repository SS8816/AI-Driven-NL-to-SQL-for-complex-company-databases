import { useState, useEffect } from 'react';
import {
  Filter,
  Code,
  Database,
  Clock,
  AlertTriangle,
  CheckCircle,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';
import {
  Card,
  Button,
  Tabs,
  TabPanel,
  Select,
  Textarea,
  Badge,
  Loading,
} from '@/components/common';
import { resultsApi } from '@/api';
import {
  CTASSchemaResponse,
  CTASQueryResponse,
  CTASCountriesResponse,
} from '@/types';
import { formatExecutionTime, truncateCell } from '@/utils/format';
import toast from 'react-hot-toast';

interface CTASQueryInterfaceProps {
  ctasTableName: string;
  database: string;
}

export function CTASQueryInterface({ ctasTableName, database }: CTASQueryInterfaceProps) {
  const [activeTab, setActiveTab] = useState('country');
  const [isLoadingSchema, setIsLoadingSchema] = useState(true);
  const [isLoadingCountries, setIsLoadingCountries] = useState(false);
  const [isExecuting, setIsExecuting] = useState(false);
  const [showSchema, setShowSchema] = useState(false);

  // Data state
  const [schema, setSchema] = useState<CTASSchemaResponse | null>(null);
  const [countries, setCountries] = useState<string[]>([]);
  const [queryResult, setQueryResult] = useState<CTASQueryResponse | null>(null);

  // Form state
  const [selectedCountry, setSelectedCountry] = useState<string>('');
  const [customSQL, setCustomSQL] = useState('SELECT * FROM {table} LIMIT 100');
  const [limit, setLimit] = useState(1000);

  // Load schema on mount
  useEffect(() => {
    loadSchema();
  }, [ctasTableName, database]);

  // Load countries when country tab is activated
  useEffect(() => {
    if (activeTab === 'country' && schema?.has_country_column && countries.length === 0) {
      loadCountries();
    }
  }, [activeTab, schema]);

  const loadSchema = async () => {
    try {
      setIsLoadingSchema(true);
      const schemaData = await resultsApi.getSchema(ctasTableName, database);
      setSchema(schemaData);
    } catch (error: any) {
      toast.error(error.message || 'Failed to load table schema');
    } finally {
      setIsLoadingSchema(false);
    }
  };

  const loadCountries = async () => {
    try {
      setIsLoadingCountries(true);
      const countriesData = await resultsApi.getCountries(ctasTableName, database);
      setCountries(countriesData.countries);
    } catch (error: any) {
      toast.error(error.message || 'Failed to load countries');
    } finally {
      setIsLoadingCountries(false);
    }
  };

  const handleCountryQuery = async () => {
    if (!selectedCountry) {
      toast.error('Please select a country');
      return;
    }

    try {
      setIsExecuting(true);
      setQueryResult(null);

      const sql = `SELECT * FROM {table} WHERE iso_country_code = '${selectedCountry}' LIMIT ${limit}`;

      const result = await resultsApi.executeQuery(ctasTableName, database, {
        custom_sql: sql,
        limit,
      });

      setQueryResult(result);

      if (result.success) {
        toast.success(`Query returned ${result.row_count} rows`);
      } else {
        toast.error(result.error || 'Query failed');
      }
    } catch (error: any) {
      toast.error(error.message || 'Query execution failed');
    } finally {
      setIsExecuting(false);
    }
  };

  const handleCustomQuery = async () => {
    if (!customSQL.trim()) {
      toast.error('Please enter a SQL query');
      return;
    }

    try {
      setIsExecuting(true);
      setQueryResult(null);

      const result = await resultsApi.executeQuery(ctasTableName, database, {
        custom_sql: customSQL,
        limit,
      });

      setQueryResult(result);

      if (result.success) {
        toast.success(`Query returned ${result.row_count} rows`);
      } else {
        toast.error(result.error || 'Query failed');
      }
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail?.error || error.message || 'Query execution failed';
      toast.error(errorMsg);
    } finally {
      setIsExecuting(false);
    }
  };

  const tabs = [
    {
      id: 'country',
      label: 'Filter by Country',
      icon: <Filter className="w-4 h-4" />,
      disabled: !schema?.has_country_column,
    },
    {
      id: 'custom',
      label: 'Custom SQL',
      icon: <Code className="w-4 h-4" />,
    },
  ];

  if (isLoadingSchema) {
    return (
      <Card title="Query CTAS Table">
        <div className="flex items-center justify-center py-12">
          <Loading />
        </div>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      {/* Schema Viewer (Collapsible) */}
      <Card
        title="Table Schema"
        subtitle={`${schema?.columns.length || 0} columns`}
        headerAction={
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setShowSchema(!showSchema)}
          >
            {showSchema ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
          </Button>
        }
      >
        {showSchema && schema && (
          <div className="space-y-2">
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2">
              {schema.columns.map((col) => (
                <div
                  key={col.name}
                  className="p-2 bg-dark-sidebar rounded border border-dark-border"
                >
                  <div className="font-mono text-sm text-gray-200">{col.name}</div>
                  <div className="text-xs text-gray-500">{col.type}</div>
                </div>
              ))}
            </div>

            {schema.has_country_column && (
              <div className="mt-4 p-3 bg-blue-500/10 border border-blue-500/30 rounded-lg">
                <div className="flex items-center gap-2 text-blue-400 text-sm">
                  <CheckCircle className="w-4 h-4" />
                  <span>This table has an <code className="font-mono">iso_country_code</code> column for country filtering</span>
                </div>
              </div>
            )}
          </div>
        )}
      </Card>

      {/* Query Interface */}
      <Card title="Query Interface">
        <Tabs tabs={tabs} activeTab={activeTab} onChange={setActiveTab} />

        {/* Tab 1: Country Filter */}
        <TabPanel activeTab={activeTab} tabId="country">
          {!schema?.has_country_column ? (
            <div className="p-6 text-center">
              <AlertTriangle className="w-12 h-12 text-yellow-500 mx-auto mb-3" />
              <p className="text-gray-400">
                This table does not have an <code className="font-mono">iso_country_code</code> column.
              </p>
              <p className="text-gray-500 text-sm mt-2">
                Use the "Custom SQL" tab to query this table.
              </p>
            </div>
          ) : (
            <div className="space-y-4">
              {isLoadingCountries ? (
                <div className="flex items-center justify-center py-8">
                  <Loading />
                </div>
              ) : (
                <>
                  <div>
                    <label className="block text-sm font-medium text-gray-300 mb-2">
                      Select Country
                    </label>
                    <Select
                      value={selectedCountry}
                      onChange={(e) => setSelectedCountry(e.target.value)}
                      disabled={isExecuting}
                    >
                      <option value="">-- Select a country --</option>
                      {countries.map((code) => (
                        <option key={code} value={code}>
                          {code}
                        </option>
                      ))}
                    </Select>
                    <p className="mt-2 text-sm text-gray-500">
                      {countries.length} countries available in this table
                    </p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-300 mb-2">
                      Result Limit
                    </label>
                    <Select
                      value={limit.toString()}
                      onChange={(e) => setLimit(parseInt(e.target.value))}
                      disabled={isExecuting}
                    >
                      <option value="100">100 rows</option>
                      <option value="500">500 rows</option>
                      <option value="1000">1,000 rows</option>
                      <option value="5000">5,000 rows</option>
                      <option value="10000">10,000 rows</option>
                    </Select>
                  </div>

                  <Button
                    onClick={handleCountryQuery}
                    disabled={isExecuting || !selectedCountry}
                    className="w-full"
                  >
                    {isExecuting ? 'Executing...' : 'Run Query'}
                  </Button>
                </>
              )}
            </div>
          )}
        </TabPanel>

        {/* Tab 2: Custom SQL */}
        <TabPanel activeTab={activeTab} tabId="custom">
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">
                Custom SQL Query
              </label>
              <Textarea
                value={customSQL}
                onChange={(e) => setCustomSQL(e.target.value)}
                rows={8}
                disabled={isExecuting}
                className="font-mono text-sm"
                placeholder={`SELECT * FROM {table} WHERE ...`}
              />
              <p className="mt-2 text-sm text-gray-500">
                Use <code className="font-mono bg-dark-sidebar px-1 py-0.5 rounded">{'{ table }'}</code> as a placeholder for the table name. Only SELECT statements are allowed.
              </p>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">
                Result Limit
              </label>
              <Select
                value={limit.toString()}
                onChange={(e) => setLimit(parseInt(e.target.value))}
                disabled={isExecuting}
              >
                <option value="100">100 rows</option>
                <option value="500">500 rows</option>
                <option value="1000">1,000 rows</option>
                <option value="5000">5,000 rows</option>
                <option value="10000">10,000 rows</option>
              </Select>
            </div>

            <Button
              onClick={handleCustomQuery}
              disabled={isExecuting || !customSQL.trim()}
              className="w-full"
            >
              {isExecuting ? 'Executing...' : 'Run Query'}
            </Button>

            <div className="p-3 bg-yellow-500/10 border border-yellow-500/30 rounded-lg">
              <div className="flex items-start gap-2 text-yellow-400 text-sm">
                <AlertTriangle className="w-4 h-4 mt-0.5 flex-shrink-0" />
                <div>
                  <div className="font-medium mb-1">SQL Validation</div>
                  <ul className="list-disc list-inside text-xs space-y-1 text-yellow-400/80">
                    <li>Only SELECT statements are allowed</li>
                    <li>Dangerous keywords (DROP, DELETE, etc.) are blocked</li>
                    <li>Maximum query length: 10,000 characters</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        </TabPanel>
      </Card>

      {/* Query Results */}
      {queryResult && (
        <Card title="Query Results">
          {queryResult.success ? (
            <div className="space-y-4">
              {/* Stats */}
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 bg-dark-sidebar rounded-lg">
                  <div className="flex items-center gap-2 text-gray-400 mb-1">
                    <Database className="w-4 h-4" />
                    <span className="text-xs">Rows Returned</span>
                  </div>
                  <div className="text-2xl font-bold text-gray-100">
                    {queryResult.row_count.toLocaleString()}
                  </div>
                </div>

                <div className="p-3 bg-dark-sidebar rounded-lg">
                  <div className="flex items-center gap-2 text-gray-400 mb-1">
                    <Clock className="w-4 h-4" />
                    <span className="text-xs">Execution Time</span>
                  </div>
                  <div className="text-2xl font-bold text-gray-100">
                    {formatExecutionTime(queryResult.execution_time_ms / 1000)}
                  </div>
                </div>
              </div>

              {/* Data Table */}
              {queryResult.rows && queryResult.rows.length > 0 && (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-dark-border">
                        {queryResult.columns?.map((col) => (
                          <th
                            key={col}
                            className="text-left p-3 font-medium text-gray-300 whitespace-nowrap"
                          >
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {queryResult.rows.map((row, idx) => (
                        <tr
                          key={idx}
                          className="border-b border-dark-border hover:bg-dark-hover"
                        >
                          {queryResult.columns?.map((col) => (
                            <td key={col} className="p-3 text-gray-400 font-mono text-xs max-w-xs truncate" title={String(row[col] ?? 'NULL')}>
                              {truncateCell(row[col], 100)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ) : (
            <div className="p-6 text-center">
              <AlertTriangle className="w-12 h-12 text-red-500 mx-auto mb-3" />
              <p className="text-gray-300 font-medium">Query Failed</p>
              <p className="text-gray-500 text-sm mt-2">{queryResult.error}</p>
            </div>
          )}
        </Card>
      )}
    </div>
  );
}
