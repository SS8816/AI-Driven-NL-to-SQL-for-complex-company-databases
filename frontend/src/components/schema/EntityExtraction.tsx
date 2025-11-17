import { useState, useEffect } from 'react';
import { Sparkles, Check, X, Edit2 } from 'lucide-react';
import { schemasApi } from '@/api';
import { Button, Card, Select } from '@/components/common';
import { EntityExtractionResult, SchemaInfo } from '@/types';
import toast from 'react-hot-toast';

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
  const [editableTables, setEditableTables] = useState<Record<string, string[]>>({});

  // Schema details for populating dropdowns
  const [schemaInfo, setSchemaInfo] = useState<SchemaInfo | null>(null);
  const [loadingSchema, setLoadingSchema] = useState(false);

  // Redacted DDL for selected tables/columns
  const [redactedDDL, setRedactedDDL] = useState<string | null>(null);
  const [loadingDDL, setLoadingDDL] = useState(false);

  // Selected values for adding new table/column
  const [selectedNewTable, setSelectedNewTable] = useState('');
  const [selectedNewColumnByTable, setSelectedNewColumnByTable] = useState<Record<string, string>>({});

  // Fetch full schema details when schema changes
  useEffect(() => {
    const fetchSchemaDetails = async () => {
      setLoadingSchema(true);
      try {
        const details = await schemasApi.getDetail(schemaName);
        setSchemaInfo(details);
      } catch (error) {
        console.error('Failed to load schema details:', error);
      } finally {
        setLoadingSchema(false);
      }
    };

    fetchSchemaDetails();
  }, [schemaName]);

  // Fetch redacted DDL whenever editable tables change
  useEffect(() => {
    const fetchRedactedDDL = async () => {
      if (Object.keys(editableTables).length === 0) {
        setRedactedDDL(null);
        return;
      }

      setLoadingDDL(true);
      try {
        const result = await schemasApi.getRedactedDDL(schemaName, editableTables);
        setRedactedDDL(result.ddl);
      } catch (error) {
        console.error('Failed to load redacted DDL:', error);
      } finally {
        setLoadingDDL(false);
      }
    };

    fetchRedactedDDL();
  }, [schemaName, editableTables]);

  const handleExtract = async () => {
    if (!nlQuery.trim()) return;

    setIsExtracting(true);
    try {
      const result = await schemasApi.analyze(schemaName, nlQuery);
      setExtracted(result);
      setEditableTables(result.tables);
      onExtracted(result);
    } catch (error: any) {
      toast.error(error.message || 'Entity extraction failed');
      console.error('Entity extraction failed:', error);
    } finally {
      setIsExtracting(false);
    }
  };

  const handleAddColumn = (tableName: string) => {
    const newColumn = selectedNewColumnByTable[tableName];
    if (!newColumn) return;

    const updatedTables = {
      ...editableTables,
      [tableName]: [...(editableTables[tableName] || []), newColumn],
    };

    setEditableTables(updatedTables);
    setSelectedNewColumnByTable((prev) => ({
      ...prev,
      [tableName]: '',
    }));

    onExtracted({
      ...extracted!,
      tables: updatedTables,
    });
  };

  const handleRemoveColumn = (tableName: string, columnName: string) => {
    const updatedColumns = editableTables[tableName].filter((col) => col !== columnName);
    const updatedTables = {
      ...editableTables,
      [tableName]: updatedColumns,
    };

    setEditableTables(updatedTables);
    onExtracted({
      ...extracted!,
      tables: updatedTables,
    });
  };

  const handleAddTable = () => {
    const tableName = selectedNewTable;
    if (!tableName || editableTables[tableName]) {
      toast.error('Invalid or duplicate table name');
      return;
    }

    const updatedTables = {
      ...editableTables,
      [tableName]: [],
    };

    setEditableTables(updatedTables);
    setSelectedNewTable('');

    onExtracted({
      ...extracted!,
      tables: updatedTables,
    });

    toast.success(`Table "${tableName}" added`);
  };

  const handleRemoveTable = (tableName: string) => {
    const { [tableName]: removed, ...rest } = editableTables;
    setEditableTables(rest);

    onExtracted({
      ...extracted!,
      tables: rest,
    });

    toast.success(`Table "${tableName}" removed`);
  };

  // Get available tables (not yet selected)
  const availableTables = schemaInfo?.tables
    .filter((t) => !editableTables[t.table_name])
    .map((t) => ({
      value: t.table_name,
      label: `${t.table_name} (${t.column_count} columns)`,
    })) || [];

  // Get available columns for a specific table (not yet selected)
  const getAvailableColumns = (tableName: string) => {
    const schemaTable = schemaInfo?.tables.find((t) => t.table_name === tableName);
    if (!schemaTable) return [];

    const selectedColumns = editableTables[tableName] || [];
    return schemaTable.columns
      .filter((col) => !selectedColumns.includes(col.column_name))
      .map((col) => ({
        value: col.column_name,
        label: `${col.column_name} (${col.full_type})`,
      }));
  };

  return (
    <Card title="Entity Extraction" subtitle="AI-powered table and column detection - Edit as needed">
      <div className="space-y-4">
        {!extracted ? (
          <Button
            onClick={handleExtract}
            variant="secondary"
            isLoading={isExtracting}
            loadingText="Analyzing query with GPT..."
            disabled={!nlQuery.trim() || loadingSchema}
            className="w-full"
          >
            <Sparkles className="w-4 h-4 mr-2" />
            Extract Tables & Columns
          </Button>
        ) : (
          <>
            <div className="flex items-start gap-2 text-sm text-success">
              <Check className="w-4 h-4 mt-0.5 flex-shrink-0" />
              <span>
                {Object.keys(editableTables).length} table(s) selected (editable)
              </span>
            </div>

            {extracted.reasoning && (
              <div className="p-3 bg-dark-sidebar rounded-lg text-sm text-gray-400">
                <span className="font-medium text-gray-300">LLM Analysis: </span>
                {extracted.reasoning}
              </div>
            )}

            {/* Extracted Tables with Edit Options */}
            <div className="space-y-3">
              {Object.entries(editableTables).map(([tableName, columns]) => (
                <div
                  key={tableName}
                  className="p-4 bg-dark-sidebar rounded-lg border border-dark-border"
                >
                  <div className="flex items-center justify-between mb-3">
                    <div className="font-medium text-gray-100 flex items-center gap-2">
                      <Edit2 className="w-4 h-4 text-primary-400" />
                      {tableName}
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => handleRemoveTable(tableName)}
                      className="text-error hover:text-red-400"
                    >
                      <X className="w-4 h-4 mr-1" />
                      Remove Table
                    </Button>
                  </div>

                  {/* Columns */}
                  {columns.length > 0 && (
                    <div className="flex flex-wrap gap-2 mb-3">
                      {columns.map((col) => (
                        <div
                          key={col}
                          className="flex items-center gap-2 px-3 py-1 bg-primary-500/20 text-primary-400 rounded text-xs font-mono"
                        >
                          <span>{col}</span>
                          <button
                            onClick={() => handleRemoveColumn(tableName, col)}
                            className="hover:text-error transition-colors"
                          >
                            <X className="w-3 h-3" />
                          </button>
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Add Column */}
                  <div className="flex gap-2">
                    <Select
                      value={selectedNewColumnByTable[tableName] || ''}
                      onChange={(e) =>
                        setSelectedNewColumnByTable((prev) => ({
                          ...prev,
                          [tableName]: e.target.value,
                        }))
                      }
                      options={getAvailableColumns(tableName)}
                      placeholder="Select column to add..."
                      className="flex-1"
                    />
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => handleAddColumn(tableName)}
                      disabled={!selectedNewColumnByTable[tableName]}
                    >
                      Add
                    </Button>
                  </div>
                </div>
              ))}
            </div>

            {/* Add New Table */}
            <div className="p-4 border border-dashed border-dark-border rounded-lg bg-dark-bg/50">
              <p className="text-sm text-gray-400 mb-3">Add additional tables if LLM missed any:</p>
              <div className="flex gap-2">
                <Select
                  value={selectedNewTable}
                  onChange={(e) => setSelectedNewTable(e.target.value)}
                  options={availableTables}
                  placeholder="Select table to add..."
                  className="flex-1"
                />
                <Button
                  variant="secondary"
                  onClick={handleAddTable}
                  disabled={!selectedNewTable}
                >
                  Add Table
                </Button>
              </div>
            </div>

            {/* Redacted DDL Display */}
            {redactedDDL && (
              <div className="p-4 bg-dark-sidebar rounded-lg border border-dark-border">
                <div className="flex items-center justify-between mb-3">
                  <h4 className="text-sm font-medium text-gray-300">Redacted DDL for Selected Entities</h4>
                  {loadingDDL && <span className="text-xs text-gray-500">Updating...</span>}
                </div>
                <pre className="text-xs font-mono text-gray-400 overflow-x-auto max-h-64 overflow-y-auto whitespace-pre-wrap">
                  {redactedDDL}
                </pre>
              </div>
            )}

            {/* Re-extract button */}
            <Button
              onClick={handleExtract}
              variant="ghost"
              size="sm"
              isLoading={isExtracting}
              className="w-full"
            >
              <Sparkles className="w-4 h-4 mr-2" />
              Re-run Entity Extraction
            </Button>
          </>
        )}
      </div>
    </Card>
  );
}
