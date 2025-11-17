import { useState } from 'react';
import { Sparkles, Check, Plus, X, Edit2 } from 'lucide-react';
import { schemasApi } from '@/api';
import { Button, Card, Input } from '@/components/common';
import { EntityExtractionResult } from '@/types';
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
  const [newTableName, setNewTableName] = useState('');
  const [newColumnsByTable, setNewColumnsByTable] = useState<Record<string, string>>({});

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
    const newColumn = newColumnsByTable[tableName]?.trim();
    if (!newColumn) return;

    const updatedTables = {
      ...editableTables,
      [tableName]: [...(editableTables[tableName] || []), newColumn],
    };

    setEditableTables(updatedTables);
    setNewColumnsByTable((prev) => ({
      ...prev,
      [tableName]: '',
    }));

    // Update parent with new tables
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

    // Update parent
    onExtracted({
      ...extracted!,
      tables: updatedTables,
    });
  };

  const handleAddTable = () => {
    const tableName = newTableName.trim();
    if (!tableName || editableTables[tableName]) {
      toast.error('Invalid or duplicate table name');
      return;
    }

    const updatedTables = {
      ...editableTables,
      [tableName]: [],
    };

    setEditableTables(updatedTables);
    setNewTableName('');

    // Update parent
    onExtracted({
      ...extracted!,
      tables: updatedTables,
    });

    toast.success(`Table "${tableName}" added`);
  };

  const handleRemoveTable = (tableName: string) => {
    const { [tableName]: removed, ...rest } = editableTables;
    setEditableTables(rest);

    // Update parent
    onExtracted({
      ...extracted!,
      tables: rest,
    });

    toast.success(`Table "${tableName}" removed`);
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
            disabled={!nlQuery.trim()}
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
                      <X className="w-4 h-4" />
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
                    <Input
                      type="text"
                      placeholder="Add column name..."
                      value={newColumnsByTable[tableName] || ''}
                      onChange={(e) =>
                        setNewColumnsByTable((prev) => ({
                          ...prev,
                          [tableName]: e.target.value,
                        }))
                      }
                      onKeyPress={(e) => {
                        if (e.key === 'Enter') {
                          handleAddColumn(tableName);
                        }
                      }}
                      className="flex-1"
                    />
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => handleAddColumn(tableName)}
                      disabled={!newColumnsByTable[tableName]?.trim()}
                    >
                      <Plus className="w-4 h-4 mr-1" />
                      Add Column
                    </Button>
                  </div>
                </div>
              ))}
            </div>

            {/* Add New Table */}
            <div className="p-4 border border-dashed border-dark-border rounded-lg bg-dark-bg/50">
              <p className="text-sm text-gray-400 mb-3">Add additional tables if LLM missed any:</p>
              <div className="flex gap-2">
                <Input
                  type="text"
                  placeholder="New table name..."
                  value={newTableName}
                  onChange={(e) => setNewTableName(e.target.value)}
                  onKeyPress={(e) => {
                    if (e.key === 'Enter') {
                      handleAddTable();
                    }
                  }}
                  className="flex-1"
                />
                <Button
                  variant="secondary"
                  onClick={handleAddTable}
                  disabled={!newTableName.trim()}
                >
                  <Plus className="w-4 h-4 mr-2" />
                  Add Table
                </Button>
              </div>
            </div>

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
