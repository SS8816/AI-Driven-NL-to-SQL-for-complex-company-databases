import { useState, useMemo } from 'react';
import { MapPin, X, Filter, Eye, EyeOff } from 'lucide-react';
import { Card, Button, Input } from '@/components/common';
import { MapView } from './MapView';
import { truncateCell } from '@/utils/format';

// Color palette for different geometry layers
const LAYER_COLORS = [
  '#FF3D00', // Red
  '#2196F3', // Blue
  '#4CAF50', // Green
  '#FF9800', // Orange
  '#9C27B0', // Purple
  '#00BCD4', // Cyan
];

interface ResultsMapViewProps {
  rows: Array<Record<string, any>>;
  columns: string[];
  onClose: () => void;
}

export interface GeojsonLayer {
  name: string;
  color: string;
  data: GeoJSON.FeatureCollection;
  visible: boolean;
}

/**
 * Convert WKT string to GeoJSON geometry
 */
function wktToGeojson(wkt: string): GeoJSON.Geometry | null {
  if (!wkt || typeof wkt !== 'string') return null;

  try {
    const wktUpper = wkt.trim().toUpperCase();

    // Parse POINT
    if (wktUpper.startsWith('POINT')) {
      const coords = wkt.match(/POINT\s*\(([^)]+)\)/i)?.[1];
      if (!coords) return null;
      const [lng, lat] = coords.trim().split(/\s+/).map(Number);
      return { type: 'Point', coordinates: [lng, lat] };
    }

    // Parse LINESTRING
    if (wktUpper.startsWith('LINESTRING')) {
      const coords = wkt.match(/LINESTRING\s*\(([^)]+)\)/i)?.[1];
      if (!coords) return null;
      const points = coords.split(',').map((p) => {
        const [lng, lat] = p.trim().split(/\s+/).map(Number);
        return [lng, lat];
      });
      return { type: 'LineString', coordinates: points };
    }

    // Parse POLYGON
    if (wktUpper.startsWith('POLYGON')) {
      const ringsMatch = wkt.match(/POLYGON\s*\(\(([^)]+)\)\)/i)?.[1];
      if (!ringsMatch) return null;
      const points = ringsMatch.split(',').map((p) => {
        const [lng, lat] = p.trim().split(/\s+/).map(Number);
        return [lng, lat];
      });
      return { type: 'Polygon', coordinates: [points] };
    }

    // Parse MULTIPOINT
    if (wktUpper.startsWith('MULTIPOINT')) {
      const coordsMatch = wkt.match(/MULTIPOINT\s*\(([^)]+)\)/i)?.[1];
      if (!coordsMatch) return null;
      const points = coordsMatch.split(',').map((p) => {
        const coords = p.trim().replace(/[()]/g, '');
        const [lng, lat] = coords.split(/\s+/).map(Number);
        return [lng, lat];
      });
      return { type: 'MultiPoint', coordinates: points };
    }

    // Parse MULTILINESTRING
    if (wktUpper.startsWith('MULTILINESTRING')) {
      const linesMatch = wkt.match(/MULTILINESTRING\s*\((.+)\)/i)?.[1];
      if (!linesMatch) return null;
      const lines = linesMatch.match(/\(([^)]+)\)/g)?.map((line) => {
        const coords = line.replace(/[()]/g, '');
        return coords.split(',').map((p) => {
          const [lng, lat] = p.trim().split(/\s+/).map(Number);
          return [lng, lat];
        });
      }) || [];
      return { type: 'MultiLineString', coordinates: lines };
    }

    // Parse MULTIPOLYGON
    if (wktUpper.startsWith('MULTIPOLYGON')) {
      const polygonsMatch = wkt.match(/MULTIPOLYGON\s*\((.+)\)/i)?.[1];
      if (!polygonsMatch) return null;

      // Extract individual polygons
      const polygons: number[][][][] = [];
      let depth = 0;
      let currentPoly = '';

      for (let i = 0; i < polygonsMatch.length; i++) {
        const char = polygonsMatch[i];
        if (char === '(') depth++;
        if (char === ')') depth--;
        currentPoly += char;

        if (depth === 1 && polygonsMatch[i + 1] === ',') {
          // End of a polygon
          const ringsMatch = currentPoly.match(/\(\(([^)]+)\)\)/);
          if (ringsMatch) {
            const points = ringsMatch[1].split(',').map((p) => {
              const [lng, lat] = p.trim().split(/\s+/).map(Number);
              return [lng, lat];
            });
            polygons.push([points]);
          }
          currentPoly = '';
          i++; // Skip the comma
        }
      }

      // Handle last polygon
      if (currentPoly) {
        const ringsMatch = currentPoly.match(/\(\(([^)]+)\)\)/);
        if (ringsMatch) {
          const points = ringsMatch[1].split(',').map((p) => {
            const [lng, lat] = p.trim().split(/\s+/).map(Number);
            return [lng, lat];
          });
          polygons.push([points]);
        }
      }

      return { type: 'MultiPolygon', coordinates: polygons };
    }

    return null;
  } catch (error) {
    console.error('Error parsing WKT:', error);
    return null;
  }
}

export function ResultsMapView({ rows, columns, onClose }: ResultsMapViewProps) {
  const [idFilter, setIdFilter] = useState('');

  // Find ALL WKT columns
  const wktColumns = useMemo(() => {
    return columns.filter(
      (col) =>
        col.toLowerCase().includes('wkt') ||
        col.toLowerCase().includes('geometry') ||
        col.toLowerCase() === 'geom'
    );
  }, [columns]);

  // State for layer visibility
  const [visibleLayers, setVisibleLayers] = useState<Set<string>>(() => {
    // Initially all layers are visible
    return new Set(wktColumns);
  });

  // Update visible layers when wktColumns change
  useMemo(() => {
    setVisibleLayers(new Set(wktColumns));
  }, [wktColumns]);

  // Find ID column
  const idColumn = columns.find(
    (col) => col.toLowerCase() === 'id' || col.toLowerCase().endsWith('_id')
  );

  // Convert rows to GeoJSON layers with filtering
  const geojsonLayers = useMemo((): GeojsonLayer[] => {
    if (wktColumns.length === 0) {
      return [];
    }

    // Filter rows by ID if specified
    const filteredRows = idFilter
      ? rows.filter((row) => {
          const idValue = String(row[idColumn || 'id'] || '').toLowerCase();
          return idValue.includes(idFilter.toLowerCase());
        })
      : rows;

    // Create a layer for each WKT column
    return wktColumns.map((wktColumn, layerIdx) => {
      const features: GeoJSON.Feature[] = [];

      filteredRows.forEach((row, idx) => {
        const wktValue = row[wktColumn];
        if (!wktValue) return;

        const geometry = wktToGeojson(String(wktValue));
        if (!geometry) return;

        // Build properties (exclude WKT columns to reduce size)
        const properties: Record<string, any> = {};
        columns.forEach((col) => {
          if (!wktColumns.includes(col)) {
            properties[col] = truncateCell(row[col], 200);
          }
        });

        features.push({
          type: 'Feature',
          id: idx,
          geometry,
          properties,
        });
      });

      return {
        name: wktColumn,
        color: LAYER_COLORS[layerIdx % LAYER_COLORS.length],
        data: {
          type: 'FeatureCollection',
          features,
        },
        visible: visibleLayers.has(wktColumn),
      };
    });
  }, [rows, columns, wktColumns, idColumn, idFilter, visibleLayers]);

  // Toggle layer visibility
  const toggleLayer = (layerName: string) => {
    setVisibleLayers((prev) => {
      const next = new Set(prev);
      if (next.has(layerName)) {
        next.delete(layerName);
      } else {
        next.add(layerName);
      }
      return next;
    });
  };

  // Calculate total features across all visible layers
  const totalFeatures = geojsonLayers
    .filter((layer) => layer.visible)
    .reduce((sum, layer) => sum + layer.data.features.length, 0);

  if (wktColumns.length === 0) {
    return (
      <Card title="Map Visualization">
        <div className="p-6 text-center">
          <MapPin className="w-12 h-12 text-gray-500 mx-auto mb-3" />
          <p className="text-gray-400">No geometry/WKT columns found in results</p>
          <p className="text-gray-500 text-sm mt-2">
            Map visualization requires a column with WKT geometry data
          </p>
          <Button onClick={onClose} variant="secondary" className="mt-4">
            Close
          </Button>
        </div>
      </Card>
    );
  }

  return (
    <Card
      title="Map Visualization"
      subtitle={`Showing ${totalFeatures} features across ${geojsonLayers.filter(l => l.visible).length} layers`}
      headerAction={
        <Button onClick={onClose} variant="ghost" size="sm">
          <X className="w-4 h-4" />
        </Button>
      }
    >
      <div className="space-y-4">
        {/* ID Filter */}
        {idColumn && (
          <div className="flex items-center gap-2">
            <Filter className="w-4 h-4 text-gray-400" />
            <Input
              type="text"
              placeholder={`Filter by ${idColumn}...`}
              value={idFilter}
              onChange={(e) => setIdFilter(e.target.value)}
              className="max-w-xs"
            />
            {idFilter && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setIdFilter('')}
              >
                Clear
              </Button>
            )}
          </div>
        )}

        {/* Layer Toggles */}
        {geojsonLayers.length > 1 && (
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm text-gray-400">Layers:</span>
            {geojsonLayers.map((layer) => (
              <Button
                key={layer.name}
                variant={layer.visible ? 'secondary' : 'ghost'}
                size="sm"
                onClick={() => toggleLayer(layer.name)}
                className="flex items-center gap-2"
              >
                {layer.visible ? (
                  <Eye className="w-3 h-3" />
                ) : (
                  <EyeOff className="w-3 h-3" />
                )}
                <div
                  className="w-3 h-3 rounded-full"
                  style={{ backgroundColor: layer.color }}
                />
                <span className="font-mono text-xs">{layer.name}</span>
                <span className="text-xs text-gray-500">
                  ({layer.data.features.length})
                </span>
              </Button>
            ))}
          </div>
        )}

        {/* Map Info */}
        <div className="flex items-center gap-4 text-sm text-gray-400 flex-wrap">
          <div>
            <span className="font-medium">Geometry Columns:</span>{' '}
            {geojsonLayers.map((layer, idx) => (
              <span key={layer.name}>
                <code className="bg-dark-sidebar px-2 py-1 rounded" style={{ borderLeft: `3px solid ${layer.color}` }}>
                  {layer.name}
                </code>
                {idx < geojsonLayers.length - 1 && ', '}
              </span>
            ))}
          </div>
          {idColumn && (
            <div>
              <span className="font-medium">ID Column:</span>{' '}
              <code className="bg-dark-sidebar px-2 py-1 rounded">{idColumn}</code>
            </div>
          )}
        </div>

        {/* Map */}
        {totalFeatures > 0 ? (
          <MapView geojsonLayers={geojsonLayers.filter((layer) => layer.visible)} />
        ) : (
          <div className="h-[400px] flex items-center justify-center bg-dark-sidebar rounded-lg">
            <div className="text-center">
              <MapPin className="w-12 h-12 text-gray-500 mx-auto mb-3" />
              <p className="text-gray-400">
                {idFilter ? 'No features match the filter' : 'No valid geometries found'}
              </p>
              {idFilter && (
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => setIdFilter('')}
                  className="mt-3"
                >
                  Clear Filter
                </Button>
              )}
            </div>
          </div>
        )}

        {/* Legend */}
        <div className="flex items-center gap-4 text-xs text-gray-500 flex-wrap">
          {geojsonLayers.filter(l => l.visible).map((layer) => (
            <div key={layer.name} className="flex items-center gap-2">
              <div className="flex items-center gap-1">
                <div
                  className="w-4 h-4 rounded-full border-2 border-white"
                  style={{ backgroundColor: layer.color }}
                />
                <div
                  className="w-4 h-4 border-2"
                  style={{ backgroundColor: `${layer.color}30`, borderColor: layer.color }}
                />
                <div className="w-8 h-0.5" style={{ backgroundColor: layer.color }} />
              </div>
              <span className="font-mono">{layer.name}</span>
            </div>
          ))}
        </div>
      </div>
    </Card>
  );
}
