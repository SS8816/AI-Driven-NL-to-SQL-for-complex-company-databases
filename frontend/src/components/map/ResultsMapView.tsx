import { useState, useMemo } from 'react';
import { MapPin, X, Filter, Layers, Eye, EyeOff } from 'lucide-react';
import { Card, Button, Input, Badge } from '@/components/common';
import { MapView } from './MapView';
import { truncateCell } from '@/utils/format';

interface ResultsMapViewProps {
  rows: Array<Record<string, any>>;
  columns: string[];
  onClose: () => void;
}

export interface GeojsonLayer {
  name: string;
  color: string;
  featureCount: number;
  geojson: GeoJSON.FeatureCollection;
  visible: boolean;
}

// Color palette for different geometry layers
const LAYER_COLORS = [
  '#FF3D00', // Red
  '#2196F3', // Blue
  '#4CAF50', // Green
  '#FF9800', // Orange
  '#9C27B0', // Purple
  '#00BCD4', // Cyan
];

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
      const lines =
        linesMatch.match(/\(([^)]+)\)/g)?.map((line) => {
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

      const polygons: number[][][][] = [];
      let depth = 0;
      let currentPoly = '';

      for (let i = 0; i < polygonsMatch.length; i++) {
        const char = polygonsMatch[i];
        if (char === '(') depth++;
        if (char === ')') depth--;
        currentPoly += char;

        if (depth === 1 && polygonsMatch[i + 1] === ',') {
          const ringsMatch = currentPoly.match(/\(\(([^)]+)\)\)/);
          if (ringsMatch) {
            const points = ringsMatch[1].split(',').map((p) => {
              const [lng, lat] = p.trim().split(/\s+/).map(Number);
              return [lng, lat];
            });
            polygons.push([points]);
          }
          currentPoly = '';
          i++;
        }
      }

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
  const [visibleLayers, setVisibleLayers] = useState<Set<string>>(new Set());

  // Find ALL WKT columns by checking if they actually contain valid WKT data
  const wktColumns = useMemo(() => {
    if (rows.length === 0) return [];

    return columns.filter((col) => {
      // First check: column name suggests it's a WKT column
      const nameMatch =
        col.toLowerCase().includes('wkt') ||
        col.toLowerCase().endsWith('_geometry') ||
        col.toLowerCase() === 'geometry' ||
        col.toLowerCase() === 'geom';

      if (!nameMatch) return false;

      // Second check: verify it actually contains WKT string data in the first row
      const firstValue = rows[0]?.[col];
      if (!firstValue || typeof firstValue !== 'string') return false;

      // Check if it looks like WKT (starts with geometry type keywords)
      const wktPattern =
        /^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(/i;
      return wktPattern.test(firstValue.trim());
    });
  }, [columns, rows]);

  // Initialize visible layers (all visible by default)
  useMemo(() => {
    if (wktColumns.length > 0 && visibleLayers.size === 0) {
      setVisibleLayers(new Set(wktColumns));
    }
  }, [wktColumns]);

  // Find ID column
  const idColumn = columns.find(
    (col) => col.toLowerCase() === 'id' || col.toLowerCase().endsWith('_id')
  );

  // Toggle layer visibility
  const toggleLayer = (layerName: string) => {
    setVisibleLayers((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(layerName)) {
        newSet.delete(layerName);
      } else {
        newSet.add(layerName);
      }
      return newSet;
    });
  };

  // Convert rows to multiple GeoJSON layers
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

    // Create a GeoJSON for each WKT column
    return wktColumns.map((wktColumn, idx) => {
      const features: GeoJSON.Feature[] = [];

      filteredRows.forEach((row, rowIdx) => {
        const wktValue = row[wktColumn];
        if (!wktValue) return;

        const geometry = wktToGeojson(String(wktValue));
        if (!geometry) return;

        // Build properties (exclude WKT columns to reduce size)
        const properties: Record<string, any> = {
          _layer: wktColumn, // Track which column this came from
        };
        columns.forEach((col) => {
          if (!wktColumns.includes(col)) {
            properties[col] = truncateCell(row[col], 200);
          }
        });

        features.push({
          type: 'Feature',
          id: `${wktColumn}-${rowIdx}`,
          geometry,
          properties,
        });
      });

      return {
        name: wktColumn,
        color: LAYER_COLORS[idx % LAYER_COLORS.length],
        featureCount: features.length,
        geojson: {
          type: 'FeatureCollection',
          features,
        },
        visible: visibleLayers.has(wktColumn),
      };
    });
  }, [rows, columns, wktColumns, idColumn, idFilter, visibleLayers]);

  // Combine all visible layers into one GeoJSON for bounds calculation
  const allVisibleFeatures = useMemo((): GeoJSON.FeatureCollection => {
    const features: GeoJSON.Feature[] = [];
    geojsonLayers.forEach((layer) => {
      if (layer.visible) {
        features.push(...layer.geojson.features);
      }
    });
    return {
      type: 'FeatureCollection',
      features,
    };
  }, [geojsonLayers]);

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

  const totalVisibleFeatures = geojsonLayers.reduce(
    (sum, layer) => sum + (layer.visible ? layer.featureCount : 0),
    0
  );

  return (
    <Card
      title="Map Visualization"
      subtitle={`${wktColumns.length} geometry layer${wktColumns.length > 1 ? 's' : ''} • ${totalVisibleFeatures} features visible`}
      headerAction={
        <Button onClick={onClose} variant="ghost" size="sm">
          <X className="w-4 h-4" />
        </Button>
      }
    >
      <div className="space-y-4">
        {/* Controls Row */}
        <div className="flex flex-wrap items-center gap-4">
          {/* ID Filter */}
          {idColumn && (
            <div className="flex items-center gap-2 flex-1 min-w-[200px]">
              <Filter className="w-4 h-4 text-gray-400" />
              <Input
                type="text"
                placeholder={`Filter by ${idColumn}...`}
                value={idFilter}
                onChange={(e) => setIdFilter(e.target.value)}
                className="flex-1"
              />
              {idFilter && (
                <Button variant="ghost" size="sm" onClick={() => setIdFilter('')}>
                  Clear
                </Button>
              )}
            </div>
          )}

          {/* Layer Toggle Buttons */}
          {wktColumns.length > 1 && (
            <div className="flex items-center gap-2">
              <Layers className="w-4 h-4 text-gray-400" />
              <span className="text-sm text-gray-500">Layers:</span>
              {geojsonLayers.map((layer) => (
                <Button
                  key={layer.name}
                  variant={layer.visible ? 'primary' : 'secondary'}
                  size="sm"
                  onClick={() => toggleLayer(layer.name)}
                  className="flex items-center gap-1"
                >
                  {layer.visible ? <Eye className="w-3 h-3" /> : <EyeOff className="w-3 h-3" />}
                  <div
                    className="w-3 h-3 rounded-full"
                    style={{ backgroundColor: layer.color }}
                  />
                  <span className="text-xs">{layer.name}</span>
                  <Badge variant="default" className="ml-1 text-xs">
                    {layer.featureCount}
                  </Badge>
                </Button>
              ))}
            </div>
          )}
        </div>

        {/* Layer Info */}
        {wktColumns.length === 1 && (
          <div className="flex items-center gap-4 text-sm text-gray-400">
            <div>
              <span className="font-medium">Geometry Column:</span>{' '}
              <code className="bg-dark-sidebar px-2 py-1 rounded">{wktColumns[0]}</code>
            </div>
            {idColumn && (
              <div>
                <span className="font-medium">ID Column:</span>{' '}
                <code className="bg-dark-sidebar px-2 py-1 rounded">{idColumn}</code>
              </div>
            )}
          </div>
        )}

        {/* Map */}
        {totalVisibleFeatures > 0 ? (
          <MapView
            geojsonLayers={geojsonLayers.filter((l) => l.visible && l.featureCount > 0)}
            allFeatures={allVisibleFeatures}
          />
        ) : (
          <div className="h-[400px] flex items-center justify-center bg-dark-sidebar rounded-lg">
            <div className="text-center">
              <MapPin className="w-12 h-12 text-gray-500 mx-auto mb-3" />
              <p className="text-gray-400">
                {idFilter
                  ? 'No features match the filter'
                  : 'No layers visible - toggle layers above to view'}
              </p>
              {idFilter && (
                <Button variant="secondary" size="sm" onClick={() => setIdFilter('')} className="mt-3">
                  Clear Filter
                </Button>
              )}
            </div>
          </div>
        )}

        {/* Legend */}
        <div className="flex flex-wrap items-center gap-4 text-xs text-gray-500">
          <div className="font-medium">Legend:</div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded-full bg-red-500 border-2 border-white"></div>
            <span>Points</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-8 h-0.5 bg-red-500"></div>
            <span>Lines</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 bg-red-500/30 border-2 border-red-500"></div>
            <span>Polygons</span>
          </div>
          {wktColumns.length > 1 && (
            <>
              <div className="h-4 w-px bg-gray-600 mx-2"></div>
              {geojsonLayers.map((layer) => (
                <div key={layer.name} className="flex items-center gap-2">
                  <div className="w-4 h-4 rounded" style={{ backgroundColor: layer.color }} />
                  <span>{layer.name}</span>
                </div>
              ))}
            </>
          )}
        </div>
      </div>
    </Card>
  );
}
