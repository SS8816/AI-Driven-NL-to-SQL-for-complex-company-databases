import { useState, useMemo } from 'react';
import { MapPin, X, Filter } from 'lucide-react';
import { Card, Button, Input } from '@/components/common';
import { MapView } from './MapView';
import { truncateCell } from '@/utils/format';

interface ResultsMapViewProps {
  rows: Array<Record<string, any>>;
  columns: string[];
  onClose: () => void;
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

  // Find WKT column
  const wktColumn = columns.find(
    (col) =>
      col.toLowerCase().includes('wkt') ||
      col.toLowerCase().includes('geometry') ||
      col.toLowerCase() === 'geom'
  );

  // Find ID column
  const idColumn = columns.find(
    (col) => col.toLowerCase() === 'id' || col.toLowerCase().endsWith('_id')
  );

  // Convert rows to GeoJSON with filtering
  const geojsonData = useMemo((): GeoJSON.FeatureCollection => {
    if (!wktColumn) {
      return { type: 'FeatureCollection', features: [] };
    }

    const features: GeoJSON.Feature[] = [];

    // Filter rows by ID if specified
    const filteredRows = idFilter
      ? rows.filter((row) => {
          const idValue = String(row[idColumn || 'id'] || '').toLowerCase();
          return idValue.includes(idFilter.toLowerCase());
        })
      : rows;

    filteredRows.forEach((row, idx) => {
      const wktValue = row[wktColumn];
      if (!wktValue) return;

      const geometry = wktToGeojson(String(wktValue));
      if (!geometry) return;

      // Build properties (exclude WKT column to reduce size)
      const properties: Record<string, any> = {};
      columns.forEach((col) => {
        if (col !== wktColumn) {
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
      type: 'FeatureCollection',
      features,
    };
  }, [rows, columns, wktColumn, idColumn, idFilter]);

  if (!wktColumn) {
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
      subtitle={`Showing ${geojsonData.features.length} of ${rows.length} features`}
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

        {/* Map Info */}
        <div className="flex items-center gap-4 text-sm text-gray-400">
          <div>
            <span className="font-medium">Geometry Column:</span>{' '}
            <code className="bg-dark-sidebar px-2 py-1 rounded">{wktColumn}</code>
          </div>
          {idColumn && (
            <div>
              <span className="font-medium">ID Column:</span>{' '}
              <code className="bg-dark-sidebar px-2 py-1 rounded">{idColumn}</code>
            </div>
          )}
        </div>

        {/* Map */}
        {geojsonData.features.length > 0 ? (
          <MapView geojsonData={geojsonData} />
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
        <div className="flex items-center gap-4 text-xs text-gray-500">
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded-full bg-red-500 border-2 border-white"></div>
            <span>Points</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 bg-red-500/30 border-2 border-red-500"></div>
            <span>Polygons</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-8 h-0.5 bg-red-500"></div>
            <span>Lines</span>
          </div>
        </div>
      </div>
    </Card>
  );
}
