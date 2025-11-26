import { useEffect, useRef, useState } from 'react';
import Map, { Source, Layer, Popup, MapRef } from 'react-map-gl';
import type { GeoJSONSourceRaw } from 'react-map-gl';
import mapboxgl from 'mapbox-gl';
import { Card } from '@/components/common';
import { config } from '@/config';
import type { GeojsonLayer } from './ResultsMapView';
import 'mapbox-gl/dist/mapbox-gl.css';

interface MapViewProps {
  geojsonLayers: GeojsonLayer[];
  allFeatures: GeoJSON.FeatureCollection;
  title?: string;
}

export function MapView({ geojsonLayers, allFeatures, title = 'Map Visualization' }: MapViewProps) {
  const mapRef = useRef<MapRef>(null);
  const [popupInfo, setPopupInfo] = useState<any>(null);

  useEffect(() => {
    // Fit bounds to show all features
    if (mapRef.current && allFeatures.features.length > 0) {
      const map = mapRef.current.getMap();
      const bounds = new mapboxgl.LngLatBounds();
      let hasValidCoords = false;

      allFeatures.features.forEach((feature: any) => {
        if (!feature.geometry || !feature.geometry.coordinates) return;

        const extendBounds = (coords: any) => {
          if (!coords) return;

          if (Array.isArray(coords[0])) {
            coords.forEach(extendBounds);
          } else {
            // Validate that we have valid lng/lat numbers
            const [lng, lat] = coords;
            if (
              typeof lng === 'number' &&
              typeof lat === 'number' &&
              !isNaN(lng) &&
              !isNaN(lat) &&
              lng >= -180 &&
              lng <= 180 &&
              lat >= -90 &&
              lat <= 90
            ) {
              bounds.extend([lng, lat]);
              hasValidCoords = true;
            }
          }
        };

        try {
          if (feature.geometry.type === 'Point') {
            const [lng, lat] = feature.geometry.coordinates;
            if (
              typeof lng === 'number' &&
              typeof lat === 'number' &&
              !isNaN(lng) &&
              !isNaN(lat)
            ) {
              bounds.extend([lng, lat]);
              hasValidCoords = true;
            }
          } else if (feature.geometry.coordinates) {
            extendBounds(feature.geometry.coordinates);
          }
        } catch (error) {
          console.warn('Error processing feature bounds:', error, feature);
        }
      });

      if (hasValidCoords && !bounds.isEmpty()) {
        map.fitBounds(bounds, { padding: 50, duration: 1000 });
      }
    }
  }, [allFeatures]);

  const totalFeatures = geojsonLayers.reduce((sum, layer) => sum + layer.featureCount, 0);

  return (
    <Card
      title={title}
      subtitle={`${geojsonLayers.length} layer${geojsonLayers.length > 1 ? 's' : ''} • ${totalFeatures} total features`}
    >
      <div className="h-[600px] rounded-lg overflow-hidden">
        <Map
          ref={mapRef}
          initialViewState={{
            longitude: config.mapbox.defaultCenter[0],
            latitude: config.mapbox.defaultCenter[1],
            zoom: config.mapbox.defaultZoom,
          }}
          mapStyle={config.mapbox.defaultStyle}
          mapboxAccessToken={config.mapbox.accessToken}
          onClick={(e) => {
            const features = e.features;
            if (features && features.length > 0) {
              setPopupInfo({
                lngLat: e.lngLat,
                properties: features[0].properties,
              });
            }
          }}
          interactiveLayerIds={geojsonLayers.map((layer) => `${layer.name}-layer`)}
        >
          {/* Render each geometry layer with its own color */}
          {geojsonLayers.map((layer) => {
            const geojsonSource: GeoJSONSourceRaw = {
              type: 'geojson',
              data: layer.geojson,
            };

            return (
              <Source key={layer.name} id={`${layer.name}-source`} {...geojsonSource}>
                {/* Point layer */}
                <Layer
                  id={`${layer.name}-layer`}
                  type="circle"
                  paint={{
                    'circle-radius': 8,
                    'circle-color': layer.color,
                    'circle-stroke-color': '#FFFFFF',
                    'circle-stroke-width': 2,
                    'circle-opacity': 0.8,
                  }}
                  filter={['==', ['geometry-type'], 'Point']}
                />

                {/* LineString layer */}
                <Layer
                  id={`${layer.name}-line-layer`}
                  type="line"
                  paint={{
                    'line-color': layer.color,
                    'line-width': 3,
                  }}
                  filter={['==', ['geometry-type'], 'LineString']}
                />

                {/* Polygon fill layer */}
                <Layer
                  id={`${layer.name}-polygon-layer`}
                  type="fill"
                  paint={{
                    'fill-color': layer.color,
                    'fill-opacity': 0.3,
                  }}
                  filter={['==', ['geometry-type'], 'Polygon']}
                />

                {/* Polygon outline */}
                <Layer
                  id={`${layer.name}-polygon-outline`}
                  type="line"
                  paint={{
                    'line-color': layer.color,
                    'line-width': 2,
                  }}
                  filter={['==', ['geometry-type'], 'Polygon']}
                />

                {/* MultiPolygon fill layer */}
                <Layer
                  id={`${layer.name}-multipolygon-layer`}
                  type="fill"
                  paint={{
                    'fill-color': layer.color,
                    'fill-opacity': 0.3,
                  }}
                  filter={['==', ['geometry-type'], 'MultiPolygon']}
                />

                {/* MultiPolygon outline */}
                <Layer
                  id={`${layer.name}-multipolygon-outline`}
                  type="line"
                  paint={{
                    'line-color': layer.color,
                    'line-width': 2,
                  }}
                  filter={['==', ['geometry-type'], 'MultiPolygon']}
                />
              </Source>
            );
          })}

          {popupInfo && (
            <Popup
              longitude={popupInfo.lngLat.lng}
              latitude={popupInfo.lngLat.lat}
              onClose={() => setPopupInfo(null)}
              closeButton={true}
              closeOnClick={false}
            >
              <div className="p-2">
                {Object.entries(popupInfo.properties || {}).map(([key, value]) => (
                  <div key={key} className="text-xs mb-1">
                    <span className="font-medium">{key}: </span>
                    <span>{String(value)}</span>
                  </div>
                ))}
              </div>
            </Popup>
          )}
        </Map>
      </div>
    </Card>
  );
}
