import { useEffect, useRef, useState } from 'react';
import Map, { Source, Layer, Popup, MapRef } from 'react-map-gl';
import type { GeoJSONSourceRaw } from 'react-map-gl';
import { Card } from '@/components/common';
import { config } from '@/config';
import 'mapbox-gl/dist/mapbox-gl.css';

interface MapViewProps {
  geojsonData: GeoJSON.FeatureCollection;
  title?: string;
}

export function MapView({ geojsonData, title = 'Map Visualization' }: MapViewProps) {
  const mapRef = useRef<MapRef>(null);
  const [popupInfo, setPopupInfo] = useState<any>(null);

  useEffect(() => {
    // Fit bounds to show all features
    if (mapRef.current && geojsonData.features.length > 0) {
      const map = mapRef.current.getMap();
      const bounds = new mapboxgl.LngLatBounds();

      geojsonData.features.forEach((feature: any) => {
        if (feature.geometry.type === 'Point') {
          bounds.extend(feature.geometry.coordinates as [number, number]);
        } else if (feature.geometry.type === 'Polygon') {
          feature.geometry.coordinates[0].forEach((coord: [number, number]) => {
            bounds.extend(coord);
          });
        }
      });

      map.fitBounds(bounds, { padding: 50, duration: 1000 });
    }
  }, [geojsonData]);

  const geojsonSource: GeoJSONSourceRaw = {
    type: 'geojson',
    data: geojsonData,
  };

  return (
    <Card title={title} subtitle={`${geojsonData.features.length} features`}>
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
          interactiveLayerIds={['data-layer']}
        >
          <Source id="data-source" {...geojsonSource}>
            {/* Point layer */}
            <Layer
              id="data-layer"
              type="circle"
              paint={{
                'circle-radius': 8,
                'circle-color': '#FF3D00',
                'circle-stroke-color': '#FFFFFF',
                'circle-stroke-width': 2,
                'circle-opacity': 0.8,
              }}
              filter={['==', ['geometry-type'], 'Point']}
            />

            {/* Polygon layer */}
            <Layer
              id="polygon-layer"
              type="fill"
              paint={{
                'fill-color': '#FF3D00',
                'fill-opacity': 0.3,
              }}
              filter={['==', ['geometry-type'], 'Polygon']}
            />

            {/* Polygon outline */}
            <Layer
              id="polygon-outline"
              type="line"
              paint={{
                'line-color': '#FF3D00',
                'line-width': 2,
              }}
              filter={['==', ['geometry-type'], 'Polygon']}
            />
          </Source>

          {popupInfo && (
            <Popup
              longitude={popupInfo.lngLat.lng}
              latitude={popupInfo.lngLat.lat}
              onClose={() => setPopupInfo(null)}
              closeButton={true}
              closeOnClick={false}
            >
              <div className="p-2">
                {Object.entries(popupInfo.properties || {}).map(
                  ([key, value]) => (
                    <div key={key} className="text-xs mb-1">
                      <span className="font-medium">{key}: </span>
                      <span>{String(value)}</span>
                    </div>
                  )
                )}
              </div>
            </Popup>
          )}
        </Map>
      </div>
    </Card>
  );
}
