import { useEffect, useRef, useState } from 'react';
import { Card } from '@/components/common';
import { config } from '@/config';
import type { GeojsonLayer } from './ResultsMapView';

interface MapViewProps {
  geojsonLayers: GeojsonLayer[];
  title?: string;
}

// HERE Maps types (loaded from CDN)
declare global {
  interface Window {
    H: any;
  }
}

export function MapView({ geojsonLayers, title = 'Map Visualization' }: MapViewProps) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<any>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const totalFeatures = geojsonLayers.reduce(
    (sum, layer) => sum + layer.data.features.length,
    0
  );

  useEffect(() => {
    // Load HERE Maps SDK from CDN
    const loadHereMapsSDK = () => {
      return new Promise<void>((resolve, reject) => {
        // Check if already loaded
        if (window.H) {
          resolve();
          return;
        }

        // Load HERE Maps core
        const coreScript = document.createElement('script');
        coreScript.src = 'https://js.api.here.com/v3/3.1/mapsjs-core.js';
        coreScript.async = true;
        coreScript.type = 'text/javascript';
        coreScript.charset = 'utf-8';

        // Load HERE Maps service
        const serviceScript = document.createElement('script');
        serviceScript.src = 'https://js.api.here.com/v3/3.1/mapsjs-service.js';
        serviceScript.async = true;
        serviceScript.type = 'text/javascript';
        serviceScript.charset = 'utf-8';

        // Load HERE Maps UI
        const uiScript = document.createElement('script');
        uiScript.src = 'https://js.api.here.com/v3/3.1/mapsjs-ui.js';
        uiScript.async = true;
        uiScript.type = 'text/javascript';
        uiScript.charset = 'utf-8';

        // Load HERE Maps events
        const eventsScript = document.createElement('script');
        eventsScript.src = 'https://js.api.here.com/v3/3.1/mapsjs-mapevents.js';
        eventsScript.async = true;
        eventsScript.type = 'text/javascript';
        eventsScript.charset = 'utf-8';

        // Load CSS
        const cssLink = document.createElement('link');
        cssLink.rel = 'stylesheet';
        cssLink.type = 'text/css';
        cssLink.href = 'https://js.api.here.com/v3/3.1/mapsjs-ui.css';

        let loadedCount = 0;
        const totalScripts = 4;

        const onLoad = () => {
          loadedCount++;
          if (loadedCount === totalScripts) {
            resolve();
          }
        };

        const onError = () => {
          reject(new Error('Failed to load HERE Maps SDK'));
        };

        coreScript.onload = onLoad;
        serviceScript.onload = onLoad;
        uiScript.onload = onLoad;
        eventsScript.onload = onLoad;

        coreScript.onerror = onError;
        serviceScript.onerror = onError;
        uiScript.onerror = onError;
        eventsScript.onerror = onError;

        document.head.appendChild(cssLink);
        document.head.appendChild(coreScript);
        document.head.appendChild(serviceScript);
        document.head.appendChild(uiScript);
        document.head.appendChild(eventsScript);
      });
    };

    const initializeMap = async () => {
      if (!mapRef.current) return;

      try {
        // Load SDK
        await loadHereMapsSDK();

        const H = window.H;

        // Check for API key
        if (!config.here.apiKey) {
          console.warn('HERE Maps API key not configured. Map will show with watermark.');
        }

        // Initialize the platform
        const platform = new H.service.Platform({
          apikey: config.here.apiKey || 'dummy-key-for-demo',
        });

        // Get the default map types from the platform
        const defaultLayers = platform.createDefaultLayers();

        // Create the map instance
        const map = new H.Map(
          mapRef.current,
          defaultLayers.raster.normal.map[config.here.defaultStyle] || defaultLayers.raster.normal.map,
          {
            zoom: config.here.defaultZoom,
            center: config.here.defaultCenter,
            pixelRatio: window.devicePixelRatio || 1,
          }
        );

        // Enable map interactions (pan, zoom, pinch-to-zoom)
        const behavior = new H.mapevents.Behavior(new H.mapevents.MapEvents(map));

        // Create default UI components (zoom controls, scale bar)
        const ui = H.ui.UI.createDefault(map, defaultLayers);

        // Store map instance
        mapInstanceRef.current = { map, platform, ui, behavior };

        // Add GeoJSON layers
        addGeoJsonLayers(map, geojsonLayers);

        // Fit to bounds if we have features
        if (totalFeatures > 0) {
          fitToBounds(map, geojsonLayers);
        }

        setIsLoading(false);
      } catch (error) {
        console.error('Failed to initialize HERE Maps:', error);
        setLoadError('Failed to load map. Please check your internet connection.');
        setIsLoading(false);
      }
    };

    initializeMap();

    // Cleanup
    return () => {
      if (mapInstanceRef.current) {
        mapInstanceRef.current.map.dispose();
        mapInstanceRef.current = null;
      }
    };
  }, []);

  // Update layers when geojsonLayers change
  useEffect(() => {
    if (mapInstanceRef.current && !isLoading) {
      const map = mapInstanceRef.current.map;

      // Remove all existing data layers
      map.removeObjects(map.getObjects());

      // Add new layers
      addGeoJsonLayers(map, geojsonLayers);

      // Fit to bounds
      if (totalFeatures > 0) {
        fitToBounds(map, geojsonLayers);
      }
    }
  }, [geojsonLayers, isLoading, totalFeatures]);

  const addGeoJsonLayers = (map: any, layers: GeojsonLayer[]) => {
    const H = window.H;

    layers.forEach((layer) => {
      if (layer.data.features.length === 0) return;

      // Create a GeoJSON reader
      const reader = new H.data.geojson.Reader();

      // Parse the GeoJSON
      reader.parseData(layer.data);

      // Get all objects from the reader
      const objects = reader.getObjects();

      // Style the objects based on geometry type and layer color
      objects.forEach((obj: any) => {
        const geomType = obj.getGeometry()?.type;

        if (geomType === 'Point' || geomType === 'MultiPoint') {
          // Point styling
          obj.setStyle({
            strokeColor: '#FFFFFF',
            lineWidth: 2,
            fillColor: layer.color,
          });
        } else if (geomType === 'LineString' || geomType === 'MultiLineString') {
          // Line styling
          obj.setStyle({
            strokeColor: layer.color,
            lineWidth: 3,
          });
        } else if (geomType === 'Polygon' || geomType === 'MultiPolygon') {
          // Polygon styling
          obj.setStyle({
            strokeColor: layer.color,
            lineWidth: 2,
            fillColor: layer.color + '4D', // 30% opacity (hex: 4D)
          });
        }

        // Add click event for popup
        obj.setData({
          layerName: layer.name,
          properties: obj.getData() || {},
        });
      });

      // Add all objects to the map
      map.addObjects(objects);
    });

    // Add click listener for info bubbles
    map.addEventListener('tap', (evt: any) => {
      if (evt.target instanceof H.map.Object) {
        const data = evt.target.getData();
        if (data && data.properties) {
          const properties = data.properties;
          const layerName = data.layerName || 'Feature';

          // Create info bubble content
          let content = `<div style="padding: 8px; max-width: 300px;">`;
          content += `<div style="font-weight: bold; margin-bottom: 8px; color: #333;">${layerName}</div>`;

          Object.entries(properties).forEach(([key, value]: [string, any]) => {
            if (key !== 'layerName') {
              content += `<div style="font-size: 12px; margin-bottom: 4px; color: #666;">`;
              content += `<span style="font-weight: 500;">${key}:</span> ${value}`;
              content += `</div>`;
            }
          });

          content += `</div>`;

          // Get position from event
          const position = map.screenToGeo(
            evt.currentPointer.viewportX,
            evt.currentPointer.viewportY
          );

          // Create and show info bubble
          const ui = mapInstanceRef.current.ui;
          const bubble = new H.ui.InfoBubble(position, {
            content: content,
          });
          ui.addBubble(bubble);
        }
      }
    });
  };

  const fitToBounds = (map: any, layers: GeojsonLayer[]) => {
    const H = window.H;
    const bounds = new H.geo.Rect(90, -180, -90, 180); // Initialize with inverted bounds

    let hasPoints = false;

    layers.forEach((layer) => {
      layer.data.features.forEach((feature: any) => {
        const coords = feature.geometry.coordinates;

        const extendBounds = (coords: any) => {
          if (typeof coords[0] === 'number') {
            // Single coordinate [lng, lat]
            bounds.mergePoint({ lat: coords[1], lng: coords[0] });
            hasPoints = true;
          } else {
            // Nested coordinates
            coords.forEach(extendBounds);
          }
        };

        extendBounds(coords);
      });
    });

    if (hasPoints) {
      map.getViewModel().setLookAtData(
        {
          bounds: bounds,
        },
        true // animate
      );
    }
  };

  if (loadError) {
    return (
      <Card title={title}>
        <div className="h-[600px] flex items-center justify-center bg-dark-sidebar rounded-lg">
          <div className="text-center">
            <p className="text-red-400 mb-2">{loadError}</p>
            <p className="text-gray-500 text-sm">Please refresh the page to try again.</p>
          </div>
        </div>
      </Card>
    );
  }

  return (
    <Card title={title} subtitle={`${totalFeatures} features across ${geojsonLayers.length} layers`}>
      <div className="relative h-[600px] rounded-lg overflow-hidden">
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center bg-dark-sidebar z-10">
            <div className="text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto mb-4"></div>
              <p className="text-gray-400">Loading HERE Maps...</p>
            </div>
          </div>
        )}
        <div ref={mapRef} className="w-full h-full" />
      </div>
    </Card>
  );
}
