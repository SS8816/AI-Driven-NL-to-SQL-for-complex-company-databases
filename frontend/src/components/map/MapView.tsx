import { useEffect, useRef, useState } from 'react';
import { Card } from '@/components/common';
import { config } from '@/config';
import type { GeojsonLayer } from './ResultsMapView';

interface MapViewProps {
  geojsonLayers: GeojsonLayer[];
  allFeatures: GeoJSON.FeatureCollection;
  title?: string;
}

export function MapView({ geojsonLayers, allFeatures, title = 'Map Visualization' }: MapViewProps) {
  const mapRef = useRef<HTMLDivElement>(null);
  const hereMapRef = useRef<H.Map | null>(null);
  const platformRef = useRef<H.service.Platform | null>(null);
  const uiRef = useRef<H.ui.UI | null>(null);
  const groupRef = useRef<H.map.Group | null>(null);
  const [infoBubble, setInfoBubble] = useState<H.ui.InfoBubble | null>(null);

  useEffect(() => {
    if (!mapRef.current || !window.H || !config.here.apiKey) return;

    // Initialize HERE Maps Platform
    platformRef.current = new window.H.service.Platform({
      apikey: config.here.apiKey,
    });

    const defaultLayers = platformRef.current.createDefaultLayers();

    // Create map instance
    hereMapRef.current = new window.H.Map(
      mapRef.current,
      defaultLayers.vector.normal.map,
      {
        center: config.here.defaultCenter,
        zoom: config.here.defaultZoom,
        pixelRatio: window.devicePixelRatio || 1,
      }
    );

    // Enable map interactions
    const behavior = new window.H.mapevents.Behavior(
      new window.H.mapevents.MapEvents(hereMapRef.current)
    );

    // Create UI
    uiRef.current = window.H.ui.UI.createDefault(hereMapRef.current, defaultLayers);

    // Create group for all map objects
    groupRef.current = new window.H.map.Group();
    hereMapRef.current.addObject(groupRef.current);

    // Cleanup on unmount
    return () => {
      if (infoBubble) {
        infoBubble.close();
      }
      if (hereMapRef.current) {
        hereMapRef.current.dispose();
      }
    };
  }, []);

  useEffect(() => {
    if (!hereMapRef.current || !groupRef.current || !window.H) return;

    // Clear existing objects
    groupRef.current.removeObjects(groupRef.current.getObjects());

    const allPoints: H.geo.Point[] = [];

    // Add features from each layer
    geojsonLayers.forEach((layer) => {
      if (!layer.geojson.features || layer.geojson.features.length === 0) return;

      layer.geojson.features.forEach((feature: any) => {
        if (!feature.geometry || !feature.geometry.coordinates) return;

        try {
          const mapObject = createMapObject(feature, layer.color);
          if (mapObject) {
            // Store feature properties in map object data
            mapObject.setData(feature.properties || {});
            groupRef.current!.addObject(mapObject);

            // Collect points for bounds calculation
            collectPoints(feature.geometry, allPoints);
          }
        } catch (error) {
          console.warn('Error creating map object:', error, feature);
        }
      });
    });

    // Fit map to show all features
    if (allPoints.length > 0) {
      try {
        const bounds = window.H.geo.Rect.coverPoints(allPoints);
        hereMapRef.current.getViewModel().setLookAtData({
          bounds: bounds,
        });
      } catch (error) {
        console.warn('Error fitting bounds:', error);
      }
    }

    // Add click listener for info bubbles
    const handleMapClick = (evt: any) => {
      if (evt.target instanceof window.H.map.Object) {
        const data = evt.target.getData();
        if (data && Object.keys(data).length > 0) {
          // Close existing info bubble
          if (infoBubble) {
            infoBubble.close();
          }

          // Get position based on object type
          let position: { lat: number; lng: number } | null = null;
          if (evt.target instanceof window.H.map.Marker) {
            position = evt.target.getGeometry();
          } else if (evt.currentPointer) {
            position = hereMapRef.current!.screenToGeo(
              evt.currentPointer.viewportX,
              evt.currentPointer.viewportY
            );
          }

          if (position) {
            // Create info bubble content
            const content = document.createElement('div');
            content.style.padding = '8px';
            content.style.maxWidth = '300px';

            Object.entries(data).forEach(([key, value]) => {
              const row = document.createElement('div');
              row.style.fontSize = '12px';
              row.style.marginBottom = '4px';
              row.innerHTML = `<span style="font-weight: 500;">${key}:</span> ${String(value)}`;
              content.appendChild(row);
            });

            const bubble = new window.H.ui.InfoBubble(position, { content });
            uiRef.current!.addBubble(bubble);
            setInfoBubble(bubble);
          }
        }
      }
    };

    hereMapRef.current.addEventListener('tap', handleMapClick);

    return () => {
      if (hereMapRef.current) {
        hereMapRef.current.removeEventListener('tap', handleMapClick);
      }
    };
  }, [geojsonLayers, allFeatures, infoBubble]);

  const totalFeatures = geojsonLayers.reduce((sum, layer) => sum + layer.featureCount, 0);

  return (
    <Card
      title={title}
      subtitle={`${geojsonLayers.length} layer${geojsonLayers.length > 1 ? 's' : ''} • ${totalFeatures} total features`}
    >
      <div
        ref={mapRef}
        className="h-[600px] rounded-lg overflow-hidden bg-light-sidebar dark:bg-dark-sidebar"
      />
    </Card>
  );
}

// Helper function to create map objects from GeoJSON features
function createMapObject(feature: any, color: string): H.map.Object | null {
  const { geometry } = feature;

  switch (geometry.type) {
    case 'Point': {
      const [lng, lat] = geometry.coordinates;
      if (isValidCoordinate(lng, lat)) {
        // Create colored circle marker
        const svgMarkup = `
          <svg width="24" height="24" xmlns="http://www.w3.org/2000/svg">
            <circle cx="12" cy="12" r="8" fill="${color}" stroke="white" stroke-width="2" opacity="0.8"/>
          </svg>
        `;
        const icon = new window.H.map.Icon(svgMarkup, {
          size: { w: 24, h: 24 },
          anchor: { x: 12, y: 12 },
        });
        return new window.H.map.Marker({ lat, lng }, { icon });
      }
      break;
    }

    case 'LineString': {
      const lineString = new window.H.geo.LineString();
      geometry.coordinates.forEach(([lng, lat]: [number, number]) => {
        if (isValidCoordinate(lng, lat)) {
          lineString.pushLatLngAlt(lat, lng);
        }
      });
      return new window.H.map.Polyline(lineString, {
        style: {
          strokeColor: color,
          lineWidth: 3,
        },
      });
    }

    case 'Polygon': {
      const exterior = new window.H.geo.LineString();
      geometry.coordinates[0].forEach(([lng, lat]: [number, number]) => {
        if (isValidCoordinate(lng, lat)) {
          exterior.pushLatLngAlt(lat, lng);
        }
      });
      const polygon = new window.H.geo.Polygon(exterior);
      return new window.H.map.Polygon(polygon, {
        style: {
          strokeColor: color,
          fillColor: color,
          lineWidth: 2,
        },
      });
    }

    case 'MultiPolygon': {
      const polygons: H.geo.Polygon[] = [];
      geometry.coordinates.forEach((polygonCoords: number[][][]) => {
        const exterior = new window.H.geo.LineString();
        polygonCoords[0].forEach(([lng, lat]: [number, number]) => {
          if (isValidCoordinate(lng, lat)) {
            exterior.pushLatLngAlt(lat, lng);
          }
        });
        polygons.push(new window.H.geo.Polygon(exterior));
      });
      const multiPolygon = new window.H.geo.MultiPolygon(polygons);
      return new window.H.map.Polygon(multiPolygon, {
        style: {
          strokeColor: color,
          fillColor: color,
          lineWidth: 2,
        },
      });
    }

    default:
      console.warn('Unsupported geometry type:', geometry.type);
      return null;
  }

  return null;
}

// Helper function to collect all points for bounds calculation
function collectPoints(geometry: any, points: H.geo.Point[]): void {
  switch (geometry.type) {
    case 'Point': {
      const [lng, lat] = geometry.coordinates;
      if (isValidCoordinate(lng, lat)) {
        points.push(new window.H.geo.Point(lat, lng));
      }
      break;
    }
    case 'LineString': {
      geometry.coordinates.forEach(([lng, lat]: [number, number]) => {
        if (isValidCoordinate(lng, lat)) {
          points.push(new window.H.geo.Point(lat, lng));
        }
      });
      break;
    }
    case 'Polygon': {
      geometry.coordinates[0].forEach(([lng, lat]: [number, number]) => {
        if (isValidCoordinate(lng, lat)) {
          points.push(new window.H.geo.Point(lat, lng));
        }
      });
      break;
    }
    case 'MultiPolygon': {
      geometry.coordinates.forEach((polygonCoords: number[][][]) => {
        polygonCoords[0].forEach(([lng, lat]: [number, number]) => {
          if (isValidCoordinate(lng, lat)) {
            points.push(new window.H.geo.Point(lat, lng));
          }
        });
      });
      break;
    }
  }
}

// Helper function to validate coordinates
function isValidCoordinate(lng: number, lat: number): boolean {
  return (
    typeof lng === 'number' &&
    typeof lat === 'number' &&
    !isNaN(lng) &&
    !isNaN(lat) &&
    lng >= -180 &&
    lng <= 180 &&
    lat >= -90 &&
    lat <= 90
  );
}
