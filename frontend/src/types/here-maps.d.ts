// Type declarations for HERE Maps JavaScript API v3.1
declare global {
  interface Window {
    H: typeof H;
  }
}

declare namespace H {
  class Map {
    constructor(
      element: HTMLElement,
      baseLayer: H.map.layer.Layer,
      options?: {
        center?: { lat: number; lng: number };
        zoom?: number;
        pixelRatio?: number;
      }
    );
    addObject(object: H.map.Object): void;
    removeObject(object: H.map.Object): void;
    removeObjects(objects: H.map.Object[]): void;
    getViewModel(): H.map.ViewModel;
    getObjects(): H.map.Object[];
    dispose(): void;
    setCenter(center: { lat: number; lng: number }): void;
    setZoom(zoom: number): void;
  }

  namespace service {
    class Platform {
      constructor(options: { apikey: string });
      createDefaultLayers(): H.service.DefaultLayers;
    }

    interface DefaultLayers {
      vector: {
        normal: {
          map: H.map.layer.TileLayer;
        };
      };
      raster: {
        normal: {
          map: H.map.layer.TileLayer;
        };
        satellite: {
          map: H.map.layer.TileLayer;
        };
      };
    }
  }

  namespace mapevents {
    class Behavior {
      constructor(events: H.mapevents.MapEvents);
      disable(): void;
      enable(): void;
    }

    class MapEvents {
      constructor(map: H.Map);
    }
  }

  namespace ui {
    class UI {
      static createDefault(map: H.Map, layers: H.service.DefaultLayers): H.ui.UI;
      addControl(name: string, control: any): void;
      removeControl(name: string): void;
    }

    namespace InfoBubble {
      interface Options {
        content: string | HTMLElement;
      }
    }

    class InfoBubble {
      constructor(position: { lat: number; lng: number }, options: InfoBubble.Options);
      close(): void;
      open(): void;
    }
  }

  namespace map {
    class Object {
      getData(): any;
      setData(data: any): void;
    }

    class Group extends Object {
      constructor();
      addObject(object: H.map.Object): void;
      addObjects(objects: H.map.Object[]): void;
      removeObject(object: H.map.Object): void;
      removeObjects(objects: H.map.Object[]): void;
      getObjects(): H.map.Object[];
    }

    class Marker extends Object {
      constructor(position: { lat: number; lng: number }, options?: Marker.Options);
      getGeometry(): { lat: number; lng: number };
    }

    namespace Marker {
      interface Options {
        data?: any;
        icon?: H.map.Icon;
      }
    }

    class Icon {
      constructor(bitmap: string | HTMLElement, options?: Icon.Options);
    }

    namespace Icon {
      interface Options {
        size?: { w: number; h: number };
        anchor?: { x: number; y: number };
      }
    }

    class Polyline extends Object {
      constructor(lineString: H.geo.LineString, options?: Polyline.Options);
    }

    namespace Polyline {
      interface Options {
        style?: {
          strokeColor?: string;
          lineWidth?: number;
        };
        data?: any;
      }
    }

    class Polygon extends Object {
      constructor(geometry: H.geo.Polygon | H.geo.MultiPolygon, options?: Polygon.Options);
    }

    namespace Polygon {
      interface Options {
        style?: {
          strokeColor?: string;
          fillColor?: string;
          lineWidth?: number;
        };
        data?: any;
      }
    }

    class ViewModel {
      setLookAtData(data: { bounds?: H.geo.Rect; position?: { lat: number; lng: number }; zoom?: number }): void;
    }

    namespace layer {
      class Layer {}
      class TileLayer extends Layer {}
    }
  }

  namespace geo {
    class Point {
      constructor(lat: number, lng: number, alt?: number);
      lat: number;
      lng: number;
    }

    class LineString {
      constructor();
      pushPoint(point: H.geo.Point): void;
      pushLatLngAlt(lat: number, lng: number, alt?: number): void;
    }

    class Polygon {
      constructor(exterior: H.geo.LineString, interiors?: H.geo.LineString[]);
    }

    class MultiPolygon {
      constructor(polygons: H.geo.Polygon[]);
    }

    class Rect {
      constructor(top: number, left: number, bottom: number, right: number);
      static fromPoints(topLeft: H.geo.Point, bottomRight: H.geo.Point): H.geo.Rect;
      static coverPoints(points: H.geo.Point[]): H.geo.Rect;
    }
  }

  namespace util {
    function wkt2shapes(wkt: string): H.map.Object[];
  }
}

export {};
