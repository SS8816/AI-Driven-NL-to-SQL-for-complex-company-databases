# HERE Maps Migration Guide

## Overview
This project has been migrated from Mapbox GL JS to HERE Maps JavaScript API v3.1.

## What Changed

### Removed Dependencies
- ❌ `mapbox-gl` - Mapbox GL JS library
- ❌ `react-map-gl` - React wrapper for Mapbox

### New Implementation
- ✅ **HERE Maps JavaScript API 3.1** (loaded from CDN)
- ✅ Native React integration with useEffect hooks
- ✅ Support for all geometry types (Point, LineString, Polygon, Multi*)
- ✅ Multiple layer support with custom colors
- ✅ Interactive info bubbles on feature click
- ✅ Auto-fit bounds to show all features

## Setup Instructions

### 1. Get your HERE API Key

Your tech lead will provide the HERE API key. Alternatively, you can:

1. Go to https://platform.here.com/
2. Sign in with your HERE Technologies account
3. Navigate to **Projects & Apps**
4. Create a new project or use existing
5. Generate an API Key with these services:
   - ✅ Vector Tile API
   - ✅ Map Tile API
   - ✅ Geocoding and Search API (optional, for future features)

### 2. Configure Environment Variables

Create a `.env` file in the `frontend/` directory:

```bash
cd frontend/
cp .env.example .env
```

Edit `.env` and add your HERE API key:

```env
# HERE Maps Configuration
VITE_HERE_API_KEY=your_actual_here_api_key_here
```

### 3. Install Dependencies

```bash
cd frontend/
npm install
```

**Note**: Mapbox dependencies have been removed from `package.json`. No HERE Maps npm packages are needed - the SDK loads from CDN.

### 4. Run the Application

```bash
npm run dev
```

The map will now use HERE Maps! 🎉

## Features

### Map Styles
The map uses `reduced.night` style by default to match the dark theme. Available styles:
- `reduced.night` - Dark minimal style (default)
- `reduced.day` - Light minimal style
- `normal.night` - Full dark style
- `normal.day` - Full light style

To change the style, edit `frontend/src/config/index.ts`:

```typescript
here: {
  defaultStyle: 'reduced.night', // Change this
}
```

### Multiple Geometry Layers
The map automatically detects and renders all WKT/geometry columns with:
- ✅ Unique colors for each layer (Red, Blue, Green, Orange, Purple, Cyan)
- ✅ Layer visibility toggles with Eye/EyeOff icons
- ✅ Color-coded legends
- ✅ Click to view feature properties in info bubble

### Supported Geometry Types
- Point
- LineString
- Polygon
- MultiPoint
- MultiLineString
- MultiPolygon

## Troubleshooting

### Map shows watermark
**Solution**: Add valid HERE API key to `.env` file

### "Failed to load map" error
**Solution**: Check internet connection - HERE Maps loads from CDN

### Map not appearing
**Solution**:
1. Check browser console for errors
2. Verify API key is set correctly
3. Clear browser cache and refresh

## API Documentation

- HERE Maps JavaScript API: https://developer.here.com/documentation/maps/3.1.34.3/dev_guide/index.html
- Platform Portal: https://platform.here.com/

## Benefits of HERE Maps

✅ **Better performance** - Optimized tile rendering
✅ **Enterprise support** - Direct support from HERE Technologies
✅ **Rich features** - Traffic, routing, geocoding APIs available
✅ **Cost effective** - Generous free tier for internal projects
✅ **Brand alignment** - Using HERE's own mapping platform!

---

**Note**: All existing functionality (country formatting, multiple geometry columns) works exactly the same. Only the underlying map rendering engine has changed.
