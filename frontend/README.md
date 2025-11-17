# AI-Driven NL-to-SQL Frontend

Production-grade React frontend for the AI-Driven Natural Language to SQL conversion system.

## 🚀 Features

- **🔐 Authentication**: JWT-based authentication with HERE endpoint integration
- **🎨 Modern UI**: Databricks-style dark theme with Tailwind CSS
- **📊 Query Builder**: Step-by-step natural language query construction
- **🤖 AI-Powered**: LLM-based entity extraction and SQL generation
- **⚡ Real-time**: WebSocket streaming for live query execution progress
- **🗺️ Map Visualization**: Mapbox GL integration for geospatial data
- **📈 Results Display**: Interactive data preview with export options
- **📜 Query History**: Bookmarkable query history with pagination
- **💾 Cache Management**: View and manage query cache statistics
- **📱 Responsive**: Works on desktop and tablet devices

## 🛠️ Tech Stack

- **Framework**: React 18 + TypeScript
- **Build Tool**: Vite
- **Routing**: React Router v6
- **State Management**: Zustand
- **Server State**: TanStack Query (React Query)
- **HTTP Client**: Axios
- **Styling**: Tailwind CSS
- **Maps**: Mapbox GL JS + react-map-gl
- **Icons**: Lucide React
- **Notifications**: react-hot-toast
- **Date Handling**: date-fns

## 📦 Installation

### Prerequisites

- Node.js 18+ and npm/yarn
- Backend API running on `http://localhost:8000` (or configured URL)
- Mapbox access token (for map visualization)

### Setup

1. **Install dependencies:**

```bash
npm install
# or
yarn install
```

2. **Configure environment variables:**

Create a `.env` file in the `frontend` directory:

```bash
cp .env.example .env
```

Edit `.env` with your configuration:

```env
# API Configuration
VITE_API_BASE_URL=http://localhost:8000
VITE_WS_BASE_URL=ws://localhost:8000

# Mapbox Configuration
VITE_MAPBOX_ACCESS_TOKEN=your_mapbox_access_token_here
```

3. **Start development server:**

```bash
npm run dev
# or
yarn dev
```

The application will be available at `http://localhost:5173`

## 🏗️ Build for Production

```bash
npm run build
# or
yarn build
```

Build output will be in the `dist/` directory.

### Preview Production Build

```bash
npm run preview
# or
yarn preview
```

## 📁 Project Structure

```
frontend/
├── src/
│   ├── api/              # API client and service modules
│   │   ├── client.ts     # Axios instance with interceptors
│   │   ├── auth.ts       # Authentication API
│   │   ├── schemas.ts    # Schema management API
│   │   ├── queries.ts    # Query execution API + WebSocket
│   │   ├── results.ts    # Export API
│   │   └── cache.ts      # Cache management API
│   │
│   ├── components/       # React components
│   │   ├── common/       # Reusable UI components
│   │   │   ├── Button.tsx
│   │   │   ├── Input.tsx
│   │   │   ├── Card.tsx
│   │   │   └── ...
│   │   ├── layout/       # Layout components
│   │   │   ├── Sidebar.tsx
│   │   │   ├── Header.tsx
│   │   │   └── MainLayout.tsx
│   │   ├── auth/         # Authentication components
│   │   ├── schema/       # Schema-related components
│   │   ├── query/        # Query execution components
│   │   ├── map/          # Map visualization
│   │   └── cache/        # Cache management
│   │
│   ├── pages/            # Page components
│   │   ├── LoginPage.tsx
│   │   ├── QueryBuilderPage.tsx
│   │   ├── QueryHistoryPage.tsx
│   │   └── CacheManagementPage.tsx
│   │
│   ├── stores/           # Zustand state stores
│   │   ├── authStore.ts  # Authentication state
│   │   └── appStore.ts   # Application state
│   │
│   ├── types/            # TypeScript type definitions
│   ├── utils/            # Utility functions
│   ├── config/           # Configuration
│   ├── styles/           # Global styles
│   ├── App.tsx           # Main application component
│   └── main.tsx          # Application entry point
│
├── public/               # Static assets
├── index.html            # HTML template
├── vite.config.ts        # Vite configuration
├── tailwind.config.js    # Tailwind CSS configuration
├── tsconfig.json         # TypeScript configuration
└── package.json          # Dependencies and scripts
```

## 🎯 Usage

### 1. Login

Navigate to `/login` and enter your credentials. The application uses HERE's authentication endpoint.

### 2. Query Builder

1. **Select Schema**: Choose a schema from the dropdown
2. **Enter Query**: Type your natural language query
3. **Extract Entities**: Click to analyze and extract relevant tables/columns
4. **Execute**: Click "Execute Query" to run the query
5. **View Results**: See real-time progress and final results

### 3. View Results

- **Summary**: Row count, execution time, result table name
- **SQL**: Generated SQL with syntax formatting
- **Preview**: First 1000 rows of data
- **Export**: Download as CSV, JSON, or GeoJSON
- **Map**: View geospatial data on interactive map (if geometry present)

### 4. Query History

- View all past queries
- Bookmark important queries
- See execution status and metrics
- Expand to view generated SQL

### 5. Cache Management

- View cache statistics
- Clear expired entries
- Invalidate specific rule caches
- Monitor cache hit rate

## 🎨 UI Components

### Common Components

- `Button`: Primary, secondary, danger, ghost variants with loading states
- `Input`: Text input with labels, errors, and helper text
- `Textarea`: Multiline text input
- `Select`: Dropdown with custom styling
- `Card`: Container with optional title, subtitle, and actions
- `Badge`: Status indicators
- `Loading`: Spinner with optional text
- `EmptyState`: Placeholder for empty data

### Layout Components

- `Sidebar`: Collapsible navigation with Databricks-style design
- `Header`: User info and current date display
- `MainLayout`: Main application layout wrapper

## 🔧 Configuration

### API Endpoints

Configure in `src/config/index.ts`:

```typescript
export const config = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000',
  wsBaseUrl: import.meta.env.VITE_WS_BASE_URL || 'ws://localhost:8000',
  apiPrefix: '/api/v1',
  // ...
};
```

### Mapbox

Set your Mapbox access token in `.env`:

```env
VITE_MAPBOX_ACCESS_TOKEN=pk.your_token_here
```

### Theme Customization

Modify `tailwind.config.js` to customize colors, fonts, and other design tokens:

```javascript
theme: {
  extend: {
    colors: {
      dark: {
        bg: '#0B1120',
        sidebar: '#1A1F2E',
        card: '#242936',
        // ...
      },
    },
  },
}
```

## 🧪 Development

### Linting

```bash
npm run lint
# or
yarn lint
```

### Type Checking

TypeScript is configured with strict mode. The build will fail on type errors.

## 🌐 Browser Support

- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)

## 📝 API Integration

The frontend expects the backend API to be running with the following endpoints:

- `POST /api/v1/auth/login` - Authentication
- `GET /api/v1/schemas` - List schemas
- `POST /api/v1/schemas/analyze` - Entity extraction
- `POST /api/v1/queries/execute` - Execute query
- `WS /api/v1/ws/execute` - Streaming execution
- `GET /api/v1/queries/history` - Query history
- `GET /api/v1/results/{ctas}/export` - Export results
- `GET /api/v1/cache/stats` - Cache statistics

See backend API documentation for full endpoint specifications.

## 🚨 Troubleshooting

### CORS Issues

Ensure the backend CORS configuration includes your frontend origin:

```python
# backend/app/config.py
CORS_ORIGINS = [
    "http://localhost:5173",  # Vite dev server
    "http://localhost:3000",  # Alternative port
]
```

### WebSocket Connection Failed

- Verify `VITE_WS_BASE_URL` is correct
- Check backend WebSocket endpoint is accessible
- Ensure JWT token is valid

### Map Not Loading

- Verify `VITE_MAPBOX_ACCESS_TOKEN` is set
- Check Mapbox account has valid credits
- Ensure GeoJSON data is properly formatted

## 📄 License

This project is part of the AI-Driven NL-to-SQL system.

## 🤝 Contributing

1. Follow existing code style and patterns
2. Use TypeScript strict mode
3. Add proper error handling
4. Test all features before committing
