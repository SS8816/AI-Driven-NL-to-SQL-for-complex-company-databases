# AI-Driven Violation Detection System

Natural Language to SQL pipeline for geospatial rule violation detection using AWS Athena MCP, Azure OpenAI, and LangGraph.

## Features

### Core Capabilities
- **Natural Language Queries**: Convert plain English to optimized SQL
- **Intelligent Caching**: 7-day cache with smart invalidation
- **CTAS Architecture**: Persistent result tables for exploration
- **Two-Stage SQL Validation**: Function + Syntax validation with RAG
- **Geospatial Visualization**: Interactive maps with Folium
- **Multi-Execution Modes**: Cache, Re-execute, or Force refresh

### Advanced Features
- **LangGraph Orchestration**: Multi-agent workflow with retry logic
- **RAG-Enhanced Fixing**: Vector store with Athena/Trino documentation
- **Smart Schema Extraction**: LLM-based entity extraction from nested DDL
- **Production Error Learning**: Dynamic error pattern catalog
- **Country-Based Filtering**: Query CTAS results by location
- **ID-Based Search**: Highlight specific features on map

---

## Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                    Streamlit UI (app.py)                    │
│  • Schema Selection • Query Input • Execution Controls      │
│  • Results Display • CTAS Query Interface • Visualization   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              LangGraph Orchestration (langgraph_orch.py)    │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌─────────┐   │
│  │ Generate  │→ │ Validate  │→ │  Execute  │→ │  Cache  │   │
│  │    SQL    │  │    SQL    │  │   CTAS    │  │ Result  │   │
│  └───────────┘  └───────────┘  └───────────┘  └─────────┘   │
│       ↓ Error         ↓ RAG         ↓ Athena       ↓        │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐                │
│  │Fix w/ RAG │  │ Func Val  │  │Preview Q  │                │
│  └───────────┘  └───────────┘  └───────────┘                │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  AWS Athena / Data Layer                    │
│  • Query Execution • CTAS Creation • S3 Storage             │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure
```
project/
├── app.py                      # Main Streamlit UI
├── langgraph_orch.py           # LangGraph workflow orchestration
├── prompts.py                  # All LLM prompts and instructions
├── cache_manager.py            # SQLite-based query caching
├── athena_client.py            # AWS Athena wrapper
├── parser.py                   # Nested schema DDL parser
├── viz_helper.py               # Geospatial visualization helper
├── ctas_utils.py               # CTAS naming utilities
├── country_codes.py            # ISO country code mappings
├── logger_config.py            # Logging configuration
├── config.py                   # Environment configuration
├── models.py                   # Pydantic data models
├── setup_docs_index.py         # RAG vector store setup
├── sync_errors_daily.py        # Production error sync script
├── db_viewer.py                # Cache & logs viewer
├── schemas/                    # Database schema files
│   └── *.txt                   # DDL schema definitions
├── athena_docs_vectorstore/    # FAISS vector store (generated)
├── query_cache.db              # SQLite cache database
├── query_logs.db               # SQLite execution logs
├── errors.txt                  # Production error patterns
├── .env                        # Environment variables
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

---

## Quick Start

### Prerequisites
- Python 3.11+
- AWS credentials with Athena access
- Azure OpenAI API access
- S3 bucket for Athena results

### Installation

1. **Clone the repository**
```bash
git clone <repo-link>
cd <project-directory>
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure environment variables**

Create `.env` file:
```env
# Azure OpenAI
AZURE_OPENAI_API_KEY=your_key_here
AZURE_OPENAI_ENDPOINT=https://your-endpoint.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT=gpt-4
AZURE_OPENAI_API_VERSION=2023-12-01-preview

# AWS Athena
AWS_REGION=us-east-1
ATHENA_S3_OUTPUT_LOCATION=s3://your-bucket/athena-results/
ATHENA_WORKGROUP=primary
ATHENA_TIMEOUT_SECONDS=1800

# Schema Location
SCHEMAS_DIR=schemas
```

4. **Set up AWS credentials**
```bash
aws configure
# Enter your AWS Access Key ID, Secret Key, and Region
```

5. **Add database schemas**

Place your DDL schema files in `schemas/` directory:
```
schemas/
├── fastmap_prod2_v2_13_base.txt
└── your_catalog.txt
```

6. **(Optional) Set up RAG vector store**

For enhanced SQL error fixing:
```bash
python setup_docs_index.py
```
This indexes Athena/Trino documentation (~10-15 minutes, one-time setup).

---

## Usage

### 1. Start the Application
```bash
streamlit run app.py
```

### 2. Basic Workflow

**Step 1: Select Database Catalog**
- Choose from available schemas in sidebar

**Step 2: Enter Query Details**
- **Rule Category**: e.g., `WBL039`
- **Natural Language Query**: e.g., "Vehicle Path is outside of Lane Group and Overlap between Vehicle Path and Lane Group Geometry is more than 5m and  Associated topology  is not a private road OR Associated topology is not a parking lot OR Associated topology  is not public access road OR Associated topology is not auto"

**Step 3: Analyze Query**
- Click "Analyze Query"
- Review LLM-extracted tables/columns
- Adjust selections if needed

**Step 4: Configure Execution**
- Choose execution mode:
  - **Use Cache**: Instant if query was run recently
  - **Re-execute**: Use cached SQL on fresh data
  - **Force Refresh**: Generate brand new SQL

**Step 5: Execute**
- Click "Execute Query"
- Wait for CTAS creation and preview

**Step 6: Explore Results**
- View preview (1,000 rows)
- Query CTAS table with filters
- Visualize geospatially

---

## Visualization Features

### Interactive Map Controls
- **ID Search**: Type ID to highlight specific feature
- **Measure Tool**: Measure distances on map
- **Fullscreen**: Expand map to full screen
- **Minimap**: Overview map in corner
- **Locate**: Find your current location
- **Rich Popups**: Click features for details
- **Hover Tooltips**: Preview info on hover
- **Collapsible Legend**: Click to show/hide

---

## Execution Modes

### 1. Use Cache (Normal Mode)
```
User Query → Check Cache → Return Cached Result
Instant results
No AWS costs
Data may be up to 7 days old
```

### 2. Re-execute Cached SQL
```
User Query → Get Cached SQL → Execute on Current Data → New CTAS
Uses proven SQL
Fresh data
Faster than regenerating SQL
New AWS query cost
```

### 3. Force Refresh
```
User Query → Generate New SQL → Validate → Execute → New CTAS
Brand new SQL generation
Latest optimizations
Fresh data
Full pipeline cost
```

---

## CTAS Query Interface

After CTAS creation, query the results:

### Simple Filter (Country-Based)
```sql
SELECT * FROM rule_wbl039_fastmap_20250115_143052
WHERE vp_country_code = 'DEU'
LIMIT 1000
```

### Advanced SQL
```sql
SELECT 
    vp_country_code,
    COUNT(*) as violation_count,
    AVG(overlap_length_meters) as avg_overlap
FROM rule_wbl039_fastmap_20250115_143052
WHERE overlap_length_meters > 5.0
GROUP BY vp_country_code
ORDER BY violation_count DESC
```

---

## Advanced Features

### Cache Management
```bash
# View cache statistics
streamlit run db_viewer.py

# Clear expired cache
python -c "from cache_manager import CacheManager; CacheManager().clear_expired_cache()"

# Invalidate specific cache
python -c "from cache_manager import CacheManager; CacheManager().invalidate_cache('WBL039', 'fastmap_prod2_v2_13_base')"
```

### Error Pattern Sync
```bash
# Sync production errors to errors.txt (run daily)
python sync_errors_daily.py
```

### Logs & Monitoring
```bash
# View all logs and cache
streamlit run db_viewer.py
```

---

## Testing

### Test Basic Query
```python
Rule Category: TEST01
Query: "All vehicle paths with their geometries"
Expected: CTAS created with vehicle path data
```

### Test Cache Hit
```python
1. Run query with rule "TEST01"
2. Run same query again immediately
3. Expected: "Using cached results from 0.0 hours ago"
```

### Test Country Filter
```python
1. Execute query (creates CTAS)
2. Go to "Simple Filter" tab
3. Select country from dropdown
4. Click "Execute Filter"
5. Expected: Filtered results displayed
```

### Test Visualization
```python
1. Execute query with geometry columns
2. Click "Visualize Geospatial Data"
3. Expected: Interactive map with features
4. Type ID in search box
5. Expected: Map auto-updates with highlighted feature
```

---

## Performance Tips

### For Large Datasets
1. **Limit Preview**: Default 1,000 rows for performance
2. **Use Filters**: Filter by country before visualizing
3. **Cache Aggressively**: Let cache handle repeated queries
4. **Partition Filtering**: Always include version/date filters

### For Faster Visualization
1. **Limit Features**: Visualize max 500 features
2. **Use ID Search**: Highlight specific features instead of loading all
3. **Disable Hover Effects**: Edit `viz_helper.py` if lag occurs

---

## Troubleshooting

### Common Issues

**Issue: "No .txt schema files found"**
```bash
Solution: Add schema DDL files to schemas/ directory
```

**Issue: "Azure OpenAI credentials not configured"**
```bash
Solution: Check .env file has all AZURE_OPENAI_* variables
```

**Issue: "Database not found: query_cache.db"**
```bash
Solution: Database auto-creates on first run. Check file permissions.
```

**Issue: "Vector store not found"**
```bash
Solution: Run python setup_docs_index.py (optional for RAG)
```

**Issue: Map laggy with many features**
```bash
Solution: 
1. Filter data before visualizing
2. Reduce feature count in viz_helper.py
3. Disable hover effects
```

---

##  Security Notes

- **Never commit `.env` file** - contains API keys
- **AWS credentials** - use IAM roles in production or gimme-aws-creds --username --email
- **SQL injection** - queries are validated before execution

---

## Configuration Files

### .env (Template)
```env
# Azure OpenAI
AZURE_OPENAI_API_KEY=
AZURE_OPENAI_ENDPOINT=
AZURE_OPENAI_DEPLOYMENT=gpt-5
AZURE_OPENAI_API_VERSION=2023-12-01-preview

# AWS
AWS_REGION=us-east-1
ATHENA_S3_OUTPUT_LOCATION=s3://bucket/path/
ATHENA_WORKGROUP=primary
ATHENA_TIMEOUT_SECONDS=1800

# Paths
SCHEMAS_DIR=schemas

# Optional
ENV=dev
```

---

## Future Enhancements

- [ ] Migrate to React + FastAPI for better scalability
- [ ] Implement Mapbox GL JS for faster visualization
- [ ] Add user authentication and role-based access



---

## Support

For issues or questions:
- Email: shubham.singh@here.com

---

