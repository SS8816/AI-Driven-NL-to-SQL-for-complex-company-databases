# AI-Driven Violation Detection API - Backend

Production-grade FastAPI backend for the AI-Driven NL-to-SQL system.

## 🎯 Project Status

### ✅ Phase 1 Complete: Foundation Layer
- [x] Project structure and configuration
- [x] Core utilities (logger, JWT, custom errors)
- [x] Authentication service (HERE endpoint integration)
- [x] Pydantic models for all API contracts
- [x] Existing core logic integrated

### 🚧 Phase 2 In Progress: Service Layer
- [ ] Schema service (wraps parser.py)
- [ ] Query service (wraps langgraph_orch.py)
- [ ] Cache service (wraps cache_manager.py)
- [ ] Export service (CSV, JSON, GeoJSON)
- [ ] Database layer (user queries history)

### ⏳ Phase 3 Upcoming: API Layer
- [ ] REST endpoints (/api/v1/...)
- [ ] WebSocket for streaming execution
- [ ] Authentication middleware
- [ ] CORS configuration
- [ ] Rate limiting

### ⏳ Phase 4 Upcoming: Testing & Deployment
- [ ] Unit tests
- [ ] Integration tests
- [ ] API documentation (OpenAPI/Swagger)
- [ ] Deployment guide

---

## 📁 Project Structure

```
backend/
├── app/
│   ├── main.py                    # FastAPI application (coming next)
│   ├── config.py                  # ✅ Settings management
│   ├── dependencies.py            # (coming next)
│   │
│   ├── api/                       # API endpoints
│   │   └── v1/
│   │       ├── auth.py           # (coming next)
│   │       ├── schemas.py        # (coming next)
│   │       ├── queries.py        # (coming next)
│   │       ├── results.py        # (coming next)
│   │       ├── cache.py          # (coming next)
│   │       ├── logs.py           # (coming next)
│   │       └── websocket.py      # (coming next)
│   │
│   ├── services/                  # Business logic
│   │   ├── auth_service.py       # ✅ HERE authentication
│   │   ├── schema_service.py     # (coming next)
│   │   ├── query_service.py      # (coming next)
│   │   ├── cache_service.py      # (coming next)
│   │   └── export_service.py     # (coming next)
│   │
│   ├── models/                    # ✅ Pydantic models
│   │   ├── auth.py               # ✅ Auth request/response
│   │   ├── query.py              # ✅ Query workflow models
│   │   ├── schema.py             # ✅ Schema information
│   │   └── response.py           # ✅ Generic responses
│   │
│   ├── core/                      # ✅ Existing code (unchanged)
│   │   ├── langgraph_orch.py     # ✅ LangGraph orchestration
│   │   ├── parser.py             # ✅ Schema parser
│   │   ├── athena_client.py      # ✅ AWS Athena client
│   │   ├── cache_manager.py      # ✅ Cache management
│   │   ├── prompts.py            # ✅ SQL generation prompts
│   │   ├── viz_helper.py         # ✅ Geospatial visualization
│   │   ├── country_codes.py      # ✅ Country mappings
│   │   ├── ctas_utils.py         # ✅ CTAS utilities
│   │   ├── logger_config.py      # ✅ Original logger
│   │   ├── athena_config.py      # ✅ Athena config
│   │   └── athena_models.py      # ✅ Athena models
│   │
│   ├── db/                        # Database layer (coming next)
│   │   ├── database.py           # SQLite connection
│   │   └── user_queries.py       # Query history CRUD
│   │
│   └── utils/                     # ✅ Utilities
│       ├── logger.py             # ✅ Structured logging (structlog)
│       ├── jwt.py                # ✅ JWT token management
│       └── errors.py             # ✅ Custom exceptions
│
├── tests/                         # (coming next)
├── logs/                          # Generated logs
├── schemas/                       # DDL schema files (symlink to ../schemas)
├── athena_docs_vectorstore/       # FAISS vectorstore (symlink)
├── vectorstores/                  # Function vectorstore (symlink)
│
├── requirements.txt               # ✅ Python dependencies
├── .env.example                   # ✅ Environment template
├── .gitignore                     # (coming next)
└── README.md                      # ✅ This file
```

---

## 🔧 Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# Azure OpenAI
AZURE_OPENAI_API_KEY=your_key
AZURE_OPENAI_ENDPOINT=https://your-endpoint.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT=gpt-4
AZURE_OPENAI_API_VERSION=2023-12-01-preview

# AWS Athena
AWS_REGION=us-east-1
ATHENA_S3_OUTPUT_LOCATION=s3://your-bucket/athena-results/
ATHENA_WORKGROUP=primary
ATHENA_TIMEOUT_SECONDS=1800

# Authentication
HERE_AUTH_ENDPOINT=https://visualization-api.analytics.in.here.com/api/v1/authenticate

# JWT
JWT_SECRET_KEY=your-super-secret-key  # Generate with: openssl rand -hex 32
JWT_ALGORITHM=HS256
JWT_EXPIRATION_MINUTES=480  # 8 hours

# Application
SCHEMAS_DIR=schemas
LOG_LEVEL=INFO
ENV=development
```

---

## 🏗️ Architecture Decisions

### 1. **JWT Authentication (Stateless)**
- No session storage needed
- Horizontally scalable
- 8-hour token expiration
- Integrates with HERE auth endpoint

### 2. **Structured Logging (structlog)**
- JSON-formatted logs
- Contextual information
- Production-ready debugging
- Easy CloudWatch integration

### 3. **Global Cache (Shared Across Users)**
- Cost-efficient (reuse Athena results)
- Tracked by rule_category + database
- 7-day TTL
- User metadata tracked separately

### 4. **WebSocket for Execution**
- Real-time progress updates
- Streaming retry attempts
- Better UX than polling

### 5. **Layered Architecture**
- API Layer → Service Layer → Core Logic
- Clean separation of concerns
- Testable components
- Easy to modify

---

## 🚀 What's Next (Phase 2)

### Service Layer Files to Create:

1. **`services/schema_service.py`**
   - Wrap `parser.py`
   - List available schemas
   - Parse and summarize schemas
   - LLM entity extraction

2. **`services/query_service.py`**
   - Wrap `langgraph_orch.py`
   - Execute queries (3 modes: normal/reexecute/force)
   - Stream progress via WebSocket
   - Handle retries with RAG

3. **`services/cache_service.py`**
   - Wrap `cache_manager.py`
   - Cache operations
   - Statistics
   - Cleanup

4. **`services/export_service.py`**
   - Export to CSV
   - Export to JSON
   - Export to GeoJSON (new!)
   - Stream large files

5. **`db/database.py`**
   - SQLite connection
   - User queries history table
   - Bookmarking support

---

## 📚 API Design Preview

### Authentication
```
POST   /api/v1/auth/login          # Login with HERE credentials
GET    /api/v1/auth/me             # Get current user info
POST   /api/v1/auth/logout         # Logout (optional, JWT is stateless)
```

### Schemas
```
GET    /api/v1/schemas             # List available schemas
GET    /api/v1/schemas/{name}      # Get schema details
GET    /api/v1/schemas/{name}/summary  # Get LLM-friendly summary
```

### Queries
```
POST   /api/v1/queries/analyze     # Analyze NL query, extract entities
POST   /api/v1/queries/execute     # Execute query (REST)
WS     /api/v1/ws/execute          # Execute query (WebSocket streaming)
GET    /api/v1/queries/history     # Get user's query history
POST   /api/v1/queries/{id}/bookmark  # Bookmark query
```

### Results
```
GET    /api/v1/results/{ctas_name}      # Get CTAS preview
GET    /api/v1/results/{ctas_name}/export  # Export data (CSV/JSON/GeoJSON)
GET    /api/v1/results/{ctas_name}/schema  # Get CTAS schema
POST   /api/v1/results/{ctas_name}/query   # Query CTAS with filters
```

### Cache
```
GET    /api/v1/cache/stats         # Get cache statistics
DELETE /api/v1/cache/clear         # Clear expired cache
DELETE /api/v1/cache/{rule}/{db}   # Invalidate specific cache
```

### Logs
```
GET    /api/v1/logs                # Get logs (paginated, filtered)
GET    /api/v1/logs/stats          # Get log statistics
```

---

## 🎨 Code Quality Features

### ✅ Already Implemented:
- Type hints everywhere (Pydantic, type annotations)
- Structured logging with context
- Custom exception hierarchy
- Clean error messages
- Configuration validation
- Security best practices (JWT, input validation)

### Coming Soon:
- Comprehensive docstrings
- Unit test coverage
- Integration tests
- OpenAPI/Swagger docs
- Performance monitoring

---

## 🔒 Security

- **Authentication**: HERE endpoint integration
- **JWT**: Secure token generation with expiration
- **SQL Injection**: Validated in `athena_client.py`
- **Input Validation**: Pydantic models
- **CORS**: Configurable origins
- **Rate Limiting**: Per-user limits (coming)

---

## 📊 Performance Considerations

- **Async SQLite**: `aiosqlite` for non-blocking DB operations
- **Connection Pooling**: (coming)
- **Caching**: 7-day query cache
- **Streaming**: Large result sets via generators
- **Lazy Loading**: Vector stores loaded on demand

---

## 🧪 Testing Strategy

### Unit Tests (Coming)
- Service layer functions
- Utility functions
- Model validation

### Integration Tests (Coming)
- API endpoints
- Authentication flow
- Query execution workflow

### E2E Tests (Coming)
- Full user workflows
- WebSocket streaming
- Error handling

---

## 📝 Development Notes

### Key Design Patterns:
1. **Dependency Injection**: Services injected via FastAPI deps
2. **Repository Pattern**: Database access layer
3. **Service Layer Pattern**: Business logic isolation
4. **Factory Pattern**: Model creation
5. **Strategy Pattern**: Export formats

### Code Organization:
- **Models**: Data validation and serialization
- **Services**: Business logic (pure functions where possible)
- **API**: HTTP request/response handling
- **Core**: Existing logic (minimal changes)
- **Utils**: Cross-cutting concerns

---

## 🎯 Next Steps

**Immediate Priority**: Build service layer
**Timeline**: Service layer → API layer → Testing → Documentation

**Ready to continue!** 🚀
