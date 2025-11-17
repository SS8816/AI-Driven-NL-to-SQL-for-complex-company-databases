"""
FastAPI Main Application
Entry point for the AI-Driven NL-to-SQL API
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from app.config import settings
from app.utils.logger import app_logger
from app.utils.errors import AppException, AuthenticationError, QueryExecutionError
from app.db.database import db

# Import API routers
from app.api.v1 import auth, schemas, queries, websocket, results, cache


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager
    Handles startup and shutdown events
    """
    # Startup
    app_logger.info("application_starting", app_name=settings.APP_NAME)

    # Initialize database
    try:
        await db.initialize()
        app_logger.info("database_initialized", db_path=str(settings.DATABASE_PATH))
    except Exception as e:
        app_logger.error("database_initialization_failed", error=str(e))
        raise

    app_logger.info("application_ready")

    yield

    # Shutdown
    app_logger.info("application_shutting_down")


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    description="AI-powered Natural Language to SQL conversion for geospatial violation detection",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)


# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"]
)


# Exception Handlers
@app.exception_handler(AppException)
async def app_exception_handler(request: Request, exc: AppException):
    """Handle custom application exceptions"""
    app_logger.error(
        "app_exception",
        error_code=exc.error_code,
        message=exc.message,
        path=request.url.path,
        details=exc.details
    )

    # Map exception types to HTTP status codes
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    if isinstance(exc, AuthenticationError):
        status_code = status.HTTP_401_UNAUTHORIZED
    elif exc.error_code in ["VALIDATION_ERROR", "SCHEMA_NOT_FOUND"]:
        status_code = status.HTTP_400_BAD_REQUEST

    return JSONResponse(
        status_code=status_code,
        content={
            "message": exc.message,
            "error_code": exc.error_code,
            "details": exc.details
        }
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle FastAPI validation errors"""
    app_logger.warning(
        "validation_error",
        path=request.url.path,
        errors=exc.errors()
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "message": "Request validation failed",
            "error_code": "VALIDATION_ERROR",
            "details": {"errors": exc.errors()}
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions"""
    app_logger.error(
        "unexpected_error",
        path=request.url.path,
        error=str(exc),
        error_type=type(exc).__name__
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "message": "An unexpected error occurred",
            "error_code": "INTERNAL_SERVER_ERROR",
            "details": {"error": str(exc)} if settings.DEBUG else {}
        }
    )


# Health Check Endpoint
@app.get("/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint

    Returns service status and basic info
    """
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": "1.0.0",
        "environment": settings.ENVIRONMENT
    }


# Root Endpoint
@app.get("/", tags=["Root"])
async def root():
    """
    Root endpoint

    Provides API information and links
    """
    return {
        "message": "AI-Driven NL-to-SQL API",
        "version": "1.0.0",
        "documentation": "/api/docs",
        "health": "/health"
    }


# Register API Routers
app.include_router(auth.router, prefix="/api/v1")
app.include_router(schemas.router, prefix="/api/v1")
app.include_router(queries.router, prefix="/api/v1")
app.include_router(websocket.router, prefix="/api/v1")
app.include_router(results.router, prefix="/api/v1")
app.include_router(cache.router, prefix="/api/v1")


# Request Logging Middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all HTTP requests"""
    app_logger.info(
        "http_request",
        method=request.method,
        path=request.url.path,
        client=request.client.host if request.client else None
    )

    response = await call_next(request)

    app_logger.info(
        "http_response",
        method=request.method,
        path=request.url.path,
        status_code=response.status_code
    )

    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level="info"
    )
