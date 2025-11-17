"""
API Models Package
Pydantic models for request/response validation
"""

from app.models.auth import LoginRequest, TokenResponse, UserInfo, LoginResponse
from app.models.query import (
    AnalyzeQueryRequest,
    EntityExtractionResponse,
    ExecuteQueryRequest,
    QueryProgress,
    QueryResult,
    CTASMetadata,
    UserQueryHistory,
)
from app.models.schema import (
    ColumnInfo,
    TableInfo,
    SchemaInfo,
    SchemaListResponse,
    SchemaSummary,
)
from app.models.response import (
    ApiResponse,
    ErrorResponse,
    SuccessResponse,
    PaginatedResponse,
    CacheStats,
    LogEntry,
)

__all__ = [
    # Auth
    "LoginRequest",
    "TokenResponse",
    "UserInfo",
    "LoginResponse",
    # Query
    "AnalyzeQueryRequest",
    "EntityExtractionResponse",
    "ExecuteQueryRequest",
    "QueryProgress",
    "QueryResult",
    "CTASMetadata",
    "UserQueryHistory",
    # Schema
    "ColumnInfo",
    "TableInfo",
    "SchemaInfo",
    "SchemaListResponse",
    "SchemaSummary",
    # Response
    "ApiResponse",
    "ErrorResponse",
    "SuccessResponse",
    "PaginatedResponse",
    "CacheStats",
    "LogEntry",
]
