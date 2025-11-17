"""
Generic Response Models
Standard API response structures
"""

from typing import Generic, TypeVar, Optional, Dict, Any
from pydantic import BaseModel, Field


T = TypeVar('T')


class ApiResponse(BaseModel, Generic[T]):
    """Generic API response wrapper"""
    success: bool = Field(..., description="Whether request succeeded")
    data: Optional[T] = Field(None, description="Response data")
    message: Optional[str] = Field(None, description="Human-readable message")
    error: Optional[Dict[str, Any]] = Field(None, description="Error details if failed")

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "data": {"key": "value"},
                "message": "Operation completed successfully",
                "error": None
            }
        }
    }


class ErrorResponse(BaseModel):
    """Error response"""
    message: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Machine-readable error code")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")

    model_config = {
        "json_schema_extra": {
            "example": {
                "message": "Authentication failed",
                "error_code": "AUTH_FAILED",
                "details": {"username": "john.doe"}
            }
        }
    }


class SuccessResponse(BaseModel):
    """Simple success response"""
    success: bool = Field(default=True, description="Success flag")
    message: str = Field(..., description="Success message")

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "message": "Operation completed successfully"
            }
        }
    }


class PaginatedResponse(BaseModel, Generic[T]):
    """Paginated response"""
    items: list[T] = Field(..., description="List of items")
    total: int = Field(..., description="Total number of items")
    page: int = Field(..., description="Current page number (1-indexed)")
    page_size: int = Field(..., description="Number of items per page")
    total_pages: int = Field(..., description="Total number of pages")
    has_next: bool = Field(..., description="Whether there is a next page")
    has_prev: bool = Field(..., description="Whether there is a previous page")

    model_config = {
        "json_schema_extra": {
            "example": {
                "items": [{"id": 1}, {"id": 2}],
                "total": 100,
                "page": 1,
                "page_size": 10,
                "total_pages": 10,
                "has_next": True,
                "has_prev": False
            }
        }
    }


class CacheStats(BaseModel):
    """Cache statistics"""
    total_entries: int = Field(..., description="Total cache entries")
    valid_entries: int = Field(..., description="Valid (non-expired) entries")
    expired_entries: int = Field(..., description="Expired entries")
    ctas_count: int = Field(..., description="CTAS execution count")
    direct_count: int = Field(..., description="Direct execution count")
    hit_rate: Optional[float] = Field(None, description="Cache hit rate percentage")

    model_config = {
        "json_schema_extra": {
            "example": {
                "total_entries": 150,
                "valid_entries": 120,
                "expired_entries": 30,
                "ctas_count": 140,
                "direct_count": 10,
                "hit_rate": 85.5
            }
        }
    }


class LogEntry(BaseModel):
    """Log entry"""
    id: int = Field(..., description="Log entry ID")
    timestamp: str = Field(..., description="Log timestamp")
    level: str = Field(..., description="Log level (INFO, WARNING, ERROR)")
    message: str = Field(..., description="Log message")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 123,
                "timestamp": "2025-01-17T14:30:52Z",
                "level": "INFO",
                "message": "Query executed successfully",
                "context": {"rule_category": "WBL039", "execution_time_ms": 15000}
            }
        }
    }
