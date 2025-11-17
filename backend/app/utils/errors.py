"""
Custom Exception Classes
Provides clear, structured error handling across the application
"""

from typing import Optional, Dict, Any
from fastapi import HTTPException, status


class AppException(Exception):
    """Base exception for all application errors"""

    def __init__(
        self,
        message: str,
        error_code: str,
        details: Optional[Dict[str, Any]] = None
    ):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)


class AuthenticationError(AppException):
    """Authentication failed"""

    def __init__(self, message: str = "Authentication failed", details: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="AUTH_FAILED",
            details=details
        )


class AuthorizationError(AppException):
    """User not authorized for this action"""

    def __init__(self, message: str = "Not authorized", details: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="NOT_AUTHORIZED",
            details=details
        )


class ValidationError(AppException):
    """Request validation failed"""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            details=details
        )


class SchemaNotFoundError(AppException):
    """Database schema not found"""

    def __init__(self, schema_name: str):
        super().__init__(
            message=f"Schema not found: {schema_name}",
            error_code="SCHEMA_NOT_FOUND",
            details={"schema_name": schema_name}
        )


class QueryExecutionError(AppException):
    """Query execution failed"""

    def __init__(self, message: str, execution_id: Optional[str] = None):
        super().__init__(
            message=message,
            error_code="QUERY_EXECUTION_FAILED",
            details={"execution_id": execution_id} if execution_id else {}
        )


class CacheError(AppException):
    """Cache operation failed"""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="CACHE_ERROR",
            details=details
        )


class ExportError(AppException):
    """Data export failed"""

    def __init__(self, message: str, format: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="EXPORT_FAILED",
            details={"format": format, **(details or {})}
        )


class VectorStoreError(AppException):
    """Vector store operation failed"""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            error_code="VECTORSTORE_ERROR",
            details=details
        )


# HTTP Exception Helpers

def http_exception(
    status_code: int,
    message: str,
    error_code: Optional[str] = None,
    details: Optional[Dict] = None
) -> HTTPException:
    """Create a structured HTTP exception"""
    return HTTPException(
        status_code=status_code,
        detail={
            "message": message,
            "error_code": error_code,
            "details": details or {}
        }
    )


def unauthorized_exception(message: str = "Not authenticated") -> HTTPException:
    """401 Unauthorized"""
    return http_exception(
        status_code=status.HTTP_401_UNAUTHORIZED,
        message=message,
        error_code="UNAUTHORIZED"
    )


def forbidden_exception(message: str = "Not authorized") -> HTTPException:
    """403 Forbidden"""
    return http_exception(
        status_code=status.HTTP_403_FORBIDDEN,
        message=message,
        error_code="FORBIDDEN"
    )


def not_found_exception(resource: str, identifier: str) -> HTTPException:
    """404 Not Found"""
    return http_exception(
        status_code=status.HTTP_404_NOT_FOUND,
        message=f"{resource} not found",
        error_code="NOT_FOUND",
        details={"resource": resource, "identifier": identifier}
    )


def bad_request_exception(message: str, details: Optional[Dict] = None) -> HTTPException:
    """400 Bad Request"""
    return http_exception(
        status_code=status.HTTP_400_BAD_REQUEST,
        message=message,
        error_code="BAD_REQUEST",
        details=details
    )


def internal_server_exception(message: str = "Internal server error") -> HTTPException:
    """500 Internal Server Error"""
    return http_exception(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        message=message,
        error_code="INTERNAL_ERROR"
    )
