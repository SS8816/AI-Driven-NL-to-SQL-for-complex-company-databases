"""
Utility modules for the application
"""

from app.utils.logger import app_logger, log_request, log_response, log_error
from app.utils.jwt import create_access_token, decode_access_token, create_user_token, extract_username_from_token
from app.utils.errors import (
    AppException,
    AuthenticationError,
    AuthorizationError,
    ValidationError,
    QueryExecutionError,
    http_exception,
    unauthorized_exception,
    forbidden_exception,
    not_found_exception,
    bad_request_exception,
)

__all__ = [
    # Logger
    "app_logger",
    "log_request",
    "log_response",
    "log_error",
    # JWT
    "create_access_token",
    "decode_access_token",
    "create_user_token",
    "extract_username_from_token",
    # Errors
    "AppException",
    "AuthenticationError",
    "AuthorizationError",
    "ValidationError",
    "QueryExecutionError",
    "http_exception",
    "unauthorized_exception",
    "forbidden_exception",
    "not_found_exception",
    "bad_request_exception",
]
