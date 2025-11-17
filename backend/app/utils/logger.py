"""
Structured Logging with structlog
Provides JSON-formatted logs with context for production debugging
"""

import logging
import sys
from typing import Any, Dict
from pathlib import Path

import structlog
from structlog.processors import JSONRenderer
from structlog.stdlib import add_log_level, filter_by_level


def setup_logging(log_level: str = "INFO") -> structlog.BoundLogger:
    """
    Configure structured logging for the application

    Args:
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured logger instance
    """

    # Create logs directory
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper())
    )

    # File handler for persistent logs
    file_handler = logging.FileHandler(logs_dir / "app.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(message)s"))

    # Get root logger and add file handler
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()


def log_request(logger: structlog.BoundLogger, method: str, path: str, **kwargs):
    """Log HTTP request"""
    logger.info(
        "http_request",
        method=method,
        path=path,
        **kwargs
    )


def log_response(logger: structlog.BoundLogger, method: str, path: str, status_code: int, duration_ms: float):
    """Log HTTP response"""
    logger.info(
        "http_response",
        method=method,
        path=path,
        status_code=status_code,
        duration_ms=round(duration_ms, 2)
    )


def log_query_execution(
    logger: structlog.BoundLogger,
    rule_category: str,
    database: str,
    status: str,
    **kwargs
):
    """Log query execution"""
    logger.info(
        "query_execution",
        rule_category=rule_category,
        database=database,
        status=status,
        **kwargs
    )


def log_llm_interaction(
    logger: structlog.BoundLogger,
    step_name: str,
    model: str,
    token_count: int = 0,
    **kwargs
):
    """Log LLM interaction"""
    logger.info(
        "llm_interaction",
        step_name=step_name,
        model=model,
        token_count=token_count,
        **kwargs
    )


def log_error(
    logger: structlog.BoundLogger,
    error_type: str,
    message: str,
    **kwargs
):
    """Log error with context"""
    logger.error(
        "application_error",
        error_type=error_type,
        message=message,
        **kwargs
    )


def log_cache_operation(
    logger: structlog.BoundLogger,
    operation: str,
    hit: bool = False,
    **kwargs
):
    """Log cache operation"""
    logger.info(
        "cache_operation",
        operation=operation,
        cache_hit=hit,
        **kwargs
    )


# Global logger instance
app_logger = setup_logging()
