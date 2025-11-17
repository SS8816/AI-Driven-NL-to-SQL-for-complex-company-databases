"""
Query API Endpoints
Handles query execution and history
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query

from app.models.query import ExecuteQueryRequest, QueryResult, UserQueryHistory
from app.models.auth import UserInfo
from app.models.response import SuccessResponse
from app.services.schema_service import schema_service
from app.services.query_service import query_service
from app.utils.errors import SchemaNotFoundError, QueryExecutionError
from app.utils.logger import app_logger
from app.dependencies import get_current_user, get_username
from app.db.user_queries import user_queries_repo


router = APIRouter(prefix="/queries", tags=["Queries"])


@router.post("/execute", response_model=QueryResult)
async def execute_query(
    request: ExecuteQueryRequest,
    user: UserInfo = Depends(get_current_user)
):
    """
    Execute query (non-streaming)

    Generates SQL from natural language, validates, and executes on Athena

    Requires: Authentication

    Args:
        request: Execute query request with all parameters

    Returns:
        QueryResult with execution results and preview data

    Raises:
        HTTPException 404: If schema not found
        HTTPException 500: If query execution fails
    """
    try:
        app_logger.info(
            "query_execution_requested",
            username=user.username,
            rule_category=request.rule_category,
            schema_name=request.schema_name,
            execution_mode=request.execution_mode
        )

        # Generate full DDL for selected tables/columns
        schema_ddl = schema_service.get_full_ddl_for_columns(
            schema_name=request.schema_name,
            selected_tables=request.selected_tables
        )

        # Execute query
        result = await query_service.execute_query(
            rule_category=request.rule_category,
            nl_query=request.nl_query,
            schema_ddl=schema_ddl,
            guardrails=request.guardrails or "",
            execution_mode=request.execution_mode,
            username=user.username
        )

        # Save to user query history
        try:
            await user_queries_repo.save_query(
                username=user.username,
                rule_category=request.rule_category,
                nl_query=request.nl_query,
                sql=result.sql,
                ctas_name=result.ctas_table_name,
                execution_id=result.execution_id,
                status="success" if result.success else "failed",
                error_message=result.error,
                execution_time_ms=result.execution_time_ms,
                bytes_scanned=result.bytes_scanned,
                row_count=result.row_count
            )
        except Exception as e:
            # Log but don't fail the request if history save fails
            app_logger.warning("save_query_history_failed", error=str(e))

        app_logger.info(
            "query_executed",
            username=user.username,
            rule_category=request.rule_category,
            success=result.success,
            execution_time_ms=result.execution_time_ms
        )

        return result

    except SchemaNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": str(e), "error_code": "SCHEMA_NOT_FOUND"}
        )

    except QueryExecutionError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": str(e), "error_code": "QUERY_EXECUTION_FAILED"}
        )

    except Exception as e:
        app_logger.error(
            "execute_query_error",
            username=user.username,
            rule_category=request.rule_category,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Query execution failed", "error": str(e)}
        )


@router.get("/history", response_model=List[UserQueryHistory])
async def get_query_history(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    bookmarked_only: bool = Query(default=False),
    username: str = Depends(get_username)
):
    """
    Get user's query history

    Requires: Authentication

    Args:
        limit: Maximum number of results (1-200)
        offset: Offset for pagination
        bookmarked_only: Only return bookmarked queries
        username: Username (injected from auth)

    Returns:
        List of UserQueryHistory objects
    """
    try:
        history = await user_queries_repo.get_user_history(
            username=username,
            limit=limit,
            offset=offset,
            bookmarked_only=bookmarked_only
        )

        app_logger.info(
            "query_history_retrieved",
            username=username,
            count=len(history),
            bookmarked_only=bookmarked_only
        )

        return history

    except Exception as e:
        app_logger.error("get_query_history_error", username=username, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed to retrieve query history", "error": str(e)}
        )


@router.post("/{query_id}/bookmark", response_model=SuccessResponse)
async def toggle_bookmark(
    query_id: int,
    username: str = Depends(get_username)
):
    """
    Toggle bookmark status for a query

    Requires: Authentication

    Args:
        query_id: Query ID to bookmark/unbookmark
        username: Username (injected from auth)

    Returns:
        Success response with bookmark status

    Raises:
        HTTPException 404: If query not found or doesn't belong to user
    """
    try:
        is_bookmarked = await user_queries_repo.toggle_bookmark(username, query_id)

        app_logger.info(
            "bookmark_toggled",
            username=username,
            query_id=query_id,
            bookmarked=is_bookmarked
        )

        return SuccessResponse(
            success=True,
            message=f"Query {'bookmarked' if is_bookmarked else 'unbookmarked'} successfully"
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"message": str(e), "error_code": "NOT_FOUND"}
        )

    except Exception as e:
        app_logger.error(
            "toggle_bookmark_error",
            username=username,
            query_id=query_id,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed to toggle bookmark", "error": str(e)}
        )
