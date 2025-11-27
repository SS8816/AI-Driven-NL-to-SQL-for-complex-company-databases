"""
Schema API Endpoints
Handles database schema operations
"""

from typing import Dict, List
from fastapi import APIRouter, Depends, HTTPException, status

from app.models.schema import SchemaListResponse, SchemaInfo, SchemaSummary, RedactedDDLRequest, RedactedDDLResponse
from app.models.query import EntityExtractionResponse, AnalyzeQueryRequest
from app.models.auth import UserInfo
from app.services.schema_service import schema_service
from app.utils.errors import SchemaNotFoundError, ValidationError, not_found_exception
from app.utils.logger import app_logger
from app.dependencies import get_current_user


router = APIRouter(prefix="/schemas", tags=["Schemas"])


@router.get("", response_model=SchemaListResponse)
async def list_schemas(user: UserInfo = Depends(get_current_user)):
    """
    List all available database schemas

    Requires: Authentication

    Returns:
        SchemaListResponse with list of schema names
    """
    try:
        result = schema_service.list_schemas()

        app_logger.info(
            "schemas_listed",
            username=user.username,
            count=result.count
        )

        return result

    except Exception as e:
        app_logger.error("list_schemas_error", username=user.username, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed to list schemas", "error": str(e)}
        )


@router.get("/{schema_name}", response_model=SchemaInfo)
async def get_schema(
    schema_name: str,
    user: UserInfo = Depends(get_current_user)
):
    """
    Get detailed information about a specific schema

    Requires: Authentication

    Args:
        schema_name: Name of the schema

    Returns:
        SchemaInfo with tables and columns

    Raises:
        HTTPException 404: If schema not found
    """
    try:
        result = schema_service.get_schema_info(schema_name)

        app_logger.info(
            "schema_retrieved",
            username=user.username,
            schema_name=schema_name,
            table_count=result.table_count
        )

        return result

    except SchemaNotFoundError:
        raise not_found_exception("Schema", schema_name)

    except Exception as e:
        app_logger.error(
            "get_schema_error",
            username=user.username,
            schema_name=schema_name,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed to get schema", "error": str(e)}
        )


@router.get("/{schema_name}/summary", response_model=SchemaSummary)
async def get_schema_summary(
    schema_name: str,
    user: UserInfo = Depends(get_current_user)
):
    """
    Get LLM-friendly schema summary (simplified)

    Useful for token-efficient schema representation

    Requires: Authentication

    Args:
        schema_name: Name of the schema

    Returns:
        SchemaSummary with text summary and token count

    Raises:
        HTTPException 404: If schema not found
    """
    try:
        result = schema_service.get_schema_summary(schema_name)

        app_logger.info(
            "schema_summary_retrieved",
            username=user.username,
            schema_name=schema_name,
            token_count=result.token_count
        )

        return result

    except SchemaNotFoundError:
        raise not_found_exception("Schema", schema_name)

    except Exception as e:
        app_logger.error(
            "get_schema_summary_error",
            username=user.username,
            schema_name=schema_name,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed to get schema summary", "error": str(e)}
        )


@router.post("/analyze", response_model=EntityExtractionResponse)
async def analyze_query(
    request: AnalyzeQueryRequest,
    user: UserInfo = Depends(get_current_user)
):
    """
    Analyze natural language query and extract relevant tables/columns

    Uses LLM to identify which tables and columns are needed for the query

    Requires: Authentication

    Args:
        request: Analyze query request with NL query and schema name

    Returns:
        EntityExtractionResponse with extracted tables/columns and reasoning

    Raises:
        HTTPException 404: If schema not found
        HTTPException 400: If LLM response is invalid
    """
    try:
        # Extract entities using LLM
        extraction_result = schema_service.extract_entities(
            schema_name=request.schema_name,
            nl_query=request.nl_query
        )

        # Get token usage info
        summary = schema_service.get_schema_summary(request.schema_name)

        # Calculate token reduction (rough estimate)
        # Full schema would be much larger, summary is optimized
        full_schema_estimate = summary.token_count * 5  # Rough multiplier
        reduction_percent = int(((full_schema_estimate - summary.token_count) / full_schema_estimate) * 100)

        response = EntityExtractionResponse(
            tables=extraction_result["tables"],
            reasoning=extraction_result["reasoning"],
            token_usage={
                "full_schema": full_schema_estimate,
                "summary": summary.token_count,
                "reduction_percent": reduction_percent
            }
        )

        app_logger.info(
            "query_analyzed",
            username=user.username,
            schema_name=request.schema_name,
            tables_extracted=len(extraction_result["tables"])
        )

        return response

    except SchemaNotFoundError:
        raise not_found_exception("Schema", request.schema_name)

    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"message": str(e), "error_code": "VALIDATION_ERROR"}
        )

    except Exception as e:
        app_logger.error(
            "analyze_query_error",
            username=user.username,
            schema_name=request.schema_name,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed to analyze query", "error": str(e)}
        )


@router.post("/redacted-ddl", response_model=RedactedDDLResponse)
async def get_redacted_ddl(
    request: RedactedDDLRequest,
    user: UserInfo = Depends(get_current_user)
):
    """
    Get redacted DDL for selected tables and columns

    Returns the full DDL schema only for the selected tables/columns,
    filtering out any unselected tables or columns.

    Requires: Authentication

    Args:
        request: Redacted DDL request with schema name and selected tables/columns

    Returns:
        RedactedDDLResponse with DDL string, table count, and column count

    Raises:
        HTTPException 404: If schema not found
    """
    try:
        # Get full DDL and summary for selected columns (summary not needed here)
        ddl, _ = schema_service.get_full_ddl_for_columns(
            schema_name=request.schema_name,
            selected_tables=request.selected_tables
        )

        # Calculate stats
        table_count = len(request.selected_tables)
        total_columns = sum(len(cols) for cols in request.selected_tables.values())

        response = RedactedDDLResponse(
            ddl=ddl,
            table_count=table_count,
            total_columns=total_columns
        )

        app_logger.info(
            "redacted_ddl_generated",
            username=user.username,
            schema_name=request.schema_name,
            table_count=table_count,
            total_columns=total_columns
        )

        return response

    except SchemaNotFoundError:
        raise not_found_exception("Schema", request.schema_name)

    except Exception as e:
        app_logger.error(
            "get_redacted_ddl_error",
            username=user.username,
            schema_name=request.schema_name,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "Failed to generate redacted DDL", "error": str(e)}
        )
