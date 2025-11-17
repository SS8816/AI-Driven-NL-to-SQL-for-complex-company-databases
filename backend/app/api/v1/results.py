"""
Results and Export API Endpoints
Handles CTAS result querying and data export
"""

from typing import Literal, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query, Response, Body
from fastapi.responses import StreamingResponse
import io

from app.models.auth import UserInfo
from app.models.query import (
    CTASSchemaResponse,
    CTASQueryRequest,
    CTASQueryResponse,
    CTASCountriesResponse
)
from app.services.export_service import export_service
from app.services.results_service import results_service
from app.utils.errors import ExportError, QueryExecutionError, ValidationError
from app.utils.logger import app_logger
from app.dependencies import get_current_user


router = APIRouter(prefix="/results", tags=["Results"])


@router.get("/{ctas_table_name}/schema", response_model=CTASSchemaResponse)
async def get_ctas_schema(
    ctas_table_name: str,
    database: str = Query(..., description="Database name"),
    user: UserInfo = Depends(get_current_user)
):
    """
    Get schema information for a CTAS table

    Returns table columns, types, and whether it has iso_country_code column

    Requires: Authentication

    Args:
        ctas_table_name: CTAS table name
        database: Database name
        user: Authenticated user (injected)

    Returns:
        CTASSchemaResponse with table schema

    Raises:
        HTTPException 500: If schema retrieval fails
    """
    try:
        app_logger.info(
            "get_schema_requested",
            username=user.username,
            ctas_table=ctas_table_name,
            database=database
        )

        schema = await results_service.get_ctas_schema(ctas_table_name, database)

        return schema

    except QueryExecutionError as e:
        app_logger.error(
            "get_schema_error",
            username=user.username,
            ctas_table=ctas_table_name,
            error=str(e)
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "message": "Failed to retrieve schema",
                "error": str(e)
            }
        )


@router.get("/{ctas_table_name}/countries", response_model=CTASCountriesResponse)
async def get_ctas_countries(
    ctas_table_name: str,
    database: str = Query(..., description="Database name"),
    user: UserInfo = Depends(get_current_user)
):
    """
    Get distinct countries from CTAS table

    Returns list of ISO 3166-1 alpha-3 country codes found in the table

    Requires: Authentication

    Args:
        ctas_table_name: CTAS table name
        database: Database name
        user: Authenticated user (injected)

    Returns:
        CTASCountriesResponse with distinct country codes

    Raises:
        HTTPException 500: If query fails
    """
    try:
        app_logger.info(
            "get_countries_requested",
            username=user.username,
            ctas_table=ctas_table_name,
            database=database
        )

        countries = await results_service.get_distinct_countries(ctas_table_name, database)

        return countries

    except QueryExecutionError as e:
        app_logger.error(
            "get_countries_error",
            username=user.username,
            ctas_table=ctas_table_name,
            error=str(e)
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "message": "Failed to retrieve countries",
                "error": str(e)
            }
        )


@router.post("/{ctas_table_name}/query", response_model=CTASQueryResponse)
async def query_ctas_table(
    ctas_table_name: str,
    database: str = Query(..., description="Database name"),
    request: CTASQueryRequest = Body(...),
    user: UserInfo = Depends(get_current_user)
):
    """
    Execute custom SQL query on CTAS table

    Allows running SELECT queries with custom filters and conditions.
    Only SELECT statements are allowed. Dangerous keywords are blocked.

    Use {table} as placeholder for the table name in your SQL.

    Requires: Authentication

    Args:
        ctas_table_name: CTAS table name
        database: Database name
        request: Query request with custom SQL
        user: Authenticated user (injected)

    Returns:
        CTASQueryResponse with query results

    Raises:
        HTTPException 400: If SQL is invalid or contains dangerous keywords
        HTTPException 500: If query execution fails
    """
    try:
        app_logger.info(
            "custom_query_requested",
            username=user.username,
            ctas_table=ctas_table_name,
            database=database,
            limit=request.limit
        )

        result = await results_service.execute_custom_query(
            ctas_table_name=ctas_table_name,
            database=database,
            custom_sql=request.custom_sql,
            limit=request.limit or 1000
        )

        return result

    except ValidationError as e:
        app_logger.warning(
            "custom_query_validation_error",
            username=user.username,
            ctas_table=ctas_table_name,
            error=str(e)
        )

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "message": "Invalid SQL query",
                "error": str(e),
                "error_code": "VALIDATION_ERROR"
            }
        )

    except QueryExecutionError as e:
        app_logger.error(
            "custom_query_error",
            username=user.username,
            ctas_table=ctas_table_name,
            error=str(e)
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "message": "Query execution failed",
                "error": str(e)
            }
        )


@router.get("/{ctas_table_name}/export")
async def export_results(
    ctas_table_name: str,
    database: str = Query(..., description="Database name"),
    format: Literal["csv", "json", "geojson"] = Query(default="csv", description="Export format"),
    filter: Optional[str] = Query(None, description="SQL filter clause (e.g., 'WHERE country_code = \"USA\"')"),
    user: UserInfo = Depends(get_current_user)
):
    """
    Export CTAS results to various formats

    Supports CSV, JSON, and GeoJSON export formats

    Requires: Authentication

    Args:
        ctas_table_name: CTAS table name to export
        database: Database name
        format: Export format (csv, json, geojson)
        filter: Optional SQL WHERE clause filter
        user: Authenticated user (injected)

    Returns:
        File download with exported data

    Raises:
        HTTPException 400: If export format invalid or no geometry for GeoJSON
        HTTPException 500: If export fails
    """
    try:
        app_logger.info(
            "export_requested",
            username=user.username,
            ctas_table=ctas_table_name,
            format=format,
            has_filter=filter is not None
        )

        # Export based on format
        if format == "csv":
            content = await export_service.export_to_csv(
                ctas_table_name=ctas_table_name,
                database=database,
                filter_sql=filter
            )
            media_type = "text/csv"
            filename = f"{ctas_table_name}.csv"

        elif format == "json":
            content = await export_service.export_to_json(
                ctas_table_name=ctas_table_name,
                database=database,
                filter_sql=filter,
                orient="records"
            )
            media_type = "application/json"
            filename = f"{ctas_table_name}.json"

        elif format == "geojson":
            content = await export_service.export_to_geojson(
                ctas_table_name=ctas_table_name,
                database=database,
                filter_sql=filter
            )
            media_type = "application/geo+json"
            filename = f"{ctas_table_name}.geojson"

        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"message": f"Unsupported format: {format}"}
            )

        app_logger.info(
            "export_completed",
            username=user.username,
            ctas_table=ctas_table_name,
            format=format,
            size_bytes=len(content)
        )

        # Return as downloadable file
        return Response(
            content=content,
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            }
        )

    except ExportError as e:
        app_logger.error(
            "export_error",
            username=user.username,
            ctas_table=ctas_table_name,
            format=format,
            error=str(e)
        )

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST if "geometry" in str(e).lower() else status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "message": str(e),
                "error_code": "EXPORT_ERROR",
                "format": format
            }
        )

    except Exception as e:
        app_logger.error(
            "export_unexpected_error",
            username=user.username,
            ctas_table=ctas_table_name,
            format=format,
            error=str(e)
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "message": "Export failed",
                "error": str(e)
            }
        )
