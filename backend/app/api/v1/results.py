"""
Results and Export API Endpoints
Handles CTAS result querying and data export
"""

from typing import Literal, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query, Response
from fastapi.responses import StreamingResponse
import io

from app.models.auth import UserInfo
from app.services.export_service import export_service
from app.utils.errors import ExportError
from app.utils.logger import app_logger
from app.dependencies import get_current_user


router = APIRouter(prefix="/results", tags=["Results"])


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
