"""
Export Service
Handles exporting query results to various formats (CSV, JSON, GeoJSON)
"""

from typing import List, Dict, Any, Literal
import asyncio
import io
import json

import pandas as pd
import geopandas as gpd
from shapely import wkt
import geojson

from app.config import settings
from app.utils.logger import app_logger
from app.utils.errors import ExportError
from app.core.athena_config import Config
from app.core.athena_models import QueryRequest
from app.core.athena_client import AthenaClient


class ExportService:
    """Service for exporting query results to different formats"""

    def __init__(self):
        self.athena_config = Config()
        self.athena_client = AthenaClient(self.athena_config)

    async def export_to_csv(
        self,
        ctas_table_name: str,
        database: str,
        filter_sql: Optional[str] = None
    ) -> str:
        """
        Export CTAS table to CSV format

        Args:
            ctas_table_name: CTAS table name
            database: Database name
            filter_sql: Optional SQL filter (e.g., "WHERE country_code = 'USA'")

        Returns:
            CSV string

        Raises:
            ExportError: If export fails
        """
        try:
            app_logger.info(
                "export_csv_start",
                ctas_table=ctas_table_name,
                has_filter=filter_sql is not None
            )

            # Build query
            query = f"SELECT * FROM {ctas_table_name}"
            if filter_sql:
                query += f" {filter_sql}"

            # Execute query
            result = await self._execute_export_query(database, query)

            # Convert to CSV
            df = pd.DataFrame(result.rows, columns=result.columns)
            csv_string = df.to_csv(index=False)

            app_logger.info(
                "export_csv_complete",
                ctas_table=ctas_table_name,
                row_count=len(df),
                size_bytes=len(csv_string)
            )

            return csv_string

        except Exception as e:
            app_logger.error("export_csv_error", ctas_table=ctas_table_name, error=str(e))
            raise ExportError(f"Failed to export to CSV: {str(e)}", format="csv")

    async def export_to_json(
        self,
        ctas_table_name: str,
        database: str,
        filter_sql: Optional[str] = None,
        orient: Literal["records", "split", "index"] = "records"
    ) -> str:
        """
        Export CTAS table to JSON format

        Args:
            ctas_table_name: CTAS table name
            database: Database name
            filter_sql: Optional SQL filter
            orient: JSON orientation (records, split, index)

        Returns:
            JSON string

        Raises:
            ExportError: If export fails
        """
        try:
            app_logger.info(
                "export_json_start",
                ctas_table=ctas_table_name,
                orient=orient,
                has_filter=filter_sql is not None
            )

            # Build query
            query = f"SELECT * FROM {ctas_table_name}"
            if filter_sql:
                query += f" {filter_sql}"

            # Execute query
            result = await self._execute_export_query(database, query)

            # Convert to JSON
            df = pd.DataFrame(result.rows, columns=result.columns)
            json_string = df.to_json(orient=orient, indent=2)

            app_logger.info(
                "export_json_complete",
                ctas_table=ctas_table_name,
                row_count=len(df),
                size_bytes=len(json_string)
            )

            return json_string

        except Exception as e:
            app_logger.error("export_json_error", ctas_table=ctas_table_name, error=str(e))
            raise ExportError(f"Failed to export to JSON: {str(e)}", format="json")

    async def export_to_geojson(
        self,
        ctas_table_name: str,
        database: str,
        filter_sql: Optional[str] = None
    ) -> str:
        """
        Export CTAS table to GeoJSON format

        Requires data to have WKT geometry columns

        Args:
            ctas_table_name: CTAS table name
            database: Database name
            filter_sql: Optional SQL filter

        Returns:
            GeoJSON string

        Raises:
            ExportError: If export fails or no geometry found
        """
        try:
            app_logger.info(
                "export_geojson_start",
                ctas_table=ctas_table_name,
                has_filter=filter_sql is not None
            )

            # Build query
            query = f"SELECT * FROM {ctas_table_name}"
            if filter_sql:
                query += f" {filter_sql}"

            # Execute query
            result = await self._execute_export_query(database, query)

            # Convert to DataFrame
            df = pd.DataFrame(result.rows, columns=result.columns)

            # Find WKT columns (columns ending with '_wkt' or named 'geometry')
            wkt_columns = [col for col in df.columns if 'wkt' in col.lower() or col.lower() == 'geometry']

            if not wkt_columns:
                raise ExportError("No WKT geometry columns found in data", format="geojson")

            # Use first WKT column as primary geometry
            primary_wkt_col = wkt_columns[0]

            # Convert WKT to geometries
            def safe_wkt_to_geometry(wkt_string):
                """Safely convert WKT string to shapely geometry"""
                try:
                    if pd.notna(wkt_string) and isinstance(wkt_string, str):
                        return wkt.loads(wkt_string)
                except Exception:
                    pass
                return None

            geometries = df[primary_wkt_col].apply(safe_wkt_to_geometry)

            # Create GeoDataFrame
            # Exclude WKT columns from properties (keep only non-geometry data)
            property_columns = [col for col in df.columns if col not in wkt_columns]
            gdf = gpd.GeoDataFrame(
                df[property_columns],
                geometry=geometries,
                crs="EPSG:4326"
            )

            # Remove rows with invalid geometries
            gdf = gdf[gdf.geometry.notna()]

            # Convert to GeoJSON string
            geojson_dict = json.loads(gdf.to_json())

            # Pretty print
            geojson_string = json.dumps(geojson_dict, indent=2)

            app_logger.info(
                "export_geojson_complete",
                ctas_table=ctas_table_name,
                feature_count=len(gdf),
                size_bytes=len(geojson_string)
            )

            return geojson_string

        except ExportError:
            raise
        except Exception as e:
            app_logger.error("export_geojson_error", ctas_table=ctas_table_name, error=str(e))
            raise ExportError(f"Failed to export to GeoJSON: {str(e)}", format="geojson")

    async def _execute_export_query(self, database: str, query: str):
        """Execute export query and return results"""
        request = QueryRequest(
            database=database,
            query=query,
            max_rows=10000  # Max rows for export (configurable)
        )

        result = await asyncio.to_thread(
            lambda: asyncio.run(self.athena_client.execute_query(request))
        )

        if isinstance(result, str):
            # Timeout - execution ID returned
            raise ExportError(f"Query timed out. Execution ID: {result}", format="export")

        return result


# Global instance
export_service = ExportService()
