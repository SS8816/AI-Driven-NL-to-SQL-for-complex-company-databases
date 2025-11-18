"""
Results Service
Handles CTAS table operations (schema inspection, querying, country filtering)
"""

from typing import List, Dict, Any, Optional
import asyncio
import re

from app.config import settings
from app.utils.logger import app_logger
from app.utils.errors import QueryExecutionError, ValidationError
from app.models.query import (
    CTASSchemaResponse,
    CTASSchemaColumn,
    CTASQueryResponse,
    CTASCountriesResponse
)
from app.core.athena_config import Config
from app.core.athena_models import QueryRequest
from app.core.athena_client import AthenaClient


class ResultsService:
    """Service for CTAS table operations"""

    # SQL keywords that are dangerous for custom queries
    DANGEROUS_KEYWORDS = [
        "DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE",
        "GRANT", "REVOKE", "EXEC", "EXECUTE", "MERGE", "REPLACE"
    ]

    def __init__(self):
        self.athena_config = Config()
        self.athena_client = AthenaClient(self.athena_config)

    async def get_ctas_schema(
        self,
        ctas_table_name: str,
        database: str
    ) -> CTASSchemaResponse:
        """
        Get schema information for a CTAS table

        Args:
            ctas_table_name: CTAS table name
            database: Database name

        Returns:
            CTASSchemaResponse with table schema

        Raises:
            QueryExecutionError: If schema retrieval fails
        """
        try:
            app_logger.info(
                "ctas_schema_request",
                ctas_table=ctas_table_name,
                database=database
            )

            # Use DESCRIBE to get table schema
            query = f"DESCRIBE {ctas_table_name}"

            result = await self._execute_query(database, query)

            # DEBUG: Log the DESCRIBE result structure
            app_logger.info(
                "describe_result_debug",
                ctas_table=ctas_table_name,
                result_columns=result.columns,
                row_count=len(result.rows),
                first_row_keys=list(result.rows[0].keys()) if result.rows else [],
                first_row_values=list(result.rows[0].values()) if result.rows else []
            )

            # Parse schema from DESCRIBE result
            columns = []
            has_country_column = False

            for row in result.rows:
                # Try different possible column name keys from DESCRIBE
                col_name = (
                    row.get("col_name") or
                    row.get("column_name") or
                    row.get("field") or
                    (list(row.values())[0] if row and len(row.values()) > 0 else "")
                )

                # Try different possible type keys
                col_type = (
                    row.get("data_type") or
                    row.get("type") or
                    (list(row.values())[1] if row and len(row.values()) > 1 else "")
                )

                # CRITICAL FIX: Athena's DESCRIBE returns tab-separated values in a single column
                # Format: "column_name\tdata_type\tcomment"
                # We need to split this if col_type is empty and col_name contains tabs
                if '\t' in str(col_name) and not col_type:
                    parts = str(col_name).split('\t')
                    col_name = parts[0].strip() if len(parts) > 0 else ""
                    col_type = parts[1].strip() if len(parts) > 1 else ""
                    # parts[2] would be comment, but we don't need it

                app_logger.info(
                    "describe_row_parsed",
                    col_name=col_name,
                    col_type=col_type,
                    row_keys=list(row.keys())
                )

                # Skip partition info and empty rows
                if not col_name or str(col_name).startswith("#") or not col_type:
                    app_logger.info(
                        "describe_row_skipped",
                        col_name=col_name,
                        col_type=col_type,
                        reason="empty or partition info"
                    )
                    continue

                columns.append(CTASSchemaColumn(name=str(col_name), type=str(col_type)))

                # Check for country column - match any column ending with country_code
                col_name_lower = str(col_name).lower()
                if (col_name_lower.endswith("country_code") or
                    col_name_lower.endswith("_country_code") or
                    col_name_lower == "iso_country_code"):
                    has_country_column = True

            app_logger.info(
                "ctas_schema_retrieved",
                ctas_table=ctas_table_name,
                column_count=len(columns),
                has_country_column=has_country_column
            )

            return CTASSchemaResponse(
                table_name=ctas_table_name,
                database=database,
                columns=columns,
                has_country_column=has_country_column
            )

        except Exception as e:
            app_logger.error(
                "ctas_schema_error",
                ctas_table=ctas_table_name,
                error=str(e)
            )
            raise QueryExecutionError(f"Failed to get CTAS schema: {str(e)}")

    async def get_distinct_countries(
        self,
        ctas_table_name: str,
        database: str
    ) -> CTASCountriesResponse:
        """
        Get distinct countries from CTAS table

        Args:
            ctas_table_name: CTAS table name
            database: Database name

        Returns:
            CTASCountriesResponse with distinct country codes

        Raises:
            QueryExecutionError: If query fails
        """
        try:
            app_logger.info(
                "ctas_countries_request",
                ctas_table=ctas_table_name,
                database=database
            )

            # First check if table has country column
            schema = await self.get_ctas_schema(ctas_table_name, database)

            if not schema.has_country_column:
                app_logger.warning(
                    "ctas_no_country_column",
                    ctas_table=ctas_table_name
                )
                return CTASCountriesResponse(
                    table_name=ctas_table_name,
                    countries=[],
                    country_count=0
                )

            # Find the country column name
            country_column = None
            for col in schema.columns:
                col_name_lower = col.name.lower()
                if (col_name_lower.endswith("country_code") or
                    col_name_lower.endswith("_country_code") or
                    col_name_lower == "iso_country_code"):
                    country_column = col.name
                    break

            if not country_column:
                return CTASCountriesResponse(
                    table_name=ctas_table_name,
                    countries=[],
                    country_count=0
                )

            # Query distinct countries using the actual column name
            query = f"""
                SELECT DISTINCT "{country_column}"
                FROM {ctas_table_name}
                WHERE "{country_column}" IS NOT NULL
                ORDER BY "{country_column}"
            """

            result = await self._execute_query(database, query, max_rows=500)

            # Extract country codes (use the actual column name)
            countries = [
                row.get(country_column, "")
                for row in result.rows
                if row.get(country_column)
            ]

            app_logger.info(
                "ctas_countries_retrieved",
                ctas_table=ctas_table_name,
                country_count=len(countries)
            )

            return CTASCountriesResponse(
                table_name=ctas_table_name,
                countries=countries,
                country_count=len(countries)
            )

        except Exception as e:
            app_logger.error(
                "ctas_countries_error",
                ctas_table=ctas_table_name,
                error=str(e)
            )
            raise QueryExecutionError(f"Failed to get countries: {str(e)}")

    async def execute_custom_query(
        self,
        ctas_table_name: str,
        database: str,
        custom_sql: str,
        limit: int = 1000
    ) -> CTASQueryResponse:
        """
        Execute custom SQL query on CTAS table

        Args:
            ctas_table_name: CTAS table name
            database: Database name
            custom_sql: Custom SQL query (SELECT only)
            limit: Maximum rows to return

        Returns:
            CTASQueryResponse with query results

        Raises:
            ValidationError: If SQL is invalid or dangerous
            QueryExecutionError: If query execution fails
        """
        try:
            app_logger.info(
                "ctas_custom_query_request",
                ctas_table=ctas_table_name,
                database=database,
                query_length=len(custom_sql)
            )

            # Validate SQL
            self._validate_custom_sql(custom_sql)

            # Replace {table} placeholder with actual table name
            query = custom_sql.replace("{table}", ctas_table_name)

            # Add LIMIT if not present
            if "limit" not in query.lower():
                query = f"{query.rstrip(';')} LIMIT {limit}"

            # Execute query
            result = await self._execute_query(database, query, max_rows=limit)

            app_logger.info(
                "ctas_custom_query_success",
                ctas_table=ctas_table_name,
                row_count=len(result.rows),
                execution_time_ms=result.execution_time_ms
            )

            return CTASQueryResponse(
                success=True,
                columns=result.columns,
                rows=result.rows,
                row_count=len(result.rows),
                execution_time_ms=result.execution_time_ms
            )

        except ValidationError:
            raise
        except Exception as e:
            app_logger.error(
                "ctas_custom_query_error",
                ctas_table=ctas_table_name,
                error=str(e)
            )

            return CTASQueryResponse(
                success=False,
                error=str(e),
                row_count=0,
                execution_time_ms=0
            )

    def _validate_custom_sql(self, sql: str) -> None:
        """
        Validate custom SQL to ensure it's safe

        Args:
            sql: SQL query to validate

        Raises:
            ValidationError: If SQL is invalid or contains dangerous keywords
        """
        if not sql or not sql.strip():
            raise ValidationError("SQL query cannot be empty")

        sql_upper = sql.upper()

        # Must be a SELECT statement
        if not sql_upper.strip().startswith("SELECT"):
            raise ValidationError("Only SELECT statements are allowed")

        # Check for dangerous keywords
        for keyword in self.DANGEROUS_KEYWORDS:
            # Use word boundaries to avoid false positives (e.g., "deleted_at" column)
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, sql_upper):
                raise ValidationError(
                    f"Query contains forbidden keyword: {keyword}"
                )

        # Check for SQL injection patterns
        if "--" in sql or "/*" in sql or "*/" in sql:
            raise ValidationError("Query contains forbidden comment syntax")

        # Check query length
        if len(sql) > 10000:
            raise ValidationError("Query is too long (max 10,000 characters)")

        app_logger.debug("custom_sql_validation_passed", query_length=len(sql))

    async def _execute_query(
        self,
        database: str,
        query: str,
        max_rows: int = 1000
    ):
        """
        Execute Athena query and return results

        Args:
            database: Database name
            query: SQL query
            max_rows: Maximum rows to return

        Returns:
            Query result from AthenaClient

        Raises:
            QueryExecutionError: If query fails or times out
        """
        request = QueryRequest(
            database=database,
            query=query,
            max_rows=max_rows
        )

        # Use thread to avoid event loop issues
        result = await asyncio.to_thread(
            lambda: asyncio.run(self.athena_client.execute_query(request))
        )

        if isinstance(result, str):
            # Timeout - execution ID returned
            raise QueryExecutionError(
                f"Query timed out. Execution ID: {result}",
                execution_id=result
            )

        return result


# Global instance
results_service = ResultsService()
