import asyncio
import logging
import re
import time
from typing import Optional, Union

import boto3
from botocore.exceptions import ClientError

from config import Config
from models import DatabaseInfo, QueryRequest, QueryResult, QueryState, QueryStatus, TableInfo

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class AthenaError(Exception):
    """Simple Athena error with code."""
    def __init__(
        self, message: str, code: str = "ATHENA_ERROR", query_execution_id: Optional[str] = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.query_execution_id = query_execution_id


class QueryValidator:
    """Validates and sanitizes SQL queries to prevent injection attacks."""
    DANGEROUS_PATTERNS = [
        r";\s*(drop|delete|truncate|alter|create|insert|update)\s+",
        r"--\s*", r"/\*.*?\*/", r"xp_cmdshell", r"sp_executesql",
        r"exec\s*\(", r"information_schema", r"sys\.",
    ]

    @classmethod
    def validate_query(cls, query: str) -> None:
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        query_lower = query.lower()
        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, query_lower, re.IGNORECASE | re.DOTALL):
                logger.warning(f"Potentially dangerous SQL pattern detected: {pattern}")
                raise ValueError(f"Query contains potentially dangerous pattern: {pattern}")
        if len(query) > 100000:
            raise ValueError("Query is too large (max 100KB)")
        logger.debug(f"Query validation passed for query of length {len(query)}")

    @classmethod
    def sanitize_identifier(cls, identifier: str) -> str:
        if not identifier or not identifier.strip():
            raise ValueError("Identifier cannot be empty")
        sanitized = re.sub(r"[^a-zA-Z0-9_.-]", "", identifier.strip())
        if not sanitized:
            raise ValueError("Identifier contains only invalid characters")
        if len(sanitized) > 255:
            raise ValueError("Identifier is too long (max 255 characters)")
        return sanitized


class AthenaClient:
    """Simple AWS Athena client wrapper."""
    def __init__(self, config: Config):
        self.config = config
        session = boto3.Session(region_name=config.aws_region)
        self.client = session.client("athena")
        logger.info(f"Initialized Athena client for region: {config.aws_region}")

    async def execute_query(self, request: QueryRequest) -> Union[QueryResult, str]:
        """
        Execute a query and return results or execution ID if timeout.
        """
        logger.info(f"Executing query in database: {request.database}")
        logger.debug(f"Query: {request.query[:200]}...")
        try:
            QueryValidator.validate_query(request.query)
            sanitized_database = QueryValidator.sanitize_identifier(request.database)
            start_params = {
                "QueryString": request.query,
                "QueryExecutionContext": {"Database": sanitized_database},
                "ResultConfiguration": {"OutputLocation": self.config.s3_output_location},
            }
            if self.config.athena_workgroup:
                start_params["WorkGroup"] = self.config.athena_workgroup
            
            response = self.client.start_query_execution(**start_params)
            query_execution_id = response["QueryExecutionId"]
            logger.info(f"Started query execution: {query_execution_id}")

            if await self._wait_for_completion(query_execution_id):
                logger.info(f"Query completed successfully: {query_execution_id}")
                return await self.get_query_results(query_execution_id, request.max_rows)
            else:
                logger.warning(f"Query timed out: {query_execution_id}")
                return query_execution_id

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            raise AthenaError(str(e), error_code) from e
        except Exception as e:
            logger.error(f"Unexpected error during query execution: {str(e)}")
            raise

    async def get_query_status(self, query_execution_id: str) -> QueryStatus:
        """Get the status of a query execution."""
        try:
            response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            execution = response.get("QueryExecution", {})
            status = execution.get("Status", {})
            stats = execution.get("Statistics", {})
            return QueryStatus(
                query_execution_id=query_execution_id,
                state=QueryState(status.get("State", "UNKNOWN")),
                state_change_reason=status.get("StateChangeReason"),
                bytes_scanned=stats.get("DataScannedInBytes", 0),
                execution_time_ms=stats.get("EngineExecutionTimeInMillis", 0),
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            raise AthenaError(str(e), error_code, query_execution_id) from e

    async def get_query_results(self, query_execution_id: str, max_rows: int = 1000) -> QueryResult:
        """Get results for a completed query."""
        logger.info(f"Getting results for query: {query_execution_id}, max_rows: {max_rows}")
        try:
            status = await self.get_query_status(query_execution_id)
            if status.state != QueryState.SUCCEEDED:
                reason = status.state_change_reason or f"Query in non-successful state: {status.state}"
                raise AthenaError(reason, f"QUERY_{status.state}", query_execution_id)

            paginator = self.client.get_paginator('get_query_results')
            pages = paginator.paginate(QueryExecutionId=query_execution_id, PaginationConfig={'MaxItems': max_rows})
            
            rows_data = []
            column_info = []

            for page in pages:
                if not column_info:
                     meta = page.get("ResultSet", {}).get("ResultSetMetadata", {})
                     column_info = meta.get("ColumnInfo", [])
                
                page_rows = page.get("ResultSet", {}).get("Rows", [])
                rows_data.extend(page_rows)
                if len(rows_data) >= max_rows + 1: # +1 for header
                    break
            
            columns = [col.get("Name", "") for col in column_info]
            start_index = 1 if rows_data and columns else 0
            
            rows = []
            for row_data in rows_data[start_index:max_rows+1]:
                row = {columns[i]: data.get("VarCharValue") for i, data in enumerate(row_data.get("Data", []))}
                rows.append(row)

            return QueryResult(
                query_execution_id=query_execution_id,
                columns=columns, rows=rows,
                bytes_scanned=status.bytes_scanned,
                execution_time_ms=status.execution_time_ms,
            )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "UNKNOWN")
            raise AthenaError(str(e), error_code, query_execution_id) from e

    async def _wait_for_completion(self, query_execution_id: str) -> bool:
        """Wait for query completion with timeout."""
        timeout = self.config.timeout_seconds
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = await self.get_query_status(query_execution_id)
            if status.state == QueryState.SUCCEEDED:
                return True
            if status.state in [QueryState.FAILED, QueryState.CANCELLED]:
                raise AthenaError(
                    status.state_change_reason or "Query failed or was cancelled",
                    f"QUERY_{status.state}",
                    query_execution_id,
                )
            await asyncio.sleep(1)
        return False