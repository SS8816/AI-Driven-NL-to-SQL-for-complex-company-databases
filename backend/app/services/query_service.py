"""
Query Service
Wraps the LangGraph orchestrator and provides query execution
"""

from typing import Dict, Any, AsyncGenerator, Optional, Literal
import asyncio
from datetime import datetime

import pandas as pd

from app.config import settings
from app.utils.logger import app_logger, log_query_execution
from app.utils.errors import QueryExecutionError, ValidationError
from app.models.query import QueryProgress, QueryResult
from app.core.langgraph_orch import run_orchestrator


class QueryService:
    """Service for executing queries via LangGraph orchestration"""

    def __init__(self):
        self.max_preview_rows = settings.MAX_PREVIEW_ROWS

    async def execute_query_stream(
        self,
        rule_category: str,
        nl_query: str,
        schema_ddl: str,
        schema_summary: str,
        guardrails: str,
        execution_mode: Literal["normal", "reexecute", "force"],
        username: Optional[str] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Execute query and stream progress updates

        Args:
            rule_category: Rule category code
            nl_query: Natural language query
            schema_ddl: Full DDL for selected schema
            schema_summary: Token-optimized schema summary (for validation/fixing)
            guardrails: Additional constraints
            execution_mode: Execution mode (normal/reexecute/force)
            username: Username executing the query

        Yields:
            Progress updates and final result

        Example yield formats:
            {"type": "progress", "data": QueryProgress(...)}
            {"type": "result", "data": QueryResult(...)}
            {"type": "error", "data": {"message": "..."}}
        """
        try:
            app_logger.info(
                "query_execution_start",
                rule_category=rule_category,
                execution_mode=execution_mode,
                username=username,
                query_length=len(nl_query)
            )

            # Yield initial progress
            yield {
                "type": "progress",
                "data": {
                    "stage": "initializing",
                    "status": "in_progress",
                    "message": f"Starting query execution (mode: {execution_mode})",
                    "details": None
                }
            }

            # Run orchestrator (it's a generator)
            for update in run_orchestrator(
                query=nl_query,
                schema=schema_ddl,
                schema_summary=schema_summary,
                guardrails=guardrails,
                rule_category=rule_category,
                execution_mode=execution_mode
            ):
                # The orchestrator yields progress strings or final result dict
                if isinstance(update, str):
                    # Progress message
                    yield {
                        "type": "progress",
                        "data": {
                            "stage": self._infer_stage_from_message(update),
                            "status": "in_progress",
                            "message": update,
                            "details": None
                        }
                    }

                elif isinstance(update, dict):
                    # Final result
                    result = self._convert_orchestrator_result(update, rule_category, username)

                    app_logger.info(
                        "query_execution_complete",
                        rule_category=rule_category,
                        success=result.success,
                        cache_hit=result.cache_hit,
                        row_count=result.row_count,
                        execution_time_ms=result.execution_time_ms
                    )

                    yield {
                        "type": "result",
                        "data": result.model_dump()
                    }

        except Exception as e:
            app_logger.error(
                "query_execution_error",
                rule_category=rule_category,
                error=str(e),
                error_type=type(e).__name__
            )

            yield {
                "type": "error",
                "data": {
                    "message": str(e),
                    "error_type": type(e).__name__
                }
            }

    async def execute_query(
        self,
        rule_category: str,
        nl_query: str,
        schema_ddl: str,
        schema_summary: str,
        guardrails: str,
        execution_mode: Literal["normal", "reexecute", "force"],
        username: Optional[str] = None
    ) -> QueryResult:
        """
        Execute query and return final result (non-streaming)

        Args:
            rule_category: Rule category code
            nl_query: Natural language query
            schema_ddl: Full DDL for selected schema
            schema_summary: Token-optimized schema summary
            guardrails: Additional constraints
            execution_mode: Execution mode (normal/reexecute/force)
            username: Username executing the query

        Returns:
            QueryResult with execution results

        Raises:
            QueryExecutionError: If query execution fails
        """
        try:
            result = None

            # Collect all updates from stream
            async for update in self.execute_query_stream(
                rule_category=rule_category,
                nl_query=nl_query,
                schema_ddl=schema_ddl,
                schema_summary=schema_summary,
                guardrails=guardrails,
                execution_mode=execution_mode,
                username=username
            ):
                if update["type"] == "result":
                    result = QueryResult(**update["data"])
                elif update["type"] == "error":
                    raise QueryExecutionError(
                        update["data"]["message"],
                        execution_id=None
                    )

            if result is None:
                raise QueryExecutionError("Query execution completed without result")

            return result

        except QueryExecutionError:
            raise
        except Exception as e:
            app_logger.error("execute_query_error", error=str(e))
            raise QueryExecutionError(str(e))

    def _infer_stage_from_message(self, message: str) -> str:
        """Infer execution stage from progress message"""
        message_lower = message.lower()

        if "cache" in message_lower or "cached" in message_lower:
            return "cache_check"
        elif "generat" in message_lower and "sql" in message_lower:
            return "generate_sql"
        elif "validat" in message_lower:
            return "validate_sql"
        elif "ctas" in message_lower or "creating" in message_lower:
            return "execute_sql"
        elif "fix" in message_lower or "retry" in message_lower:
            return "fix_sql"
        elif "complete" in message_lower or "success" in message_lower:
            return "complete"
        else:
            return "processing"

    def _convert_orchestrator_result(
        self,
        orchestrator_result: Dict[str, Any],
        rule_category: str,
        username: Optional[str]
    ) -> QueryResult:
        """Convert orchestrator result dict to QueryResult model"""

        # Extract result DataFrame if present
        result_df = orchestrator_result.get("result_df")
        preview_data = None
        columns = None
        row_count = 0

        if result_df is not None and isinstance(result_df, pd.DataFrame):
            # Convert DataFrame to list of dicts
            preview_data = result_df.head(self.max_preview_rows).to_dict(orient="records")
            columns = result_df.columns.tolist()
            row_count = len(result_df)

        # Check for success
        success = orchestrator_result.get("error") is None

        return QueryResult(
            success=success,
            sql=orchestrator_result.get("final_sql"),
            ctas_table_name=orchestrator_result.get("ctas_table_name"),
            execution_id=orchestrator_result.get("execution_id"),
            s3_path=orchestrator_result.get("s3_path"),
            preview_data=preview_data,
            columns=columns,
            row_count=row_count,
            total_rows=orchestrator_result.get("row_count"),
            bytes_scanned=orchestrator_result.get("bytes_scanned", 0),
            execution_time_ms=orchestrator_result.get("execution_time_ms", 0),
            cache_hit=orchestrator_result.get("cache_hit", False),
            cache_age_hours=orchestrator_result.get("cached_age_hours"),
            error=orchestrator_result.get("error"),
            rag_used=orchestrator_result.get("rag_used", False)
        )


# Global instance
query_service = QueryService()
