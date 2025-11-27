"""
WebSocket API Endpoints
Handles streaming query execution with real-time progress updates
"""

import json
from typing import Dict, Any
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, status

from app.services.schema_service import schema_service
from app.services.query_service import query_service
from app.utils.jwt import extract_username_from_token
from app.utils.errors import AuthenticationError, SchemaNotFoundError
from app.utils.logger import app_logger
from app.db.user_queries import user_queries_repo


router = APIRouter(prefix="/ws", tags=["WebSocket"])


@router.websocket("/execute")
async def execute_query_stream(websocket: WebSocket):
    """
    Execute query with streaming progress updates

    WebSocket protocol:
    1. Client connects and sends token + query params
    2. Server authenticates and starts execution
    3. Server streams progress updates
    4. Server sends final result
    5. Connection closes

    Message formats:
    - Client→Server (initial): {"token": "...", "request": {...}}
    - Server→Client (progress): {"type": "progress", "data": {...}}
    - Server→Client (result): {"type": "result", "data": {...}}
    - Server→Client (error): {"type": "error", "data": {...}}

    Args:
        websocket: WebSocket connection
    """
    await websocket.accept()

    username = None
    rule_category = None

    try:
        # Receive initial message with auth token and request
        initial_message = await websocket.receive_text()
        message_data = json.loads(initial_message)

        # Extract token and validate
        token = message_data.get("token")
        if not token:
            await websocket.send_json({
                "type": "error",
                "data": {
                    "message": "Authentication token required",
                    "error_code": "AUTH_REQUIRED"
                }
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        # Authenticate user
        try:
            username = extract_username_from_token(token)
        except AuthenticationError as e:
            await websocket.send_json({
                "type": "error",
                "data": {
                    "message": str(e),
                    "error_code": "AUTH_FAILED"
                }
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        # Extract request data
        request_data = message_data.get("request", {})
        rule_category = request_data.get("rule_category")
        nl_query = request_data.get("nl_query")
        schema_name = request_data.get("schema_name")
        selected_tables = request_data.get("selected_tables", {})
        guardrails = request_data.get("guardrails", "")
        execution_mode = request_data.get("execution_mode", "normal")

        # Validate required fields
        if not all([rule_category, nl_query, schema_name, selected_tables]):
            await websocket.send_json({
                "type": "error",
                "data": {
                    "message": "Missing required fields",
                    "error_code": "VALIDATION_ERROR"
                }
            })
            await websocket.close(code=status.WS_1003_UNSUPPORTED_DATA)
            return

        app_logger.info(
            "websocket_query_started",
            username=username,
            rule_category=rule_category,
            execution_mode=execution_mode
        )

        # Generate full DDL and schema summary for selected tables/columns
        try:
            schema_ddl, schema_summary = schema_service.get_full_ddl_for_columns(
                schema_name=schema_name,
                selected_tables=selected_tables
            )
        except SchemaNotFoundError as e:
            await websocket.send_json({
                "type": "error",
                "data": {
                    "message": str(e),
                    "error_code": "SCHEMA_NOT_FOUND"
                }
            })
            await websocket.close(code=status.WS_1003_UNSUPPORTED_DATA)
            return

        # Stream query execution
        final_result = None
        async for update in query_service.execute_query_stream(
            rule_category=rule_category,
            nl_query=nl_query,
            schema_ddl=schema_ddl,
            schema_summary=schema_summary,
            guardrails=guardrails,
            execution_mode=execution_mode,
            username=username
        ):
            # Send update to client
            await websocket.send_json(update)

            # Capture final result for history
            if update.get("type") == "result":
                final_result = update.get("data")

        # Save to user query history
        if final_result:
            try:
                await user_queries_repo.save_query(
                    username=username,
                    rule_category=rule_category,
                    nl_query=nl_query,
                    sql=final_result.get("sql"),
                    ctas_name=final_result.get("ctas_table_name"),
                    execution_id=final_result.get("execution_id"),
                    status="success" if final_result.get("success") else "failed",
                    error_message=final_result.get("error"),
                    execution_time_ms=final_result.get("execution_time_ms", 0),
                    bytes_scanned=final_result.get("bytes_scanned", 0),
                    row_count=final_result.get("row_count", 0)
                )
            except Exception as e:
                app_logger.warning("save_query_history_failed", error=str(e))

        app_logger.info(
            "websocket_query_completed",
            username=username,
            rule_category=rule_category,
            success=final_result.get("success") if final_result else False
        )

        # Close connection gracefully
        await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)

    except WebSocketDisconnect:
        app_logger.info(
            "websocket_disconnected",
            username=username,
            rule_category=rule_category
        )

    except json.JSONDecodeError as e:
        await websocket.send_json({
            "type": "error",
            "data": {
                "message": "Invalid JSON in request",
                "error_code": "INVALID_JSON"
            }
        })
        await websocket.close(code=status.WS_1003_UNSUPPORTED_DATA)

    except Exception as e:
        app_logger.error(
            "websocket_error",
            username=username,
            rule_category=rule_category,
            error=str(e)
        )

        try:
            await websocket.send_json({
                "type": "error",
                "data": {
                    "message": f"Execution failed: {str(e)}",
                    "error_code": "EXECUTION_ERROR"
                }
            })
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        except:
            # Connection might already be closed
            pass
