"""
Query Models
Pydantic models for query execution workflow
"""

from typing import List, Dict, Optional, Any, Literal
from datetime import datetime
from pydantic import BaseModel, Field


class AnalyzeQueryRequest(BaseModel):
    """Request to analyze natural language query"""
    nl_query: str = Field(..., min_length=10, description="Natural language query")
    schema_name: str = Field(..., description="Database schema/catalog name")

    model_config = {
        "json_schema_extra": {
            "example": {
                "nl_query": "Vehicle paths outside lane groups with overlap > 5m",
                "schema_name": "fastmap_prod2_v2_13_base"
            }
        }
    }


class EntityExtractionResponse(BaseModel):
    """Response from entity extraction (tables/columns)"""
    tables: Dict[str, List[str]] = Field(..., description="Extracted tables and their columns")
    reasoning: str = Field(..., description="LLM's reasoning for extraction")
    token_usage: Dict[str, int] = Field(..., description="Token usage stats")

    model_config = {
        "json_schema_extra": {
            "example": {
                "tables": {
                    "vehicle_path": ["id", "geometry", "iso_country_code"],
                    "lanegroup": ["id", "geometry", "iso_country_code"]
                },
                "reasoning": "Selected vehicle_path and lanegroup tables for spatial analysis",
                "token_usage": {
                    "full_schema": 15000,
                    "summary": 3000,
                    "reduction_percent": 80
                }
            }
        }
    }


class ExecuteQueryRequest(BaseModel):
    """Request to execute query with selected schema"""
    rule_category: str = Field(..., description="Rule category code (e.g., WBL039)")
    nl_query: str = Field(..., min_length=10, description="Natural language query")
    schema_name: str = Field(..., description="Database schema/catalog name")
    selected_tables: Dict[str, List[str]] = Field(..., description="User-approved tables and columns")
    guardrails: Optional[str] = Field(None, description="Additional constraints/hints")
    execution_mode: Literal["normal", "reexecute", "force"] = Field(
        default="normal",
        description="Execution mode: normal (cache), reexecute (cached SQL), force (new SQL)"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "rule_category": "WBL039",
                "nl_query": "Vehicle paths outside lane groups",
                "schema_name": "fastmap_prod2_v2_13_base",
                "selected_tables": {
                    "vehicle_path": ["id", "geometry", "iso_country_code"],
                    "lanegroup": ["id", "geometry"]
                },
                "guardrails": "Filter by version = 'latest'",
                "execution_mode": "normal"
            }
        }
    }


class QueryProgress(BaseModel):
    """Query execution progress update"""
    stage: str = Field(..., description="Current stage name")
    status: Literal["in_progress", "completed", "failed"] = Field(..., description="Stage status")
    message: str = Field(..., description="Human-readable message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details")

    model_config = {
        "json_schema_extra": {
            "example": {
                "stage": "validate_sql",
                "status": "in_progress",
                "message": "Stage 1: Function validation...",
                "details": {"functions_found": 12, "suspicious": 2}
            }
        }
    }


class QueryResult(BaseModel):
    """Complete query execution result"""
    success: bool = Field(..., description="Whether execution succeeded")
    sql: Optional[str] = Field(None, description="Generated SQL query")
    ctas_table_name: Optional[str] = Field(None, description="Created CTAS table name")
    execution_id: Optional[str] = Field(None, description="Athena execution ID")
    s3_path: Optional[str] = Field(None, description="S3 path to full results")
    preview_data: Optional[List[Dict[str, Any]]] = Field(None, description="Preview rows (max 1000)")
    columns: Optional[List[str]] = Field(None, description="Column names")
    row_count: int = Field(default=0, description="Number of rows in preview")
    total_rows: Optional[int] = Field(None, description="Total rows in CTAS (if known)")
    bytes_scanned: int = Field(default=0, description="Data scanned in bytes")
    execution_time_ms: int = Field(default=0, description="Execution time in milliseconds")
    cache_hit: bool = Field(default=False, description="Whether result was from cache")
    cache_age_hours: Optional[float] = Field(None, description="Age of cache in hours")
    error: Optional[str] = Field(None, description="Error message if failed")
    rag_used: bool = Field(default=False, description="Whether RAG was used for validation/fixing")

    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "sql": "SELECT * FROM vehicle_path WHERE...",
                "ctas_table_name": "rule_wbl039_fastmap_20250117_143052",
                "execution_id": "abc-123-def",
                "s3_path": "s3://bucket/results/abc-123-def.csv",
                "preview_data": [{"id": "123", "geometry": "LINESTRING(...)"}],
                "columns": ["id", "geometry", "iso_country_code"],
                "row_count": 1000,
                "bytes_scanned": 50000000,
                "execution_time_ms": 15000,
                "cache_hit": False,
                "rag_used": True
            }
        }
    }


class CTASMetadata(BaseModel):
    """CTAS table metadata"""
    ctas_name: str = Field(..., description="CTAS table name")
    rule_category: str = Field(..., description="Rule category")
    database: str = Field(..., description="Database name")
    created_at: datetime = Field(..., description="Creation timestamp")
    row_count: int = Field(..., description="Number of rows")
    bytes_scanned: int = Field(..., description="Data scanned in bytes")
    execution_time_ms: int = Field(..., description="Execution time in milliseconds")
    created_by: Optional[str] = Field(None, description="Username who created it")

    model_config = {
        "json_schema_extra": {
            "example": {
                "ctas_name": "rule_wbl039_fastmap_20250117_143052",
                "rule_category": "WBL039",
                "database": "fastmap_prod2_v2_13_base",
                "created_at": "2025-01-17T14:30:52Z",
                "row_count": 5234,
                "bytes_scanned": 50000000,
                "execution_time_ms": 15000,
                "created_by": "john.doe"
            }
        }
    }


class UserQueryHistory(BaseModel):
    """User's query history entry"""
    id: int = Field(..., description="Query ID")
    rule_category: str = Field(..., description="Rule category")
    nl_query: str = Field(..., description="Natural language query")
    sql: str = Field(..., description="Generated SQL")
    ctas_name: Optional[str] = Field(None, description="CTAS table name")
    timestamp: datetime = Field(..., description="Query timestamp")
    bookmarked: bool = Field(default=False, description="Whether query is bookmarked")

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 123,
                "rule_category": "WBL039",
                "nl_query": "Vehicle paths outside lane groups",
                "sql": "SELECT * FROM...",
                "ctas_name": "rule_wbl039_fastmap_20250117_143052",
                "timestamp": "2025-01-17T14:30:52Z",
                "bookmarked": True
            }
        }
    }
