from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class QueryRequest(BaseModel):
    """Request to execute a query."""
    database: str = Field(..., description="The Athena database to query")
    query: str = Field(..., description="SQL query to execute")
    max_rows: int = Field(1000, ge=1, le=10000, description="Maximum rows to return")

class QueryState(str, Enum):
    """Possible states of an Athena query."""
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"

class QueryStatus(BaseModel):
    """Status of a query execution."""
    query_execution_id: str
    state: QueryState
    state_change_reason: Optional[str] = None
    bytes_scanned: int = 0
    execution_time_ms: int = 0

class QueryResult(BaseModel):
    """Results of a successful query execution."""
    query_execution_id: str
    columns: List[str]
    rows: List[Dict[str, Any]]
    bytes_scanned: int
    execution_time_ms: int

class TableInfo(BaseModel):
    """Information about a database table."""
    database: str
    table_name: str
    columns: List[Dict[str, str]]

class DatabaseInfo(BaseModel):
    """Information about a database."""
    database: str
    tables: List[str]
    table_count: int