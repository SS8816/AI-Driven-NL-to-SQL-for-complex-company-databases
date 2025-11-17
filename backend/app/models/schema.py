"""
Schema Models
Pydantic models for database schema information
"""

from typing import List, Dict, Optional
from pydantic import BaseModel, Field


class ColumnInfo(BaseModel):
    """Database column information"""
    column_name: str = Field(..., description="Column name")
    full_type: str = Field(..., description="Full type definition")
    is_nested: bool = Field(..., description="Whether column has nested structure")
    nested_fields: Optional[List[str]] = Field(None, description="Nested field names (for structs)")

    model_config = {
        "json_schema_extra": {
            "example": {
                "column_name": "geometry",
                "full_type": "struct<type:varchar, coordinates:array<array<double>>>",
                "is_nested": True,
                "nested_fields": ["type", "coordinates"]
            }
        }
    }


class TableInfo(BaseModel):
    """Database table information"""
    table_name: str = Field(..., description="Table name")
    columns: List[ColumnInfo] = Field(..., description="List of columns")
    column_count: int = Field(..., description="Number of columns")

    model_config = {
        "json_schema_extra": {
            "example": {
                "table_name": "vehicle_path",
                "columns": [
                    {
                        "column_name": "id",
                        "full_type": "varchar",
                        "is_nested": False
                    },
                    {
                        "column_name": "geometry",
                        "full_type": "struct<type:varchar, coordinates:array<array<double>>>",
                        "is_nested": True,
                        "nested_fields": ["type", "coordinates"]
                    }
                ],
                "column_count": 2
            }
        }
    }


class SchemaInfo(BaseModel):
    """Complete schema information"""
    schema_name: str = Field(..., description="Schema/catalog name")
    tables: List[TableInfo] = Field(..., description="List of tables")
    table_count: int = Field(..., description="Number of tables")
    total_columns: int = Field(..., description="Total columns across all tables")

    model_config = {
        "json_schema_extra": {
            "example": {
                "schema_name": "fastmap_prod2_v2_13_base",
                "tables": [
                    {
                        "table_name": "vehicle_path",
                        "columns": [],
                        "column_count": 15
                    }
                ],
                "table_count": 12,
                "total_columns": 245
            }
        }
    }


class SchemaListResponse(BaseModel):
    """List of available schemas"""
    schemas: List[str] = Field(..., description="Available schema names")
    count: int = Field(..., description="Number of schemas")

    model_config = {
        "json_schema_extra": {
            "example": {
                "schemas": [
                    "fastmap_prod2_v2_13_base",
                    "fastmap_violations_prod_v4_base"
                ],
                "count": 2
            }
        }
    }


class SchemaSummary(BaseModel):
    """Simplified schema summary for LLM"""
    schema_name: str = Field(..., description="Schema name")
    summary: str = Field(..., description="Human-readable schema summary")
    token_count: int = Field(..., description="Estimated token count")

    model_config = {
        "json_schema_extra": {
            "example": {
                "schema_name": "fastmap_prod2_v2_13_base",
                "summary": "TABLE: vehicle_path\\nCOLUMNS:\\n  - id (varchar)\\n  - geometry (nested: type, coordinates)\\n...",
                "token_count": 3000
            }
        }
    }
