"""
Schema Service
Wraps the schema parser and provides schema-related operations
"""

from typing import List, Dict, Optional
from pathlib import Path
import json

from openai import AzureOpenAI

from app.config import settings
from app.utils.logger import app_logger
from app.utils.errors import SchemaNotFoundError, ValidationError
from app.models.schema import ColumnInfo, TableInfo, SchemaInfo, SchemaListResponse, SchemaListItem, SchemaSummary
from app.core.parser import NestedSchemaParser


class SchemaService:
    """Service for managing database schemas"""

    def __init__(self):
        self.schemas_dir = settings.schemas_path
        self.azure_config = {
            "api_key": settings.AZURE_OPENAI_API_KEY,
            "api_version": settings.AZURE_OPENAI_API_VERSION,
            "azure_endpoint": settings.AZURE_OPENAI_ENDPOINT,
        }

    def list_schemas(self) -> SchemaListResponse:
        """
        List all available database schemas

        Returns:
            SchemaListResponse with list of schema objects
        """
        try:
            if not self.schemas_dir.exists():
                app_logger.error("schemas_directory_not_found", path=str(self.schemas_dir))
                raise SchemaNotFoundError("Schemas directory not found")

            # Find all .txt schema files
            schema_files = list(self.schemas_dir.glob("*.txt"))

            schema_items = []
            for schema_file in sorted(schema_files):
                schema_name = schema_file.stem

                # Quick parse to get table count
                try:
                    with open(schema_file, "r", encoding="utf-8") as f:
                        schema_ddl = f.read()

                    parser = NestedSchemaParser(schema_ddl)
                    tables = parser.parse()
                    table_count = len(tables)
                except Exception as e:
                    app_logger.warning(f"Failed to parse {schema_name}", error=str(e))
                    table_count = 0

                # Extract database name from schema name (usually before .latest_ or similar)
                database = schema_name.split('.')[0] if '.' in schema_name else "awsdatacatalog"

                schema_items.append(SchemaListItem(
                    name=schema_name,
                    database=database,
                    table_count=table_count
                ))

            app_logger.info(
                "schemas_listed",
                count=len(schema_items),
                schemas=[s.name for s in schema_items[:5]]  # Log first 5
            )

            return SchemaListResponse(
                schemas=schema_items,
                count=len(schema_items)
            )

        except Exception as e:
            app_logger.error("list_schemas_error", error=str(e))
            raise

    def get_schema_info(self, schema_name: str) -> SchemaInfo:
        """
        Get detailed information about a specific schema

        Args:
            schema_name: Name of the schema

        Returns:
            SchemaInfo with tables and columns

        Raises:
            SchemaNotFoundError: If schema doesn't exist
        """
        try:
            # Load schema file
            schema_path = self.schemas_dir / f"{schema_name}.txt"
            if not schema_path.exists():
                raise SchemaNotFoundError(schema_name)

            app_logger.info("loading_schema", schema_name=schema_name, path=str(schema_path))

            # Read schema DDL
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_ddl = f.read()

            # Parse schema
            parser = NestedSchemaParser(schema_ddl)
            tables = parser.parse()

            # Convert to API models
            table_infos = []
            total_columns = 0

            for table_name, columns in tables.items():
                column_infos = [
                    ColumnInfo(
                        column_name=col["column_name"],
                        full_type=col["full_type"],
                        is_nested=col["is_nested"],
                        nested_fields=col.get("nested_fields")
                    )
                    for col in columns
                ]

                table_infos.append(
                    TableInfo(
                        table_name=table_name,
                        columns=column_infos,
                        column_count=len(column_infos)
                    )
                )
                total_columns += len(column_infos)

            app_logger.info(
                "schema_loaded",
                schema_name=schema_name,
                table_count=len(table_infos),
                total_columns=total_columns
            )

            return SchemaInfo(
                schema_name=schema_name,
                tables=table_infos,
                table_count=len(table_infos),
                total_columns=total_columns
            )

        except SchemaNotFoundError:
            raise
        except Exception as e:
            app_logger.error("get_schema_error", schema_name=schema_name, error=str(e))
            raise

    def get_schema_summary(self, schema_name: str) -> SchemaSummary:
        """
        Get LLM-friendly schema summary (simplified)

        Args:
            schema_name: Name of the schema

        Returns:
            SchemaSummary with text summary and token count

        Raises:
            SchemaNotFoundError: If schema doesn't exist
        """
        try:
            # Load schema file
            schema_path = self.schemas_dir / f"{schema_name}.txt"
            if not schema_path.exists():
                raise SchemaNotFoundError(schema_name)

            # Read schema DDL
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_ddl = f.read()

            # Parse and create summary
            parser = NestedSchemaParser(schema_ddl)
            parser.parse()
            summary = parser.create_llm_summary()

            # Estimate token count (rough: 1 token ≈ 4 chars)
            token_count = len(summary) // 4

            app_logger.info(
                "schema_summary_created",
                schema_name=schema_name,
                summary_length=len(summary),
                token_count=token_count
            )

            return SchemaSummary(
                schema_name=schema_name,
                summary=summary,
                token_count=token_count
            )

        except SchemaNotFoundError:
            raise
        except Exception as e:
            app_logger.error("get_schema_summary_error", schema_name=schema_name, error=str(e))
            raise

    def extract_entities(
        self,
        schema_name: str,
        nl_query: str
    ) -> Dict[str, List[str]]:
        """
        Use LLM to extract relevant tables/columns from natural language query

        Args:
            schema_name: Name of the schema
            nl_query: Natural language query

        Returns:
            Dict mapping table names to column lists

        Raises:
            SchemaNotFoundError: If schema doesn't exist
            ValidationError: If LLM response is invalid
        """
        try:
            # Get schema summary
            summary_result = self.get_schema_summary(schema_name)
            summary = summary_result.summary

            # Build prompt for LLM
            prompt = f"""You are a database schema analyzer. Given a simplified database schema summary and a natural language query,
identify the REQUIRED set of tables and columns needed to answer the query.

DATABASE SCHEMA SUMMARY:
{summary}

USER QUERY:
{nl_query}

Instructions:
- Be selective: only include tables and columns DIRECTLY needed for the query.
- For nested columns (e.g., "geometry (nested: type, coordinates)"), you MUST only include the parent column name (e.g., "geometry"). Do NOT include the nested fields like "geometry.type".

  **From ANY table that has them, ALWAYS include:**
  - `iso_country_code` (enables filtering by country)
  - `id` (always needed for identification)
  - `geometry` (needed for geospatial operations)

  **These context columns allow users to filter CTAS results by location after execution.**

- Return ONLY valid JSON in the exact format below.

{{
    "tables": {{
        "table_name": ["column1", "column2"],
        "another_table": ["column1"]
    }},
    "reasoning": "A brief explanation of why these tables and parent columns were selected."
}}"""

            app_logger.info(
                "llm_entity_extraction_start",
                schema_name=schema_name,
                query_length=len(nl_query)
            )

            # Call Azure OpenAI
            client = AzureOpenAI(
                api_key=self.azure_config["api_key"],
                api_version=self.azure_config["api_version"],
                azure_endpoint=self.azure_config["azure_endpoint"],
            )

            response = client.chat.completions.create(
                model=settings.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a database expert. Always return valid JSON.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=1,
                response_format={"type": "json_object"},
            )

            # Parse response
            result_text = response.choices[0].message.content
            result = json.loads(result_text)

            tables = result.get("tables", {})
            reasoning = result.get("reasoning", "")

            app_logger.info(
                "llm_entity_extraction_complete",
                schema_name=schema_name,
                tables_extracted=len(tables),
                reasoning=reasoning[:100]
            )

            return {
                "tables": tables,
                "reasoning": reasoning
            }

        except SchemaNotFoundError:
            raise
        except json.JSONDecodeError as e:
            app_logger.error("llm_response_invalid_json", error=str(e))
            raise ValidationError("LLM returned invalid JSON")
        except Exception as e:
            app_logger.error("extract_entities_error", schema_name=schema_name, error=str(e))
            raise

    def get_full_ddl_for_columns(
        self,
        schema_name: str,
        selected_tables: Dict[str, List[str]]
    ) -> tuple[str, str]:
        """
        Generate full DDL and schema summary for selected tables/columns

        Args:
            schema_name: Name of the schema
            selected_tables: Dict mapping table names to column lists

        Returns:
            Tuple of (full_ddl, schema_summary)
            - full_ddl: Complete DDL string for selected columns
            - schema_summary: Token-optimized summary showing nested field counts

        Raises:
            SchemaNotFoundError: If schema doesn't exist
        """
        try:
            # Load schema file
            schema_path = self.schemas_dir / f"{schema_name}.txt"
            if not schema_path.exists():
                raise SchemaNotFoundError(schema_name)

            # Read schema DDL
            with open(schema_path, "r", encoding="utf-8") as f:
                schema_ddl = f.read()

            # Parse schema
            parser = NestedSchemaParser(schema_ddl)
            parser.parse()

            # Build DDL for each table
            ddl_parts = []
            for table_name, columns in selected_tables.items():
                if table_name in parser.tables:
                    table_ddl = parser.get_full_ddl_for_columns(table_name, columns)
                    ddl_parts.append(table_ddl)

            full_ddl = "\n\n".join(ddl_parts)

            # Generate schema summary (once, to be reused in validation/fixing)
            schema_summary = parser.create_llm_summary()

            app_logger.info(
                "ddl_and_summary_generated",
                schema_name=schema_name,
                table_count=len(selected_tables),
                ddl_length=len(full_ddl),
                summary_length=len(schema_summary)
            )

            return full_ddl, schema_summary

        except SchemaNotFoundError:
            raise
        except Exception as e:
            app_logger.error("get_full_ddl_error", schema_name=schema_name, error=str(e))
            raise


# Global instance
schema_service = SchemaService()
