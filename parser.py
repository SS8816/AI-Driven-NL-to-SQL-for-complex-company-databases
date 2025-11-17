import re
from typing import Dict, List
import json

class NestedSchemaParser:
    """
    Handles deeply nested AWS Athena/Hive schema parsing.
    Extracts columns with their full type definitions including nested structures.
    """
    
    def __init__(self, schema_text: str):
        self.schema_text = schema_text
        self.tables = {}
    
    def parse(self) -> Dict[str, List[Dict]]:
        """
        Parse CREATE EXTERNAL TABLE statements with deeply nested types.
        """
        table_pattern = r"CREATE EXTERNAL TABLE `?([^`\s]+)`?\s*\((.*?)\)(?:\s*PARTITIONED BY|\s*ROW FORMAT|\s*STORED AS)"
        matches = re.finditer(table_pattern, self.schema_text, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            table_name = match.group(1)
            columns_text = match.group(2)
            
            columns = self._parse_columns(columns_text)
            self.tables[table_name] = columns
        
        return self.tables
    
    def _parse_columns(self, columns_text: str) -> List[Dict]:
        """
        Parse column definitions, handling deeply nested struct/array types.
        """
        columns = []
        columns_text = re.sub(r'--.*', '', columns_text)
        column_defs = self._split_columns(columns_text)
        
        for col_def in column_defs:
            col_def = col_def.strip()
            if not col_def:
                continue
            
            match = re.match(r'`?([^`\s]+)`?\s+(.+)', col_def, re.DOTALL)
            if match:
                col_name = match.group(1).strip()
                col_type = match.group(2).strip().rstrip(',')
                
                column_info = {
                    "column_name": col_name,
                    "full_type": col_type,
                    "is_nested": self._is_nested_type(col_type)
                }
                
                if column_info["is_nested"] and col_type.startswith("struct<"):
                    column_info["nested_fields"] = self._extract_nested_field_names(col_type)
                
                columns.append(column_info)
        
        return columns
    
    def _split_columns(self, text: str) -> List[str]:
        """
        Split column definitions by commas, but respect nested structures.
        """
        columns = []
        current = []
        depth = 0
        
        for char in text:
            if char in '<([':
                depth += 1
            elif char in '>)]':
                depth -= 1
            elif char == ',' and depth == 0:
                columns.append(''.join(current))
                current = []
                continue
            
            current.append(char)
        
        if current:
            columns.append(''.join(current))
        
        return columns
    
    def _is_nested_type(self, col_type: str) -> bool:
        """Check if column type is nested (struct, array, map)."""
        return any(col_type.strip().startswith(t) for t in ['struct<', 'array<', 'map<'])
    
    def _extract_nested_field_names(self, struct_type: str) -> List[str]:
        """
        Extract top-level field names from a struct type.
        """
        inner = struct_type[7:-1]
        fields = []
        depth = 0
        current_field = []
        
        for char in inner:
            if char in '<([':
                depth += 1
            elif char in '>)]':
                depth -= 1
            elif char == ',' and depth == 0:
                field_def = ''.join(current_field).strip()
                if ':' in field_def:
                    field_name = field_def.split(':')[0].strip('` ')
                    fields.append(field_name)
                current_field = []
                continue
            
            current_field.append(char)
        
        if current_field:
            field_def = ''.join(current_field).strip()
            if ':' in field_def:
                field_name = field_def.split(':')[0].strip('` ')
                fields.append(field_name)
        
        return fields

    # ✅ MOVED INSIDE THE CLASS to be used as a method
    def create_llm_summary(self) -> str:
        """
        Create a simplified, LLM-friendly schema summary for entity extraction.
        Shows structure without overwhelming detail.
        """
        summary = []
        for table_name, columns in self.tables.items():
            summary.append(f"TABLE: {table_name}")
            summary.append("COLUMNS:")
            
            for col in columns:
                if col["is_nested"]:
                    if col.get("nested_fields"):
                        nested_preview = ", ".join(col["nested_fields"][:5])
                        if len(col["nested_fields"]) > 5:
                            nested_preview += ", ..."
                        summary.append(f"  - {col['column_name']} (nested: {nested_preview})")
                    else:
                        summary.append(f"  - {col['column_name']} (complex nested type)")
                else:
                    summary.append(f"  - {col['column_name']} ({col['full_type']})")
            
            summary.append("")
        
        return "\n".join(summary)

    def get_full_ddl_for_columns(self, table_name: str, selected_columns: List[str]) -> str:
        """
        Generate a DDL statement with full type definitions for selected columns.
        This is what you'd pass to SQL generation.
        """
        if table_name not in self.tables:
            return ""
        
        all_columns = self.tables[table_name]
        selected_col_defs = [
            col for col in all_columns 
            if col["column_name"] in selected_columns
        ]
        
        ddl = f"CREATE EXTERNAL TABLE `{table_name}` (\n"
        col_lines = [f"  `{col['column_name']}` {col['full_type']}" for col in selected_col_defs]
        ddl += ",\n".join(col_lines)
        ddl += "\n);"
        
        return ddl