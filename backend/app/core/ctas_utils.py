"""
Utility functions for CTAS table management.
"""
from datetime import datetime
import re


def generate_ctas_name(rule_category: str, database: str) -> str:
    """
    Generate CTAS table name with date and time.
    
    Format: rule_{category}_{database}_{YYYYMMDD_HHMMSS}
    Example: rule_wbl039_fastmap_prod2_v2_13_base_20250114_143052
    
    Args:
        rule_category: Rule category (e.g., "WBL039")
        database: Database name (e.g., "fastmap_prod2_v2_13_base")
    
    Returns:
        Full CTAS table name with database prefix
    """
    # Normalize rule category (lowercase, remove special chars)
    category_clean = re.sub(r'[^a-z0-9_]', '', rule_category.lower())
    
    # Extract database name without catalog
    if '.' in database:
        db_clean = database.split('.')[-1]
    else:
        db_clean = database
    
    # Get current date (YYYYMMDD)
    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Build CTAS name
    ctas_name = f"rule_{category_clean}_{db_clean}_{date_str}"
    
    # Add database prefix
    full_name = f"{database}.{ctas_name}"
    print("###########CTAS name was generated#########")
    
    return full_name


def validate_ctas_name(ctas_name: str) -> bool:
    """
    Validate CTAS table name format.
    
    Expected: database.rule_category_database_YYYYMMDD
    """
    pattern = r'^[a-z0-9_]+\.rule_[a-z0-9_]+_[a-z0-9_]+_\d{8}$'
    return bool(re.match(pattern, ctas_name))


def extract_ctas_metadata(ctas_name: str) -> dict:
    """
    Extract metadata from CTAS table name.
    
    Example: 
        Input: "fastmap.rule_wbl039_fastmap_prod2_v2_13_base_20250114"
        Output: {
            'database': 'fastmap',
            'rule_category': 'wbl039',
            'date': '20250114'
        }
    """
    try:
        # Split by dot
        parts = ctas_name.split('.')
        if len(parts) != 2:
            return {}
        
        database = parts[0]
        table_part = parts[1]
        
        # Extract components
        # Format: rule_{category}_{db}_{date}
        match = re.match(r'rule_([a-z0-9_]+)_([a-z0-9_]+)_(\d{8})$', table_part)
        
        if not match:
            return {}
        
        return {
            'database': database,
            'rule_category': match.group(1),
            'date': match.group(3)
        }
    except:
        return {}


def format_ctas_date(date_str: str) -> str:
    """
    Format CTAS date for display.
    
    Input: "20250114"
    Output: "2025-01-14"
    """
    try:
        dt = datetime.strptime(date_str, '%Y%m%d')
        return dt.strftime('%Y-%m-%d')
    except:
        return date_str