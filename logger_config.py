import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional


# Setup file logger
def setup_file_logger():
    """Configure file-based logging."""
    logger = logging.getLogger("NL2SQL_Logger")
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # File handler
    file_handler = logging.FileHandler("llm_interactions.log", mode='a')
    file_handler.setLevel(logging.INFO)
    
    # Format
    formatter = logging.Formatter(
        '%(asctime)s - [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    return logger


file_logger = setup_file_logger()


# Setup SQLite logger
class SQLiteLogger:
    """SQLite-based structured logging."""
    
    def __init__(self, db_path: str = "query_logs.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Create logging tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # LLM interactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS llm_interactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                step_name TEXT NOT NULL,
                prompt TEXT,
                response TEXT,
                context TEXT,
                token_count INTEGER
            )
        """)
        
        # Query executions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS query_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rule_category TEXT,
                database_name TEXT,
                nl_query TEXT,
                generated_sql TEXT,
                status TEXT NOT NULL,
                execution_id TEXT,
                error_message TEXT,
                bytes_scanned INTEGER,
                execution_time_ms INTEGER,
                row_count INTEGER
            )
        """)
        
        conn.commit()
        conn.close()
    
    def log_llm_interaction(
        self,
        step_name: str,
        prompt: Optional[str],
        response: Optional[str],
        context: Optional[str] = None
    ):
        """Log LLM prompt/response."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Estimate token count (rough: 1 token ≈ 4 chars)
        token_count = 0
        if prompt:
            token_count += len(prompt) // 4
        if response:
            token_count += len(response) // 4
        
        cursor.execute("""
            INSERT INTO llm_interactions (step_name, prompt, response, context, token_count)
            VALUES (?, ?, ?, ?, ?)
        """, (step_name, prompt, response, context, token_count))
        
        conn.commit()
        conn.close()
    
    def log_query_execution(
        self,
        rule_category: str,
        database: str,
        sql: str,
        status: str,
        nl_query: Optional[str] = None,
        execution_id: Optional[str] = None,
        error: Optional[str] = None,
        bytes_scanned: int = 0,
        execution_time_ms: int = 0,
        row_count: int = 0
    ):
        """Log query execution outcome."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO query_executions (
                rule_category, database_name, nl_query, generated_sql,
                status, execution_id, error_message,
                bytes_scanned, execution_time_ms, row_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rule_category, database, nl_query, sql,
            status, execution_id, error,
            bytes_scanned, execution_time_ms, row_count
        ))
        
        conn.commit()
        conn.close()


# Singleton instance
sql_logger = SQLiteLogger()


# Public API functions
def log_llm_interaction(step_name: str, prompt: Optional[str], response: Optional[str], context: Optional[str] = None):
    """
    Log LLM interaction to both file and SQLite.
    
    Args:
        step_name: Name of the step (e.g., "generate_sql_start", "fix_sql_complete")
        prompt: Full prompt sent to LLM (can be None for start events)
        response: LLM's response (can be None for start events)
        context: Additional context (e.g., NL query, retry number)
    """
    # File log
    if prompt and response:
        file_logger.info(f"=== {step_name} ===")
        file_logger.info(f"Context: {context}")
        file_logger.info(f"Prompt (first 500 chars): {prompt[:500]}...")
        file_logger.info(f"Response (first 500 chars): {response[:500]}...")
    else:
        file_logger.info(f"=== {step_name} === Context: {context}")
    
    # SQLite log (stores full prompt/response)
    sql_logger.log_llm_interaction(step_name, prompt, response, context)


def log_query_execution(
    rule_category: str,
    database: str,
    sql: str,
    status: str,
    nl_query: Optional[str] = None,
    execution_id: Optional[str] = None,
    error: Optional[str] = None,
    bytes_scanned: int = 0,
    execution_time_ms: int = 0,
    row_count: int = 0
):
    """
    Log query execution outcome to both file and SQLite.
    
    Args:
        rule_category: Rule category (e.g., "WBL039")
        database: Database name
        sql: Generated SQL query
        status: Status ("executing", "success", "failed", "timeout")
        nl_query: Natural language query
        execution_id: Athena execution ID
        error: Error message if failed
        bytes_scanned: Data scanned in bytes
        execution_time_ms: Execution time in milliseconds
        row_count: Number of rows returned
    """
    # File log
    file_logger.info(f"=== QUERY EXECUTION: {rule_category} on {database} ===")
    file_logger.info(f"Status: {status}")
    file_logger.info(f"SQL (first 300 chars): {sql[:300]}...")
    if error:
        file_logger.error(f"Error: {error}")
    if execution_id:
        file_logger.info(f"Execution ID: {execution_id}")
    
    # SQLite log
    sql_logger.log_query_execution(
        rule_category=rule_category,
        database=database,
        sql=sql,
        status=status,
        nl_query=nl_query,
        execution_id=execution_id,
        error=error,
        bytes_scanned=bytes_scanned,
        execution_time_ms=execution_time_ms,
        row_count=row_count
    )


def get_recent_logs(limit: int = 50):
    """Retrieve recent logs from SQLite."""
    conn = sqlite3.connect(sql_logger.db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM query_executions
        ORDER BY timestamp DESC
        LIMIT ?
    """, (limit,))
    
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(row) for row in rows]


def get_query_statistics():
    """Get aggregate statistics from logs."""
    conn = sqlite3.connect(sql_logger.db_path)
    cursor = conn.cursor()
    
    # Total queries
    cursor.execute("SELECT COUNT(*) FROM query_executions")
    total = cursor.fetchone()[0]
    
    # Success rate
    cursor.execute("SELECT COUNT(*) FROM query_executions WHERE status = 'success'")
    successful = cursor.fetchone()[0]
    
    # Average execution time
    cursor.execute("SELECT AVG(execution_time_ms) FROM query_executions WHERE status = 'success'")
    avg_time = cursor.fetchone()[0] or 0
    
    # Total data scanned
    cursor.execute("SELECT SUM(bytes_scanned) FROM query_executions WHERE status = 'success'")
    total_bytes = cursor.fetchone()[0] or 0
    
    conn.close()
    
    return {
        'total_queries': total,
        'successful_queries': successful,
        'failed_queries': total - successful,
        'success_rate': (successful / total * 100) if total > 0 else 0,
        'avg_execution_time_ms': avg_time,
        'total_data_scanned_gb': total_bytes / (1024**3)
    }