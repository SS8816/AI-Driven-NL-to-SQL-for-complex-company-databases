import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict
from pathlib import Path


class CacheManager:
    """Manages query result caching using SQLite + S3 + CTAS."""
    
    def __init__(self, db_path: str = "query_cache.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Create cache table if not exists."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS query_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_category TEXT NOT NULL,
                database_name TEXT NOT NULL,
                nl_query_text TEXT NOT NULL,
                final_sql TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                s3_result_path TEXT NOT NULL,
                ctas_table_name TEXT,
                execution_type TEXT DEFAULT 'direct',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count INTEGER,
                bytes_scanned INTEGER,
                execution_time_ms INTEGER,
                UNIQUE(rule_category, database_name)
            )
        """)
        
        # Create index for faster lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cache_lookup 
            ON query_cache(rule_category, database_name)
        """)
        
        conn.commit()
        conn.close()
    
    def _normalize_rule_category(self, rule_category: str) -> str:
        """Normalize rule category to uppercase for consistent matching."""
        return rule_category.strip().upper()
    
    def get_cached_result(
        self,
        rule_category: str,
        database: str,
        nl_query: str
    ) -> Optional[Dict]:
        """
        Check if query result is cached and still valid.
        
        Matches on: normalized rule_category + database_name only.
        NL query text is ignored for matching (allows variations).
        
        Returns cached result dict if hit, None if miss.
        """
        normalized_category = self._normalize_rule_category(rule_category)
        
        # Debug logging
        print(f"[CACHE DEBUG] Looking for cache:")
        print(f"  Rule Category (normalized): {normalized_category}")
        print(f"  Database: {database}")
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM query_cache
            WHERE rule_category = ? 
              AND database_name = ? 
            ORDER BY created_at DESC
            LIMIT 1
        """, (normalized_category, database))
        
        row = cursor.fetchone()
        
        if not row:
            print(f"[CACHE DEBUG] No cache entry found")
            conn.close()
            return None
        
        # Check if cache is still valid (1 week = 168 hours)
        created_at = datetime.fromisoformat(row['created_at'])
        age = datetime.now() - created_at
        age_hours = age.total_seconds() / 3600
        
        print(f"[CACHE DEBUG] Found cache entry, age: {age_hours:.1f} hours")
        
        if age_hours > 168:  # 1 week
            print(f"[CACHE DEBUG] Cache expired (>{168} hours)")
            conn.close()
            return None
        
        print(f"[CACHE DEBUG] Cache HIT! Returning cached result")
        conn.close()
        
        return {
            'sql': row['final_sql'],
            'execution_id': row['execution_id'],
            's3_path': row['s3_result_path'],
            'ctas_table_name': row['ctas_table_name'],
            'execution_type': row['execution_type'],
            'row_count': row['row_count'],
            'bytes_scanned': row['bytes_scanned'],
            'execution_time_ms': row['execution_time_ms'],
            'age_hours': age_hours,
            'created_at': created_at,
            'original_query': row['nl_query_text']
        }
    
    def cache_result(
        self,
        rule_category: str,
        database: str,
        nl_query: str,
        sql: str,
        execution_id: str,
        s3_path: str,
        ctas_table_name: Optional[str],
        execution_type: str,
        bytes_scanned: int,
        execution_time_ms: int,
        row_count: int
    ):
        """
        Store successful query result in cache.
        
        Uses normalized rule_category for storage.
        execution_type: 'direct' or 'ctas'
        """
        normalized_category = self._normalize_rule_category(rule_category)
        
        print(f"[CACHE DEBUG] Storing in cache:")
        print(f"  Rule Category (normalized): {normalized_category}")
        print(f"  Database: {database}")
        print(f"  Execution ID: {execution_id}")
        print(f"  CTAS Table: {ctas_table_name}")
        print(f"  Execution Type: {execution_type}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO query_cache (
                    rule_category, database_name, nl_query_text,
                    final_sql, execution_id, s3_result_path,
                    ctas_table_name, execution_type,
                    row_count, bytes_scanned, execution_time_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                normalized_category, database, nl_query,
                sql, execution_id, s3_path,
                ctas_table_name, execution_type,
                row_count, bytes_scanned, execution_time_ms
            ))
            
            conn.commit()
            print(f"[CACHE DEBUG] Successfully cached result")
        except Exception as e:
            print(f"[CACHE DEBUG] Error caching result: {str(e)}")
            raise
        finally:
            conn.close()
    
    def clear_expired_cache(self):
        """Remove cache entries older than 1 week."""
        cutoff = datetime.now() - timedelta(weeks=1)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM query_cache
            WHERE created_at < ?
        """, (cutoff.isoformat(),))
        
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        
        return deleted
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM query_cache")
        total = cursor.fetchone()[0]
        
        cutoff = datetime.now() - timedelta(weeks=1)
        cursor.execute("""
            SELECT COUNT(*) FROM query_cache
            WHERE created_at >= ?
        """, (cutoff.isoformat(),))
        valid = cursor.fetchone()[0]
        
        # Count CTAS vs direct executions
        cursor.execute("""
            SELECT execution_type, COUNT(*) 
            FROM query_cache 
            GROUP BY execution_type
        """)
        type_counts = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            'total_entries': total,
            'valid_entries': valid,
            'expired_entries': total - valid,
            'ctas_count': type_counts.get('ctas', 0),
            'direct_count': type_counts.get('direct', 0)
        }
    
    def invalidate_cache(self, rule_category: str, database: str):
        """
        Manually invalidate cache for specific rule + database combination.
        Useful when you know data has been updated.
        """
        normalized_category = self._normalize_rule_category(rule_category)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM query_cache
            WHERE rule_category = ? AND database_name = ?
        """, (normalized_category, database))
        
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        
        return deleted
    
    def get_all_cached_rules(self, database: Optional[str] = None) -> list:
        """
        Get list of all cached rules, optionally filtered by database.
        Useful for displaying cached rules to users.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if database:
            cursor.execute("""
                SELECT rule_category, database_name, created_at, 
                       ctas_table_name, execution_type,
                       substr(nl_query_text, 1, 100) as query_preview
                FROM query_cache
                WHERE database_name = ?
                ORDER BY created_at DESC
            """, (database,))
        else:
            cursor.execute("""
                SELECT rule_category, database_name, created_at,
                       ctas_table_name, execution_type,
                       substr(nl_query_text, 1, 100) as query_preview
                FROM query_cache
                ORDER BY created_at DESC
            """)
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_ctas_tables_for_cleanup(self, older_than_days: int = 7) -> list:
        """
        Get list of CTAS table names that are older than specified days.
        Used for cleanup operations.
        """
        cutoff = datetime.now() - timedelta(days=older_than_days)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ctas_table_name, database_name, created_at, rule_category
            FROM query_cache
            WHERE ctas_table_name IS NOT NULL
              AND execution_type = 'ctas'
              AND created_at < ?
            ORDER BY created_at ASC
        """, (cutoff.isoformat(),))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                'ctas_name': row[0],
                'database': row[1],
                'created_at': row[2],
                'rule_category': row[3]
            }
            for row in rows
        ]