"""
Database Layer
Handles SQLite database connections and table creation
"""

import sqlite3
import aiosqlite
from typing import Optional
from pathlib import Path

from app.config import settings
from app.utils.logger import app_logger


class Database:
    """Database connection manager"""

    def __init__(self, db_path: str = "app_data.db"):
        self.db_path = Path(db_path)
        self._initialized = False

    async def initialize(self):
        """Initialize database and create tables if needed"""
        if self._initialized:
            return

        try:
            # Create database file if it doesn't exist
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # Create tables
            async with aiosqlite.connect(self.db_path) as db:
                # Users table (stores user info from auth)
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT UNIQUE NOT NULL,
                        email TEXT,
                        display_name TEXT,
                        last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # User queries table (query history)
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS user_queries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        rule_category TEXT NOT NULL,
                        nl_query TEXT NOT NULL,
                        sql TEXT,
                        ctas_name TEXT,
                        execution_id TEXT,
                        status TEXT NOT NULL,
                        error_message TEXT,
                        execution_time_ms INTEGER DEFAULT 0,
                        bytes_scanned INTEGER DEFAULT 0,
                        row_count INTEGER DEFAULT 0,
                        bookmarked INTEGER DEFAULT 0,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                """)

                # Create indexes
                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_user_queries_user_id
                    ON user_queries(user_id)
                """)

                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_user_queries_timestamp
                    ON user_queries(timestamp DESC)
                """)

                await db.execute("""
                    CREATE INDEX IF NOT EXISTS idx_user_queries_bookmarked
                    ON user_queries(user_id, bookmarked, timestamp DESC)
                """)

                await db.commit()

            self._initialized = True
            app_logger.info("database_initialized", path=str(self.db_path))

        except Exception as e:
            app_logger.error("database_init_error", error=str(e))
            raise

    async def get_connection(self) -> aiosqlite.Connection:
        """Get database connection"""
        if not self._initialized:
            await self.initialize()

        return await aiosqlite.connect(self.db_path)


# Global database instance
db = Database()
