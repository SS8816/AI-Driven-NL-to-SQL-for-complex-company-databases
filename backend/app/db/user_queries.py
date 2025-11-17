"""
User Queries Repository
CRUD operations for user query history
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from app.db.database import db
from app.utils.logger import app_logger
from app.models.query import UserQueryHistory


class UserQueriesRepository:
    """Repository for user query history"""

    async def create_or_update_user(
        self,
        username: str,
        email: str,
        display_name: str
    ) -> int:
        """
        Create or update user record

        Args:
            username: Username
            email: Email address
            display_name: Display name

        Returns:
            User ID
        """
        try:
            async with db.get_connection() as conn:
                # Try to get existing user
                cursor = await conn.execute(
                    "SELECT id FROM users WHERE username = ?",
                    (username,)
                )
                row = await cursor.fetchone()

                if row:
                    # Update last_login
                    user_id = row[0]
                    await conn.execute(
                        """UPDATE users
                           SET email = ?, display_name = ?, last_login = CURRENT_TIMESTAMP
                           WHERE id = ?""",
                        (email, display_name, user_id)
                    )
                else:
                    # Create new user
                    cursor = await conn.execute(
                        """INSERT INTO users (username, email, display_name)
                           VALUES (?, ?, ?)""",
                        (username, email, display_name)
                    )
                    user_id = cursor.lastrowid

                await conn.commit()

                app_logger.info("user_record_updated", username=username, user_id=user_id)
                return user_id

        except Exception as e:
            app_logger.error("create_update_user_error", username=username, error=str(e))
            raise

    async def get_user_id(self, username: str) -> Optional[int]:
        """Get user ID by username"""
        try:
            async with db.get_connection() as conn:
                cursor = await conn.execute(
                    "SELECT id FROM users WHERE username = ?",
                    (username,)
                )
                row = await cursor.fetchone()
                return row[0] if row else None

        except Exception as e:
            app_logger.error("get_user_id_error", username=username, error=str(e))
            raise

    async def save_query(
        self,
        username: str,
        rule_category: str,
        nl_query: str,
        sql: Optional[str],
        ctas_name: Optional[str],
        execution_id: Optional[str],
        status: str,
        error_message: Optional[str] = None,
        execution_time_ms: int = 0,
        bytes_scanned: int = 0,
        row_count: int = 0
    ) -> int:
        """
        Save query execution to history

        Args:
            username: Username
            rule_category: Rule category
            nl_query: Natural language query
            sql: Generated SQL
            ctas_name: CTAS table name
            execution_id: Athena execution ID
            status: Execution status (success/failed)
            error_message: Error message if failed
            execution_time_ms: Execution time in milliseconds
            bytes_scanned: Data scanned in bytes
            row_count: Number of rows returned

        Returns:
            Query ID
        """
        try:
            # Get or create user
            user_id = await self.get_user_id(username)
            if not user_id:
                # User should exist from auth, but create if missing
                user_id = await self.create_or_update_user(username, f"{username}@here.com", username)

            async with db.get_connection() as conn:
                cursor = await conn.execute(
                    """INSERT INTO user_queries (
                        user_id, rule_category, nl_query, sql, ctas_name,
                        execution_id, status, error_message,
                        execution_time_ms, bytes_scanned, row_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        user_id, rule_category, nl_query, sql, ctas_name,
                        execution_id, status, error_message,
                        execution_time_ms, bytes_scanned, row_count
                    )
                )

                query_id = cursor.lastrowid
                await conn.commit()

                app_logger.info(
                    "query_saved",
                    username=username,
                    query_id=query_id,
                    rule_category=rule_category,
                    status=status
                )

                return query_id

        except Exception as e:
            app_logger.error("save_query_error", username=username, error=str(e))
            raise

    async def get_user_history(
        self,
        username: str,
        limit: int = 50,
        offset: int = 0,
        bookmarked_only: bool = False
    ) -> List[UserQueryHistory]:
        """
        Get user's query history

        Args:
            username: Username
            limit: Maximum number of results
            offset: Offset for pagination
            bookmarked_only: Only return bookmarked queries

        Returns:
            List of UserQueryHistory objects
        """
        try:
            user_id = await self.get_user_id(username)
            if not user_id:
                return []

            async with db.get_connection() as conn:
                conn.row_factory = lambda cursor, row: {
                    col[0]: row[idx] for idx, col in enumerate(cursor.description)
                }

                if bookmarked_only:
                    query = """
                        SELECT id, rule_category, nl_query, sql, ctas_name, timestamp, bookmarked
                        FROM user_queries
                        WHERE user_id = ? AND bookmarked = 1
                        ORDER BY timestamp DESC
                        LIMIT ? OFFSET ?
                    """
                else:
                    query = """
                        SELECT id, rule_category, nl_query, sql, ctas_name, timestamp, bookmarked
                        FROM user_queries
                        WHERE user_id = ?
                        ORDER BY timestamp DESC
                        LIMIT ? OFFSET ?
                    """

                cursor = await conn.execute(query, (user_id, limit, offset))
                rows = await cursor.fetchall()

                history = [
                    UserQueryHistory(
                        id=row["id"],
                        rule_category=row["rule_category"],
                        nl_query=row["nl_query"],
                        sql=row["sql"] or "",
                        ctas_name=row["ctas_name"],
                        timestamp=datetime.fromisoformat(row["timestamp"]),
                        bookmarked=bool(row["bookmarked"])
                    )
                    for row in rows
                ]

                app_logger.info(
                    "user_history_retrieved",
                    username=username,
                    count=len(history),
                    bookmarked_only=bookmarked_only
                )

                return history

        except Exception as e:
            app_logger.error("get_user_history_error", username=username, error=str(e))
            raise

    async def toggle_bookmark(self, username: str, query_id: int) -> bool:
        """
        Toggle bookmark status for a query

        Args:
            username: Username
            query_id: Query ID

        Returns:
            New bookmark status (True if bookmarked, False if unbookmarked)
        """
        try:
            user_id = await self.get_user_id(username)
            if not user_id:
                raise ValueError(f"User not found: {username}")

            async with db.get_connection() as conn:
                # Get current bookmark status
                cursor = await conn.execute(
                    "SELECT bookmarked FROM user_queries WHERE id = ? AND user_id = ?",
                    (query_id, user_id)
                )
                row = await cursor.fetchone()

                if not row:
                    raise ValueError(f"Query not found or doesn't belong to user: {query_id}")

                # Toggle bookmark
                current_status = row[0]
                new_status = 0 if current_status else 1

                await conn.execute(
                    "UPDATE user_queries SET bookmarked = ? WHERE id = ?",
                    (new_status, query_id)
                )
                await conn.commit()

                app_logger.info(
                    "bookmark_toggled",
                    username=username,
                    query_id=query_id,
                    bookmarked=bool(new_status)
                )

                return bool(new_status)

        except Exception as e:
            app_logger.error("toggle_bookmark_error", username=username, query_id=query_id, error=str(e))
            raise


# Global repository instance
user_queries_repo = UserQueriesRepository()
