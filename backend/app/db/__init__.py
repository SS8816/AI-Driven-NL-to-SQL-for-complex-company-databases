"""
Database Package
Data access layer for user queries and history
"""

from app.db.database import db
from app.db.user_queries import user_queries_repo

__all__ = ["db", "user_queries_repo"]
