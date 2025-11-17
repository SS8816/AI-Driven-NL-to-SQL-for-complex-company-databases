"""
Cache Service
Wraps the cache manager and provides cache operations
"""

from typing import List, Dict, Optional

from app.utils.logger import app_logger
from app.utils.errors import CacheError
from app.models.response import CacheStats
from app.models.query import CTASMetadata
from app.core.cache_manager import CacheManager


class CacheService:
    """Service for cache management operations"""

    def __init__(self):
        self.cache_manager = CacheManager()

    def get_cache_stats(self) -> CacheStats:
        """
        Get cache statistics

        Returns:
            CacheStats with cache metrics
        """
        try:
            stats = self.cache_manager.get_cache_stats()

            app_logger.info(
                "cache_stats_retrieved",
                total=stats["total_entries"],
                valid=stats["valid_entries"],
                expired=stats["expired_entries"]
            )

            # Calculate hit rate if we have tracking (future enhancement)
            hit_rate = None

            return CacheStats(
                total_entries=stats["total_entries"],
                valid_entries=stats["valid_entries"],
                expired_entries=stats["expired_entries"],
                ctas_count=stats["ctas_count"],
                direct_count=stats["direct_count"],
                hit_rate=hit_rate
            )

        except Exception as e:
            app_logger.error("get_cache_stats_error", error=str(e))
            raise CacheError("Failed to retrieve cache statistics")

    def clear_expired_cache(self) -> int:
        """
        Clear expired cache entries

        Returns:
            Number of entries deleted
        """
        try:
            deleted_count = self.cache_manager.clear_expired_cache()

            app_logger.info("cache_expired_cleared", deleted_count=deleted_count)

            return deleted_count

        except Exception as e:
            app_logger.error("clear_expired_cache_error", error=str(e))
            raise CacheError("Failed to clear expired cache")

    def invalidate_cache(self, rule_category: str, database: str) -> int:
        """
        Manually invalidate cache for specific rule + database

        Args:
            rule_category: Rule category to invalidate
            database: Database name

        Returns:
            Number of entries deleted
        """
        try:
            deleted_count = self.cache_manager.invalidate_cache(rule_category, database)

            app_logger.info(
                "cache_invalidated",
                rule_category=rule_category,
                database=database,
                deleted_count=deleted_count
            )

            return deleted_count

        except Exception as e:
            app_logger.error(
                "invalidate_cache_error",
                rule_category=rule_category,
                database=database,
                error=str(e)
            )
            raise CacheError(f"Failed to invalidate cache for {rule_category}/{database}")

    def list_cached_rules(self, database: Optional[str] = None) -> List[Dict]:
        """
        List all cached rules

        Args:
            database: Optional database filter

        Returns:
            List of cached rule info dicts
        """
        try:
            cached_rules = self.cache_manager.get_all_cached_rules(database)

            app_logger.info(
                "cached_rules_listed",
                count=len(cached_rules),
                database_filter=database
            )

            return cached_rules

        except Exception as e:
            app_logger.error("list_cached_rules_error", error=str(e))
            raise CacheError("Failed to list cached rules")

    def get_ctas_for_cleanup(self, older_than_days: int = 7) -> List[Dict]:
        """
        Get list of old CTAS tables for cleanup

        Args:
            older_than_days: Age threshold in days

        Returns:
            List of CTAS table info dicts
        """
        try:
            ctas_tables = self.cache_manager.get_ctas_tables_for_cleanup(older_than_days)

            app_logger.info(
                "ctas_cleanup_list_generated",
                count=len(ctas_tables),
                age_threshold_days=older_than_days
            )

            return ctas_tables

        except Exception as e:
            app_logger.error("get_ctas_for_cleanup_error", error=str(e))
            raise CacheError("Failed to get CTAS cleanup list")


# Global instance
cache_service = CacheService()
