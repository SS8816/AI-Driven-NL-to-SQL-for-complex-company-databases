"""
Cache Management API Endpoints
Handles cache statistics and management operations
"""

from typing import List, Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query

from app.models.response import CacheStats, SuccessResponse
from app.models.auth import UserInfo
from app.services.cache_service import cache_service
from app.utils.errors import CacheError
from app.utils.logger import app_logger
from app.dependencies import get_current_user


router = APIRouter(prefix="/cache", tags=["Cache"])


@router.get("/stats", response_model=CacheStats)
async def get_cache_statistics(user: UserInfo = Depends(get_current_user)):
    """
    Get cache statistics

    Shows total entries, valid/expired counts, hit rates, etc.

    Requires: Authentication

    Returns:
        CacheStats with cache metrics
    """
    try:
        stats = cache_service.get_cache_stats()

        app_logger.info(
            "cache_stats_retrieved",
            username=user.username,
            total_entries=stats.total_entries,
            valid_entries=stats.valid_entries
        )

        return stats

    except CacheError as e:
        app_logger.error("cache_stats_error", username=user.username, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": str(e), "error_code": "CACHE_ERROR"}
        )


@router.delete("/expired", response_model=SuccessResponse)
async def clear_expired_cache(user: UserInfo = Depends(get_current_user)):
    """
    Clear expired cache entries

    Removes all cache entries older than TTL (7 days)

    Requires: Authentication

    Returns:
        Success response with count of deleted entries
    """
    try:
        deleted_count = cache_service.clear_expired_cache()

        app_logger.info(
            "expired_cache_cleared",
            username=user.username,
            deleted_count=deleted_count
        )

        return SuccessResponse(
            success=True,
            message=f"Cleared {deleted_count} expired cache entries"
        )

    except CacheError as e:
        app_logger.error("clear_expired_cache_error", username=user.username, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": str(e), "error_code": "CACHE_ERROR"}
        )


@router.delete("/{rule_category}/{database}", response_model=SuccessResponse)
async def invalidate_cache(
    rule_category: str,
    database: str,
    user: UserInfo = Depends(get_current_user)
):
    """
    Manually invalidate cache for specific rule + database

    Useful when you know data has been updated

    Requires: Authentication

    Args:
        rule_category: Rule category to invalidate
        database: Database name

    Returns:
        Success response with count of deleted entries
    """
    try:
        deleted_count = cache_service.invalidate_cache(rule_category, database)

        app_logger.info(
            "cache_invalidated",
            username=user.username,
            rule_category=rule_category,
            database=database,
            deleted_count=deleted_count
        )

        return SuccessResponse(
            success=True,
            message=f"Invalidated {deleted_count} cache entries for {rule_category}/{database}"
        )

    except CacheError as e:
        app_logger.error(
            "invalidate_cache_error",
            username=user.username,
            rule_category=rule_category,
            database=database,
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": str(e), "error_code": "CACHE_ERROR"}
        )


@router.get("/rules", response_model=List[Dict])
async def list_cached_rules(
    database: Optional[str] = Query(None, description="Filter by database"),
    user: UserInfo = Depends(get_current_user)
):
    """
    List all cached rules

    Optionally filter by database

    Requires: Authentication

    Args:
        database: Optional database filter
        user: Authenticated user (injected)

    Returns:
        List of cached rule info dicts
    """
    try:
        cached_rules = cache_service.list_cached_rules(database)

        app_logger.info(
            "cached_rules_listed",
            username=user.username,
            count=len(cached_rules),
            database_filter=database
        )

        return cached_rules

    except CacheError as e:
        app_logger.error("list_cached_rules_error", username=user.username, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": str(e), "error_code": "CACHE_ERROR"}
        )


@router.get("/ctas/cleanup", response_model=List[Dict])
async def get_ctas_cleanup_list(
    older_than_days: int = Query(default=7, ge=1, le=365),
    user: UserInfo = Depends(get_current_user)
):
    """
    Get list of old CTAS tables for cleanup

    Returns CTAS tables older than specified threshold

    Requires: Authentication

    Args:
        older_than_days: Age threshold in days (default: 7)
        user: Authenticated user (injected)

    Returns:
        List of CTAS table info dicts
    """
    try:
        ctas_tables = cache_service.get_ctas_for_cleanup(older_than_days)

        app_logger.info(
            "ctas_cleanup_list_generated",
            username=user.username,
            count=len(ctas_tables),
            age_threshold_days=older_than_days
        )

        return ctas_tables

    except CacheError as e:
        app_logger.error("get_ctas_cleanup_error", username=user.username, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": str(e), "error_code": "CACHE_ERROR"}
        )
