"""
Utility script to clean up old CTAS tables.

Usage:
    python ctas_cleanup.py --older-than 7 --dry-run
    python ctas_cleanup.py --older-than 30 --execute
"""

import argparse
import asyncio
from datetime import datetime
from cache_manager import CacheManager
from athena_client import AthenaClient
from config import Config
from models import QueryRequest


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Clean up old CTAS tables from Athena"
    )
    
    parser.add_argument(
        '--older-than',
        type=int,
        default=7,
        help='Delete CTAS tables older than N days (default: 7)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Actually execute the deletion (required for non-dry-run)'
    )
    
    return parser.parse_args()


async def drop_ctas_table(ctas_name: str, database: str, athena_client: AthenaClient) -> bool:
    """
    Drop a CTAS table from Athena.
    
    Args:
        ctas_name: Full CTAS table name (with database prefix)
        database: Database name
        athena_client: AthenaClient instance
    
    Returns:
        True if successful, False otherwise
    """
    try:
        drop_sql = f"DROP TABLE IF EXISTS {ctas_name}"
        
        request = QueryRequest(
            database=database,
            query=drop_sql,
            max_rows=1
        )
        
        result = await athena_client.execute_query(request)
        
        if isinstance(result, str):
            print(f"     Timeout dropping {ctas_name}: {result}")
            return False
        
        print(f"   ✓ Dropped {ctas_name}")
        return True
    
    except Exception as e:
        print(f"   ✗ Error dropping {ctas_name}: {str(e)}")
        return False


def main():
    """Main cleanup process."""
    args = parse_args()
    
    if not args.dry_run and not args.execute:
        print("Error: Must specify either --dry-run or --execute")
        return
    
    print("=" * 80)
    print("CTAS CLEANUP UTILITY")
    print("=" * 80)
    print(f"Mode: {'DRY RUN' if args.dry_run else 'EXECUTE'}")
    print(f"Delete tables older than: {args.older_than} days")
    print()
    
    # Get old CTAS tables from cache
    cache_mgr = CacheManager()
    old_ctas = cache_mgr.get_ctas_tables_for_cleanup(older_than_days=args.older_than)
    
    if not old_ctas:
        print("✓ No CTAS tables found older than {} days".format(args.older_than))
        return
    
    print(f"Found {len(old_ctas)} CTAS tables to delete:\n")
    
    # Display tables to be deleted
    for idx, ctas_info in enumerate(old_ctas, 1):
        age_days = (datetime.now() - datetime.fromisoformat(ctas_info['created_at'])).days
        print(f"{idx}. {ctas_info['ctas_name']}")
        print(f"   Rule: {ctas_info['rule_category']}")
        print(f"   Database: {ctas_info['database']}")
        print(f"   Age: {age_days} days")
        print()
    
    if args.dry_run:
        print("=" * 80)
        print("DRY RUN - No tables were deleted")
        print("Run with --execute to actually delete these tables")
        print("=" * 80)
        return
    
    # Confirm deletion
    print("=" * 80)
    confirmation = input(f"Delete {len(old_ctas)} tables? (yes/no): ").strip().lower()
    
    if confirmation != 'yes':
        print("Cancelled.")
        return
    
    print("\nDeleting tables...")
    print("=" * 80)
    
    # Initialize Athena client
    config = Config()
    athena_client = AthenaClient(config)
    
    # Delete each CTAS table
    success_count = 0
    failed_count = 0
    
    for idx, ctas_info in enumerate(old_ctas, 1):
        ctas_name = ctas_info['ctas_name']
        database = ctas_info['database']
        
        print(f"\n[{idx}/{len(old_ctas)}] Dropping {ctas_name}...")
        
        success = asyncio.run(drop_ctas_table(ctas_name, database, athena_client))
        
        if success:
            success_count += 1
        else:
            failed_count += 1
    
    # Summary
    print("\n" + "=" * 80)
    print("CLEANUP COMPLETE")
    print("=" * 80)
    print(f"✓ Successfully deleted: {success_count}")
    print(f"✗ Failed to delete: {failed_count}")
    print(f"Total processed: {len(old_ctas)}")
    
    # Ask if should remove from cache
    if success_count > 0:
        print("\n" + "=" * 80)
        remove_cache = input(f"Remove deleted tables from cache? (yes/no): ").strip().lower()
        
        if remove_cache == 'yes':
            # Remove cache entries for deleted tables
            removed_count = 0
            for ctas_info in old_ctas:
                deleted = cache_mgr.invalidate_cache(
                    ctas_info['rule_category'],
                    ctas_info['database']
                )
                if deleted > 0:
                    removed_count += deleted
            
            print(f"✓ Removed {removed_count} cache entries")
    
    print("=" * 80)


if __name__ == "__main__":
    main()