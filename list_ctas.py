"""
List all CTAS tables in cache with details.

Usage:
    python list_ctas.py
    python list_ctas.py --database fastmap_prod2_v2_13_base
"""

import argparse
from datetime import datetime
from cache_manager import CacheManager
from ctas_utils import extract_ctas_metadata, format_ctas_date


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="List all cached CTAS tables"
    )
    
    parser.add_argument(
        '--database',
        type=str,
        default=None,
        help='Filter by database name'
    )
    
    parser.add_argument(
        '--sort',
        type=str,
        choices=['date', 'rule', 'size'],
        default='date',
        help='Sort by: date (default), rule category, or size'
    )
    
    return parser.parse_args()


def format_age(created_at_str: str) -> str:
    """Format age of CTAS table."""
    created_at = datetime.fromisoformat(created_at_str)
    age = datetime.now() - created_at
    
    days = age.days
    hours = age.seconds // 3600
    
    if days > 0:
        return f"{days}d {hours}h"
    else:
        return f"{hours}h"


def main():
    """Main listing process."""
    args = parse_args()
    
    print("=" * 100)
    print("CACHED CTAS TABLES")
    print("=" * 100)
    
    cache_mgr = CacheManager()
    cached_rules = cache_mgr.get_all_cached_rules(database=args.database)
    
    # Filter to only CTAS entries
    ctas_entries = [
        entry for entry in cached_rules 
        if entry.get('ctas_table_name') and entry.get('execution_type') == 'ctas'
    ]
    
    if not ctas_entries:
        print("\nNo CTAS tables found in cache.")
        if args.database:
            print(f"(Filtered by database: {args.database})")
        return
    
    print(f"\nFound {len(ctas_entries)} CTAS tables:\n")
    
    # Table header
    print(f"{'Rule':<15} {'Database':<30} {'CTAS Date':<12} {'Age':<10} {'Rows':<10}")
    print("-" * 100)
    
    # Display each entry
    for entry in ctas_entries:
        rule = entry['rule_category']
        database = entry['database_name']
        
        # Extract date from CTAS name
        ctas_meta = extract_ctas_metadata(entry['ctas_table_name'])
        ctas_date = format_ctas_date(ctas_meta.get('date', '')) if ctas_meta.get('date') else 'Unknown'
        
        age = format_age(entry['created_at'])
        
        # Get row count from query preview (first 100 chars)
        query_preview = entry.get('query_preview', '')
        
        print(f"{rule:<15} {database:<30} {ctas_date:<12} {age:<10} {query_preview[:8]:<10}")
    
    print("-" * 100)
    print(f"\nTotal: {len(ctas_entries)} CTAS tables")
    
    # Statistics
    print("\n" + "=" * 100)
    print("STATISTICS")
    print("=" * 100)
    
    # Group by database
    by_database = {}
    for entry in ctas_entries:
        db = entry['database_name']
        by_database[db] = by_database.get(db, 0) + 1
    
    print("\nBy Database:")
    for db, count in sorted(by_database.items()):
        print(f"  {db}: {count} tables")
    
    # Group by rule category
    by_rule = {}
    for entry in ctas_entries:
        rule = entry['rule_category']
        by_rule[rule] = by_rule.get(rule, 0) + 1
    
    print("\nBy Rule Category:")
    for rule, count in sorted(by_rule.items()):
        print(f"  {rule}: {count} tables")
    
    # Age distribution
    now = datetime.now()
    age_buckets = {'< 1 day': 0, '1-7 days': 0, '7-30 days': 0, '> 30 days': 0}
    
    for entry in ctas_entries:
        created = datetime.fromisoformat(entry['created_at'])
        age_days = (now - created).days
        
        if age_days < 1:
            age_buckets['< 1 day'] += 1
        elif age_days < 7:
            age_buckets['1-7 days'] += 1
        elif age_days < 30:
            age_buckets['7-30 days'] += 1
        else:
            age_buckets['> 30 days'] += 1
    
    print("\nBy Age:")
    for bucket, count in age_buckets.items():
        print(f"  {bucket}: {count} tables")
    
    print("=" * 100)


if __name__ == "__main__":
    main()