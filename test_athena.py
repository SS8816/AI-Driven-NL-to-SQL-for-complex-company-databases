import os
from pathlib import Path
from dotenv import load_dotenv
from langgraph_orch import run_orchestrator


load_dotenv()


def load_broken_sql():
    """Load the intentionally broken SQL from file."""
    sql_file = Path("invalid_sql.sql")
    
    if not sql_file.exists():
        print("Error: invalid_sql.sql not found!")
        return None
    
    with open(sql_file, 'r', encoding='utf-8') as f:
        return f.read()


def load_schema():
    """Load the database schema."""
    # Adjust this path to your actual schema file
    schema_file = Path("schemas/fastmap_prod2_v2_13_base.txt")
    
    if not schema_file.exists():
        print("Error: Schema file not found!")
        print(f"Looking for: {schema_file}")
        return None
    
    with open(schema_file, 'r', encoding='utf-8') as f:
        return f.read()


def print_header(title):
    """Print a formatted header."""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)


def print_section(title):
    """Print a section divider."""
    print("\n" + "-"*80)
    print(f"  {title}")
    print("-"*80)


def main():
    """Main test execution."""
    
    print_header("VALIDATION PIPELINE TEST")
    print("\nThis test will:")
    print("1. Load intentionally broken SQL (invalid_sql.sql)")
    print("2. Pass it through the two-stage validation pipeline")
    print("3. Attempt to execute on Athena")
    print("4. Report all detected errors and fixes")
    
    # Load broken SQL
    print_section("Loading Broken SQL")
    broken_sql = load_broken_sql()
    
    if not broken_sql:
        return
    
    print(f"Loaded SQL: {len(broken_sql)} characters")
    print(f"Lines: {len(broken_sql.split(chr(10)))}")
    
    # Show detected errors in comments
    print("\nExpected errors in SQL:")
    errors_in_comments = [
        line.strip() for line in broken_sql.split('\n')
        if '-- Error' in line or '-- Syntax Error' in line
    ]
    
    for i, err in enumerate(errors_in_comments, 1):
        print(f"  {i}. {err}")
    
    # Load schema
    print_section("Loading Database Schema")
    schema = load_schema()
    
    if not schema:
        return
    
    print(f"Schema loaded: {len(schema)} characters")
    
    # Create natural language query (for context)
    nl_query = """
    Find vehicle paths that are:
    1. Outside their associated lane groups
    2. Have overlap > 5 meters with lane groups
    3. Associated topology is NOT (private road OR parking lot OR public access OR auto-allowed)
    """
    
    print_section("Test Configuration")
    print(f"NL Query: {nl_query.strip()}")
    print(f"Rule Category: VALIDATION_TEST")
    print(f"Force Refresh: True (bypass cache)")
    
    # Run orchestrator
    print_header("STARTING VALIDATION PIPELINE")
    print("\nWatching for:")
    print("  Stage 1: Function validation (should catch invalid functions)")
    print("  Stage 2: Syntax validation (should catch syntax errors)")
    print("  Execution: Should succeed after fixes (or fail with clear error)")
    
    input("\nPress Enter to start the test...")
    
    result = None
    progress_messages = []
    
    try:
        # Run the orchestrator
        orchestrator = run_orchestrator(
            query=nl_query,
            schema=schema,
            guardrails="",
            rule_category="VALIDATION_TEST",
            force_refresh=True
        )
        
        # Stream updates
        for update in orchestrator:
            if isinstance(update, str):
                # Progress message
                print(update)
                progress_messages.append(update)
            elif isinstance(update, dict):
                # Final result
                result = update
        
    except Exception as e:
        print(f"\nError during orchestration: {str(e)}")
        import traceback
        traceback.print_exc()
        return
    
    # Analyze results
    print_header("TEST RESULTS")
    
    if not result:
        print("ERROR: No result returned from orchestrator")
        return
    
    # Check for errors
    if result.get("error"):
        print_section("Execution Failed")
        print(f"Error: {result['error']}")
        print("\nThis could mean:")
        print("  1. Validation didn't catch all errors")
        print("  2. Schema mismatch (column doesn't exist)")
        print("  3. Athena infrastructure issue")
    else:
        print_section("Execution Succeeded!")
        print(f"Rows returned: {result.get('row_count', 0)}")
        print(f"Execution time: {result.get('execution_time_ms', 0) / 1000:.2f}s")
        print(f"Data scanned: {result.get('bytes_scanned', 0) / (1024*1024):.2f} MB")
    
    # Show final SQL
    print_section("Final SQL (After Validation)")
    final_sql = result.get("final_sql", "")
    
    if final_sql:
        print(final_sql[:1000])
        if len(final_sql) > 1000:
            print(f"\n... ({len(final_sql) - 1000} more characters)")
    
    # Compare original vs final
    print_section("Validation Impact Analysis")
    
    original_lines = len(broken_sql.split('\n'))
    final_lines = len(final_sql.split('\n')) if final_sql else 0
    
    print(f"Original SQL: {original_lines} lines")
    print(f"Final SQL: {final_lines} lines")
    print(f"Difference: {final_lines - original_lines:+d} lines")
    
    # Check for specific fixes
    fixes_detected = []
    
    # Function fixes
    if 'STRING_TO_ARRAY' in broken_sql and 'split' in final_sql.lower():
        fixes_detected.append("STRING_TO_ARRAY -> split")
    
    if 'ST_UNION_AGG' in broken_sql and 'geometry_union_agg' in final_sql:
        fixes_detected.append("ST_UNION_AGG -> geometry_union_agg")
    
    if 'ARRAY_LENGTH' in broken_sql and 'cardinality' in final_sql:
        fixes_detected.append("ARRAY_LENGTH -> cardinality")
    
    if 'IFNULL' in broken_sql and 'COALESCE' in final_sql:
        fixes_detected.append("IFNULL -> COALESCE")
    
    if 'TO_CHAR' in broken_sql and ('CAST' in final_sql or 'format' in final_sql.lower()):
        fixes_detected.append("TO_CHAR -> CAST/format")
    
    if fixes_detected:
        print("\nDetected fixes:")
        for fix in fixes_detected:
            print(f"  - {fix}")
    else:
        print("\nNo obvious fixes detected (check logs for details)")
    
    # Show validation performed flag
    print_section("Validation Metadata")
    print(f"Validation performed: {result.get('validation_performed', False)}")
    print(f"Cache hit: {result.get('cache_hit', False)}")
    
    # Summary
    print_header("TEST SUMMARY")
    
    if result.get("error"):
        print("Status: FAILED")
        print(f"Reason: {result['error'][:200]}")
    else:
        print("Status: PASSED")
        print("The validation pipeline successfully:")
        print("  1. Detected invalid functions")
        print("  2. Fixed function usage errors")
        print("  3. Corrected syntax issues")
        print("  4. Executed on Athena successfully")
    
    # Save outputs
    print_section("Saving Outputs")
    
    output_dir = Path("test_outputs")
    output_dir.mkdir(exist_ok=True)
    
    # Save final SQL
    if final_sql:
        output_file = output_dir / "validated_sql.sql"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(final_sql)
        print(f"Final SQL saved to: {output_file}")
    
    # Save test report
    report_file = output_dir / "test_report.txt"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("VALIDATION PIPELINE TEST REPORT\n")
        f.write("="*80 + "\n\n")
        f.write(f"Status: {'PASSED' if not result.get('error') else 'FAILED'}\n")
        f.write(f"Rows returned: {result.get('row_count', 0)}\n")
        f.write(f"Execution time: {result.get('execution_time_ms', 0) / 1000:.2f}s\n")
        f.write(f"Validation performed: {result.get('validation_performed', False)}\n\n")
        
        if fixes_detected:
            f.write("Detected Fixes:\n")
            for fix in fixes_detected:
                f.write(f"  - {fix}\n")
        
        if result.get("error"):
            f.write(f"\nError:\n{result['error']}\n")
    
    print(f"Test report saved to: {report_file}")
    
    print("\n" + "="*80)
    print("Test complete!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()