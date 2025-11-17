import os
from pathlib import Path
from dotenv import load_dotenv
from langgraph_orch import (
    extract_functions_from_sql,
    get_athena_supported_functions,
    get_known_invalid_functions,
    _get_function_vectorstore, _get_docs_vectorstore,
    GraphState
)
from prompts import create_function_validation_prompt, create_syntax_validation_prompt
from openai import AzureOpenAI

# Load environment
load_dotenv()

def test_function_validation():
    """Test Stage 1: Function validation."""
    print("\n" + "="*80)
    print("STAGE 1: FUNCTION VALIDATION TEST")
    print("="*80)
    
    # Load broken SQL
    with open("invalid_sql.sql", 'r') as f:
        sql = f.read()
    
    # Extract functions
    print("\n1. Extracting functions from SQL...")
    functions = extract_functions_from_sql(sql)
    print(f"   Found {len(functions)} functions: {', '.join(functions[:10])}...")
    
    # Classify functions
    print("\n2. Classifying functions...")
    supported = get_athena_supported_functions()
    invalid = get_known_invalid_functions()
    
    valid_funcs = []
    suspicious_funcs = []
    invalid_funcs = []
    
    for func in functions:
        func_upper = func.upper()
        if func_upper in invalid:
            invalid_funcs.append({'function': func, 'issue': invalid[func_upper]})
        elif func_upper in supported:
            valid_funcs.append(func)
        else:
            suspicious_funcs.append(func)
    
    print(f"   Valid: {len(valid_funcs)}")
    print(f"   Suspicious: {len(suspicious_funcs)} - {suspicious_funcs}")
    print(f"   Invalid: {len(invalid_funcs)}")
    
    for inv in invalid_funcs:
        print(f"      - {inv['function']}: {inv['issue'][:60]}...")
    
    # RAG all functions
    print("\n3. Retrieving usage docs for ALL functions...")
    vectorstore = _get_function_vectorstore()
    
    if not vectorstore:
        print("   ERROR: Vectorstore not available")
        return None
    
    all_functions_with_docs = {}
    for func in functions:
        try:
            search_query = f"{func} Athena SQL function syntax usage"
            retriever = vectorstore.as_retriever(search_kwargs={"k": 2})
            docs = retriever.invoke(search_query)
            all_functions_with_docs[func] = docs
        except:
            all_functions_with_docs[func] = []
    
    docs_count = sum(1 for docs in all_functions_with_docs.values() if docs)
    print(f"   Retrieved docs for {docs_count}/{len(functions)} functions")
    
    # Load minimal schema
    schema = "CREATE EXTERNAL TABLE fastmap_prod2_v2_13_base.latest_vehiclepath (...)"
    
    # Create function validation prompt
    print("\n4. Creating function validation prompt...")
    prompt = create_function_validation_prompt(
        generated_sql=sql,
        all_functions_with_docs=all_functions_with_docs,
        suspicious_functions=suspicious_funcs,
        invalid_functions=invalid_funcs,
        schema=schema
    )
    
    print(f"   Prompt size: {len(prompt)} characters")
    
    # Call LLM
    print("\n5. Calling LLM for function validation...")
    azure_config = {
        "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
        "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
        "azure_deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        "api_version": os.getenv("AZURE_OPENAI_API_VERSION")
    }
    
    client = AzureOpenAI(**azure_config)
    
    response = client.chat.completions.create(
        model=azure_config["azure_deployment"],
        messages=[
            {"role": "system", "content": "You are an AWS Athena SQL function validator."},
            {"role": "user", "content": prompt}
        ],
        temperature=1
    )
    
    validated_sql = response.choices[0].message.content.strip()
    
    # Check for fixes
    print("\n6. Analyzing fixes...")
    fixes = []
    
    if 'STRING_TO_ARRAY' in sql and 'split' in validated_sql:
        fixes.append("STRING_TO_ARRAY -> split")
    if 'ST_UNION_AGG' in sql and 'geometry_union_agg' in validated_sql:
        fixes.append("ST_UNION_AGG -> geometry_union_agg")
    if 'IFNULL' in sql and 'COALESCE' in validated_sql:
        fixes.append("IFNULL -> COALESCE")
    if 'TO_CHAR' in sql and 'CAST' in validated_sql:
        fixes.append("TO_CHAR -> CAST")
    
    if fixes:
        print("   Fixes applied:")
        for fix in fixes:
            print(f"      - {fix}")
    else:
        print("   No fixes detected")
    
    sql_changed = validated_sql.strip() != sql.strip()
    print(f"\n   SQL changed: {sql_changed}")
    
    return validated_sql


def test_syntax_validation(function_validated_sql):
    """Test Stage 2: Syntax validation."""
    print("\n" + "="*80)
    print("STAGE 2: SYNTAX VALIDATION TEST")
    print("="*80)
    
    # Load errors.txt
    print("\n1. Loading production errors...")
    errors_txt_path = Path("errors.txt")
    
    if errors_txt_path.exists():
        with open(errors_txt_path, 'r') as f:
            errors_content = f.read()
        error_count = errors_content.count('[')
        print(f"   Loaded {error_count} error patterns from errors.txt")
    else:
        errors_content = ""
        print("   errors.txt not found")
    
    # Load minimal schema
    schema = "CREATE EXTERNAL TABLE fastmap_prod2_v2_13_base.latest_vehiclepath (...)"
    
    # Create syntax validation prompt
    print("\n2. Creating syntax validation prompt...")
    prompt = create_syntax_validation_prompt(
        function_validated_sql=function_validated_sql,
        errors_txt_content=errors_content,
        schema=schema
    )
    
    print(f"   Prompt size: {len(prompt)} characters")
    
    # Call LLM
    print("\n3. Calling LLM for syntax validation...")
    azure_config = {
        "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
        "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
        "azure_deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        "api_version": os.getenv("AZURE_OPENAI_API_VERSION")
    }
    
    client = AzureOpenAI(**azure_config)
    
    response = client.chat.completions.create(
        model=azure_config["azure_deployment"],
        messages=[
            {"role": "system", "content": "You are an AWS Athena SQL syntax validator."},
            {"role": "user", "content": prompt}
        ],
        temperature=1
    )
    
    final_sql = response.choices[0].message.content.strip()
    
    # Check for syntax fixes
    print("\n4. Analyzing syntax fixes...")
    sql_changed = final_sql.strip() != function_validated_sql.strip()
    print(f"   SQL changed: {sql_changed}")
    
    # Check for specific patterns
    if 'ON TRUE' in final_sql and 'ON TRUE' not in function_validated_sql:
        print("   - Added missing ON TRUE to UNNEST join")
    
    if final_sql.count('GROUP BY') != function_validated_sql.count('GROUP BY'):
        print("   - Modified GROUP BY clause")
    
    return final_sql


def main():
    """Run quick validation test."""
    print("="*80)
    print("QUICK VALIDATION TEST")
    print("="*80)
    print("\nThis test validates the SQL without executing on Athena.")
    print("Much faster for debugging validation logic.")
    
    input("\nPress Enter to start...")
    
    try:
        # Stage 1
        function_validated_sql = test_function_validation()
        
        if not function_validated_sql:
            print("\nStage 1 failed. Stopping.")
            return
        
        # Stage 2
        final_sql = test_syntax_validation(function_validated_sql)
        
        # Save output
        print("\n" + "="*80)
        print("SAVING RESULTS")
        print("="*80)
        
        output_dir = Path("test_outputs")
        output_dir.mkdir(exist_ok=True)
        
        # Save function-validated SQL
        with open(output_dir / "stage1_function_validated.sql", 'w') as f:
            f.write(function_validated_sql)
        print(f"Stage 1 output: test_outputs/stage1_function_validated.sql")
        
        # Save final SQL
        with open(output_dir / "stage2_final_validated.sql", 'w') as f:
            f.write(final_sql)
        print(f"Stage 2 output: test_outputs/stage2_final_validated.sql")
        
        # Summary
        print("\n" + "="*80)
        print("TEST COMPLETE")
        print("="*80)
        print("\nCheck test_outputs/ for validated SQL files.")
        print("Compare them with invalid_sql.sql to see all fixes.")
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()