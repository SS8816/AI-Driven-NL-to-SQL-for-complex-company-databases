import sqlite3
import re
import os
from datetime import datetime, timedelta
from pathlib import Path
from langchain_community.vectorstores import FAISS
from langchain_openai import AzureOpenAIEmbeddings
from dotenv import load_dotenv


load_dotenv()


DB_PATH = "query_logs.db"
ERRORS_TXT_PATH = "errors.txt"
VECTORSTORE_PATH = Path("athena_docs_vectorstore")


def get_vectorstore():
    """Load FAISS vectorstore for RAG."""
    if not VECTORSTORE_PATH.exists():
        print(f"Error: Vectorstore not found at {VECTORSTORE_PATH}")
        return None
    
    try:
        embeddings = AzureOpenAIEmbeddings(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            azure_deployment="text-embedding-3-small",
            api_version=os.getenv("AZURE_OPENAI_API_VERSION")
        )
        
        vectorstore = FAISS.load_local(
            str(VECTORSTORE_PATH),
            embeddings,
            allow_dangerous_deserialization=True
        )
        
        return vectorstore
    except Exception as e:
        print(f"Error loading vectorstore: {str(e)}")
        return None


def normalize_error_type(error_message: str) -> str:
    """
    Extract error type from error message.
    
    Examples:
    - "MISMATCHED_COLUMN_ALIASES: line 46..." -> "MISMATCHED_COLUMN_ALIASES"
    - "SYNTAX_ERROR: mismatched input..." -> "SYNTAX_ERROR"
    """
    # Try to extract error type before colon
    match = re.match(r'^([A-Z_]+):', error_message)
    if match:
        return match.group(1)
    
    # Fallback: infer from keywords
    error_lower = error_message.lower()
    
    if 'column' in error_lower and 'alias' in error_lower:
        return 'MISMATCHED_COLUMN_ALIASES'
    elif 'aggregate' in error_lower:
        return 'EXPRESSION_NOT_AGGREGATE'
    elif 'function' in error_lower and 'argument' in error_lower:
        return 'INVALID_FUNCTION_ARGUMENT'
    elif 'syntax' in error_lower:
        return 'SYNTAX_ERROR'
    else:
        return 'UNKNOWN_ERROR'


def get_existing_error_types():
    """
    Read errors.txt and extract all existing error types.
    
    Returns:
        Set of error types already logged
    """
    if not Path(ERRORS_TXT_PATH).exists():
        return set()
    
    existing_types = set()
    
    try:
        with open(ERRORS_TXT_PATH, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find all [ERROR_TYPE] patterns
        matches = re.findall(r'\[([A-Z_]+)\]', content)
        existing_types = set(matches)
        
    except Exception as e:
        print(f"Warning: Failed to read errors.txt: {str(e)}")
    
    return existing_types


def get_recent_errors(days_back: int = 1):
    """
    Query query_logs.db for unique errors in last N days.
    
    Args:
        days_back: Number of days to look back
        
    Returns:
        List of (error_type, error_message, count) tuples
    """
    if not Path(DB_PATH).exists():
        print(f"Error: Database not found at {DB_PATH}")
        return []
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S')
        
        query = """
        SELECT error_message, COUNT(*) as count
        FROM query_executions
        WHERE status = 'failed'
          AND error_message IS NOT NULL
          AND timestamp >= ?
        GROUP BY error_message
        ORDER BY count DESC
        """
        
        cursor.execute(query, (cutoff_date,))
        rows = cursor.fetchall()
        conn.close()
        
        # Normalize to error types
        errors = []
        for error_msg, count in rows:
            error_type = normalize_error_type(error_msg)
            errors.append((error_type, error_msg, count))
        
        return errors
        
    except Exception as e:
        print(f"Error querying database: {str(e)}")
        return []


def rag_error_docs(error_type: str, vectorstore):
    """
    Retrieve top 2 docs for an error type.
    
    Args:
        error_type: Error type name
        vectorstore: FAISS vectorstore
        
    Returns:
        List of top 2 doc chunks
    """
    if not vectorstore:
        return []
    
    try:
        search_query = f"{error_type} Athena SQL error fix solution"
        
        retriever = vectorstore.as_retriever(
            search_type="similarity",
            search_kwargs={"k": 2}
        )
        
        docs = retriever.invoke(search_query)
        return docs
        
    except Exception as e:
        print(f"   RAG failed for {error_type}: {str(e)[:50]}")
        return []


def append_error_to_file(error_type: str, error_message: str, count: int, docs: list):
    """
    Append new error to errors.txt.
    
    Format:
    [ERROR_TYPE]
    Example: <raw error message>
    Doc 1: <rag chunk 1>
    Doc 2: <rag chunk 2>
    ---
    """
    try:
        with open(ERRORS_TXT_PATH, 'a', encoding='utf-8') as f:
            f.write(f"\n[{error_type}]\n")
            f.write(f"Example: {error_message[:200]}\n")
            
            if docs:
                for i, doc in enumerate(docs[:2], 1):
                    content = doc.page_content[:400].replace('\n', ' ')
                    f.write(f"Doc {i}: {content}...\n")
            else:
                f.write("Doc 1: No documentation found\n")
                f.write("Doc 2: No documentation found\n")
            
            f.write(f"Occurrences: {count}\n")
            f.write("---\n")
        
        print(f"   Appended {error_type} to errors.txt")
        
    except Exception as e:
        print(f"   Error writing to file: {str(e)}")


def main():
    """Main sync process."""
    print("="*80)
    print(f"ERROR SYNC - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Load vectorstore
    print("\n1. Loading vectorstore...")
    vectorstore = get_vectorstore()
    
    if not vectorstore:
        print("   Failed to load vectorstore. Exiting.")
        return
    
    print("   Vectorstore loaded successfully")
    
    # Get existing errors
    print("\n2. Checking existing errors in errors.txt...")
    existing_types = get_existing_error_types()
    print(f"   Found {len(existing_types)} existing error types")
    
    # Get recent errors from DB
    print("\n3. Querying database for recent errors (last 24 hours)...")
    recent_errors = get_recent_errors(days_back=1)
    print(f"   Found {len(recent_errors)} unique errors in last 24 hours")
    
    # Filter to new errors only
    seen_types = set()
    new_errors = []
    for error_type, error_msg, count in recent_errors:
        if error_type not in existing_types and error_type not in seen_types:
            new_errors.append((error_type, error_msg, count))
            seen_types.add(error_type)
    
    if not new_errors:
        print("\n4. No new errors to log. All errors already in errors.txt")
        print("="*80)
        return
    
    print(f"\n4. Processing {len(new_errors)} new error types...")
    
    # Process each new error
    added_count = 0
    for error_type, error_msg, count in new_errors:
        print(f"\n   Processing: {error_type} ({count} occurrences)")
        
        # RAG docs for this error
        docs = rag_error_docs(error_type, vectorstore)
        print(f"   Retrieved {len(docs)} docs")
        
        # Append to file
        append_error_to_file(error_type, error_msg, count, docs)
        added_count += 1
    
    # Summary
    
    print(f"SYNC COMPLETE")
    print(f"  New errors logged: {added_count}")
    print(f"  Total errors in errors.txt: {len(existing_types) + added_count}")
    


if __name__ == "__main__":
    main()