from langchain_community.vectorstores import FAISS
from langchain_openai import AzureOpenAIEmbeddings
import os
from dotenv import load_dotenv
import re

load_dotenv()


embeddings = AzureOpenAIEmbeddings(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_deployment="text-embedding-3-small",
    api_version=os.getenv("AZURE_OPENAI_API_VERSION")
)

vectorstore = FAISS.load_local("athena_docs_vectorstore", embeddings, allow_dangerous_deserialization=True)


test_sql = """
ST_Length(Linestring)
"""


def extract_functions(sql):
    """Extract all function calls from SQL."""
    sql_cleaned = re.sub(r"'[^']*'", '', sql)  
    sql_cleaned = re.sub(r'"[^"]*"', '', sql_cleaned)  
    
    pattern = r'\b([A-Z_][A-Z0-9_]*)\s*\('
    matches = re.findall(pattern, sql_cleaned, re.IGNORECASE)
    
    
    keywords_to_exclude = {
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 
        'OUTER', 'CROSS', 'ON', 'AND', 'OR', 'AS', 'IN', 'EXISTS',
        'NOT', 'BETWEEN', 'LIKE', 'IS', 'NULL', 'FULL',
        'CREATE', 'ALTER', 'DROP', 'TABLE', 'VIEW', 'INDEX', 'SCHEMA',
        'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF',
        'WITH', 'HAVING', 'GROUP', 'ORDER', 'PARTITION', 'OVER',
        'WINDOW', 'ROWS', 'RANGE', 'BY',
        'UNION', 'INTERSECT', 'EXCEPT', 'MINUS', 'ALL',
        'ANY', 'SOME',
        'INSERT', 'UPDATE', 'DELETE', 'INTO', 'VALUES', 'SET',
        'LIMIT', 'OFFSET', 'FETCH', 'DISTINCT', 'UNIQUE', 'USING'
    }
    
    functions = set()
    for func in matches:
        func_upper = func.upper()
        if func_upper not in keywords_to_exclude:
            functions.add(func_upper)
    
    return sorted(functions)


functions_used = extract_functions(test_sql)


print("EXTRACTED FUNCTIONS FROM SQL")

print(f"\nFound {len(functions_used)} unique functions:")
for func in functions_used:
    print(f"  - {func}")


print("RETRIEVING DOCUMENTATION FOR EACH FUNCTION")


for func in functions_used:
    print(f"\n{'─' * 80}")
    print(f"Function: {func}")
    print(f"{'─' * 80}")
    
   
    query = f"Athena Trino {func} function syntax examples"
    results = vectorstore.similarity_search(query, k=2)
    
    if results:
        print(f"\nFound {len(results)} relevant docs")
        print(f"\nTop Result Preview:")
        print(results[0].page_content[:40000] + "...")
        print(f"\nSource: {results[0].metadata.get('source_url', 'Unknown')}")
    else:
        print("\n✗ No documentation found (possibly not a built-in function)")


print("SUMMARY")

print(f"\nTotal functions analyzed: {len(functions_used)}")

