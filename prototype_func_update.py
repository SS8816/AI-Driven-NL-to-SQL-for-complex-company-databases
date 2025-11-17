def build_known_functions_from_vectorstore():
    """Dynamically extract supported functions from docs."""
    vectorstore = _get_vectorstore()
    
    # Search for function definition patterns
    function_pattern = r'([A-Z_][A-Z0-9_]*)\s*\([^)]*\)\s*→'
    
    all_functions = set()
    
    # Sample documentation chunks
    for doc in vectorstore.docstore._dict.values():
        matches = re.findall(function_pattern, doc.page_content)
        all_functions.update(matches)
    
    return all_functions

# Cache this on startup
KNOWN_FUNCTIONS = build_known_functions_from_vectorstore()
