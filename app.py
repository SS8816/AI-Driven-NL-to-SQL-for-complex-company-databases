import streamlit as st
from pathlib import Path
import json
import os
import re
import boto3
from datetime import timedelta
from dotenv import load_dotenv
from openai import AzureOpenAI
import tiktoken
import tempfile
import subprocess
import sys
import streamlit.components.v1 as components
from prompts import create_geospatial_viz_prompt
import geopandas as gpd
import folium
from shapely import wkt
import pandas as pd

from parser import NestedSchemaParser
from langgraph_orch import run_orchestrator
from cache_manager import CacheManager

from country_codes import (
    COUNTRY_NAME_TO_CODE, 
    get_country_name, 
    get_all_country_names,
    format_country_dropdown_option
)
from ctas_utils import generate_ctas_name, extract_ctas_metadata, format_ctas_date
import asyncio
from config import Config
from models import QueryRequest
from athena_client import AthenaClient

load_dotenv()



AZURE_CONFIG = {
    "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
    "endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
    "deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
    "api_version": os.getenv("AZURE_OPENAI_API_VERSION"),
}

SCHEMAS_DIR = Path(os.getenv("SCHEMAS_DIR", "schemas"))

if not all(
    [AZURE_CONFIG["api_key"], AZURE_CONFIG["endpoint"], AZURE_CONFIG["deployment"]]
):
    st.error("Azure OpenAI credentials are not fully configured in your .env file!")
    st.stop()


def load_available_schemas(schemas_dir: Path):
    """Scans the schemas directory and returns a dict of available .txt schema files."""
    if not schemas_dir.exists():
        st.error(f"Schemas directory not found: {schemas_dir}")
        st.stop()

    schemas = {f.stem: f for f in schemas_dir.glob("*.txt")}

    if not schemas:
        st.error(f"No .txt schema files found in {schemas_dir}")
        st.stop()

    return schemas


# Add this near the top of app.py, after imports

@st.cache_data(ttl=3600, show_spinner=False)  # Cache for 1 hour
def get_ctas_schema(ctas_name: str, database_name: str):
    """
    Fetch CTAS schema (cached to avoid repeated queries).
    """
    try:
        schema_query = f"DESCRIBE {ctas_name}"
        
        athena_config = Config()
        athena_client = AthenaClient(athena_config)
        
        schema_request = QueryRequest(
            database=database_name,
            query=schema_query,
            max_rows=100
        )
        
        schema_result = asyncio.run(athena_client.execute_query(schema_request))
        
        if not isinstance(schema_result, str):
            return pd.DataFrame(schema_result.rows, columns=schema_result.columns)
        else:
            return None
    except Exception as e:
        st.error(f"Error fetching schema: {str(e)}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)  # Cache for 1 hour
def get_available_countries(ctas_name: str, database_name: str, country_col: str):
    """
    Fetch distinct country codes from CTAS (cached).
    """
    try:
        countries_query = f"""
        SELECT DISTINCT {country_col}
        FROM {ctas_name} 
        WHERE {country_col} IS NOT NULL
        ORDER BY {country_col}
        """
        
        athena_config = Config()
        athena_client = AthenaClient(athena_config)
        
        countries_request = QueryRequest(
            database=database_name,
            query=countries_query,
            max_rows=1000
        )
        
        countries_result = asyncio.run(athena_client.execute_query(countries_request))
        
        if not isinstance(countries_result, str):
            countries_df = pd.DataFrame(countries_result.rows, columns=countries_result.columns)
            return countries_df[country_col].tolist()
        else:
            return []
    except Exception as e:
        return []


@st.cache_data(ttl=3600, show_spinner=False)  # Cache for 1 hour
def check_country_column_exists(ctas_name: str, database_name: str):
    """
    Check if CTAS has country code column (cached).
    Returns: (has_country_code, column_name)
    """
    try:
        check_query = f"SELECT * FROM {ctas_name} LIMIT 1"
        
        athena_config = Config()
        athena_client = AthenaClient(athena_config)
        
        check_request = QueryRequest(
            database=database_name,
            query=check_query,
            max_rows=1
        )
        
        check_result = asyncio.run(athena_client.execute_query(check_request))
        
        if not isinstance(check_result, str):
            sample_df = pd.DataFrame(check_result.rows, columns=check_result.columns)
            # Check if any column contains 'country_code'
            country_col = next((col for col in sample_df.columns if 'country_code' in col.lower()), None)
            return (country_col is not None, country_col)
        else:
            return (False, None)
    except Exception as e:
        return (False, None)


def count_tokens(text: str, model: str = "gpt-4") -> int:
    """Counts the number of tokens in a text string using tiktoken."""
    try:
        encoding = tiktoken.get_encoding("cl100k_base")
    except Exception:
        encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(text))


def call_llm_for_entity_extraction(
    schema_summary: str, nl_query: str, azure_config: dict
):
    """Calls Azure OpenAI to extract relevant tables/columns using a simplified schema summary."""
    prompt = f"""You are a database schema analyzer. Given a simplified database schema summary and a natural language query,
identify the REQUIRED set of tables and columns needed to answer the query.

DATABASE SCHEMA SUMMARY:
{schema_summary}

USER QUERY:
{nl_query}

Instructions:
- Be selective: only include tables and columns DIRECTLY needed for the query.
- For nested columns (e.g., "geometry (nested: type, coordinates)"), you MUST only include the parent column name (e.g., "geometry"). Do NOT include the nested fields like "geometry.type".
  
  **From ANY table that has them, ALWAYS include:**
  - `iso_country_code` (enables filtering by country)
  - `id` (always needed for identification)
  - `geometry` (needed for geospatial operations)
  
  **These context columns allow users to filter CTAS results by location after execution.**

- Return ONLY valid JSON in the exact format below.



{{
    "tables": {{
        "table_name": ["column1", "column2"],
        "another_table": ["column1"]
    }},
    "reasoning": "A brief explanation of why these tables and parent columns were selected."
}}"""

    try:
        client = AzureOpenAI(
            api_key=azure_config["api_key"],
            api_version=azure_config["api_version"],
            azure_endpoint=azure_config["endpoint"],
            azure_deployment=azure_config["deployment"],
        )

        response = client.chat.completions.create(
            model=azure_config["deployment"],
            messages=[
                {
                    "role": "system",
                    "content": "You are a database expert. Always return valid JSON.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=1,
            response_format={"type": "json_object"},
        )

        result = json.loads(response.choices[0].message.content)
        return result

    except Exception as e:
        st.error(f"Error calling Azure OpenAI: {str(e)}")
        raise


def generate_presigned_url(s3_path: str, expiration: int = 3600) -> str:
    """
    Generate pre-signed URL for S3 download.

    COMMENTED OUT - Uncomment this section if you want pre-signed URLs instead of direct S3 paths.

    Args:
        s3_path: Full S3 path (s3://bucket/key)
        expiration: URL expiration time in seconds (default 1 hour)

    Returns:
        Pre-signed URL string

    Usage in UI:
        Instead of showing s3_path directly, use:
        download_url = generate_presigned_url(result["s3_path"])
        st.markdown(f"[Download Full Results]({download_url})")
    """
    # Uncomment below to enable pre-signed URLs:

    # # Parse S3 path
    # if not s3_path.startswith("s3://"):
    #     raise ValueError(f"Invalid S3 path: {s3_path}")
    #
    # parts = s3_path.replace("s3://", "").split("/", 1)
    # bucket = parts[0]
    # key = parts[1]
    #
    # # Generate pre-signed URL
    # s3_client = boto3.client('s3')
    # url = s3_client.generate_presigned_url(
    #     'get_object',
    #     Params={'Bucket': bucket, 'Key': key},
    #     ExpiresIn=expiration
    # )
    #
    # return url

    # For now, just return the S3 path
    return s3_path


# MAIN UI FLOW

st.set_page_config(page_title="NL-to-SQL with Caching", layout="wide")
st.title("NL-to-SQL Pipeline with Smart Caching")
st.markdown(
    "Transform natural language queries into SQL, with intelligent result caching."
)


# SIDEBAR

st.sidebar.title("Configuration")

available_schemas = load_available_schemas(SCHEMAS_DIR)
selected_catalog = st.sidebar.selectbox(
    "Choose Database Catalog", options=list(available_schemas.keys())
)

with open(available_schemas[selected_catalog], "r", encoding="utf-8") as f:
    full_schema_text = f.read()

st.sidebar.metric("Schema Size", f"{len(full_schema_text):,} characters")
with st.sidebar.expander("Preview Schema"):
    st.code(full_schema_text[:1000] + "...", language="sql")

# Parse schema
if (
    "schema_parser" not in st.session_state
    or st.session_state.get("current_catalog") != selected_catalog
):
    with st.spinner(f"Parsing schema for **{selected_catalog}**..."):
        parser = NestedSchemaParser(full_schema_text)
        parser.parse()
        st.session_state["schema_parser"] = parser
        st.session_state["current_catalog"] = selected_catalog
        # Clear old session data
        for key in [
            "llm_extracted",
            "user_approved_schema",
            "final_schema_for_sql_agent",
        ]:
            if key in st.session_state:
                del st.session_state[key]
    st.sidebar.success(f"Parsed: {selected_catalog}")

# Cache statistics in sidebar
st.sidebar.markdown("---")
st.sidebar.markdown("### Cache Statistics")
cache_mgr = CacheManager()
stats = cache_mgr.get_cache_stats()
col1, col2 = st.sidebar.columns(2)
with col1:
    st.metric("Cached Queries", stats["valid_entries"])
with col2:
    st.metric("Expired", stats["expired_entries"])


# RULE CATEGORY & QUERY INPUT

st.markdown("### Query Input")

col1, col2 = st.columns([1, 3])

with col1:
    rule_category = st.text_input(
        "Rule Category",
        placeholder="e.g., WBL039",
        help="Enter the rule category code.",
    )

with col2:
    user_query = st.text_area(
        "Natural Language Query",
        height=100,
        placeholder="Vehicle Path is putside of lanegroup",
        help="Describe what data you need in plain English",
    )

if not rule_category:
    st.warning("Please enter a Rule Category to continue.")
    st.stop()


# ENTITY EXTRACTION

if st.button("Analyze Query", type="primary", width="stretch"):
    if not user_query.strip():
        st.warning("Please enter a query first.")
    else:
        with st.spinner("Analyzing query with LLM..."):
            try:
                parser = st.session_state["schema_parser"]
                llm_schema_summary = parser.create_llm_summary()

                full_schema_tokens = count_tokens(full_schema_text)
                summary_tokens = count_tokens(llm_schema_summary)
                st.session_state["token_info"] = {
                    "full": full_schema_tokens,
                    "summary": summary_tokens,
                }

                llm_response = call_llm_for_entity_extraction(
                    llm_schema_summary, user_query, AZURE_CONFIG
                )

                st.session_state["llm_extracted"] = llm_response.get("tables", {})
                st.session_state["llm_reasoning"] = llm_response.get("reasoning", "")
                st.session_state["user_approved_schema"] = dict(
                    llm_response.get("tables", {})
                )
                st.session_state["user_query"] = user_query
                st.session_state["rule_category"] = rule_category

                st.success(
                    f"LLM identified **{len(llm_response.get('tables', {}))}** relevant tables!"
                )

            except Exception as e:
                st.error(f"Error during LLM analysis: {str(e)}")


# Token usage in sidebar
if "token_info" in st.session_state:
    st.sidebar.markdown("---")
    with st.sidebar.expander("Token Usage", expanded=False):
        info = st.session_state["token_info"]
        full = info["full"]
        summary = info["summary"]
        reduction = ((full - summary) / full) * 100 if full > 0 else 0

        st.metric("Full Schema", f"{full:,} tokens")
        st.metric(
            "Simplified",
            f"{summary:,} tokens",
            delta=f"-{reduction:.1f}%",
            delta_color="inverse",
        )


# USER REVIEW

if "llm_extracted" in st.session_state:
    st.markdown("---")
    st.markdown("## LLM Analysis & Review")

    if st.session_state.get("llm_reasoning"):
        st.info(f"**LLM's Reasoning:** {st.session_state['llm_reasoning']}")

    st.markdown("**Review selections below. Uncheck or add tables/columns as needed.**")

    parser = st.session_state["schema_parser"]
    approved_schema = st.session_state["user_approved_schema"]

    # Display tables
    for table_name in list(approved_schema.keys()):
        with st.container(border=True):
            col1, col2 = st.columns([1, 4])

            with col1:
                include_table = st.checkbox(
                    f"Include `{table_name}`", value=True, key=f"table_cb_{table_name}"
                )

            if not include_table:
                del approved_schema[table_name]
                st.rerun()

            with col2:
                all_table_cols = parser.tables.get(table_name, [])
                all_column_options = [col["column_name"] for col in all_table_cols]
                col_info_map = {col["column_name"]: col for col in all_table_cols}

                selected_cols = st.multiselect(
                    "Select columns:",
                    options=all_column_options,
                    default=approved_schema.get(table_name, []),
                    key=f"ms_{table_name}",
                )
                approved_schema[table_name] = selected_cols

                for col_name in selected_cols:
                    if col_info_map[col_name].get("is_nested"):
                        st.caption(f"  🔗 `{col_name}` is nested")

    # Add additional tables
    st.markdown("### Add Table")
    available_tables = [t for t in parser.tables.keys() if t not in approved_schema]

    if available_tables:
        new_table = st.selectbox(
            "Select a table to add:", options=["-- None --"] + available_tables
        )
        if new_table != "-- None --":
            approved_schema[new_table] = []
            st.rerun()
    else:
        st.info("All tables included.")


# FINAL SCHEMA & EXECUTION

if (
    "user_approved_schema" in st.session_state
    and st.session_state["user_approved_schema"]
):
    st.markdown("---")
    st.markdown("## Final Schema & Execution")

    parser = st.session_state["schema_parser"]
    approved_schema = st.session_state["user_approved_schema"]

    # Generate DDL
    final_ddls = []
    is_valid = True
    for table, columns in approved_schema.items():
        if not columns:
            st.warning(f"Table `{table}` has no columns selected.")
            is_valid = False

        parent_cols = list(dict.fromkeys([c.split(".")[0] for c in columns]))
        ddl = parser.get_full_ddl_for_columns(table, parent_cols)
        final_ddls.append(ddl)

    if is_valid:
        final_schema_for_sql_agent = "\n\n".join(final_ddls)

        with st.expander("View Final DDL"):
            st.code(final_schema_for_sql_agent, language="sql")

        # Additional instructions
        st.markdown("### Additional Instructions (Optional)")
        additional_instructions = st.text_area(
            "Any extra rules or constraints for SQL generation:",
            placeholder=" ",
            height=100,
            help="These instructions are passed to the LLM ",
        )

        # Force refresh option
        

        # Execute button
        # Initialize session state for results persistence
    if "query_executed" not in st.session_state:
        st.session_state["query_executed"] = False
    if "last_query_result" not in st.session_state:
        st.session_state["last_query_result"] = None


    st.markdown("---")
    st.markdown("## Execution Options")

    execution_mode = st.radio(
        "Choose execution mode:",
        options=["normal", "reexecute", "force"],
        format_func=lambda x: {
            "normal": "Use Cache (if available)",
            "reexecute": "Re-execute Cached SQL (new CTAS with current data)",
            "force": "Force Refresh (generate new SQL)"
        }[x],
        help="""
        **Use Cache**: Instant results if query was run recently (< 7 days)
        **Re-execute**: Use previous SQL but run on fresh data (creates new CTAS with today's date)
        **Force Refresh**: Ignore cache, generate brand new SQL and create new CTAS
        """,
        horizontal=True
    )
    database_for_cache = selected_catalog.split('.')[0] if '.' in selected_catalog else selected_catalog
    
    cache_mgr = CacheManager()
    cached_result = cache_mgr.get_cached_result(
        st.session_state.get('rule_category', ''),
        database_for_cache,
        st.session_state.get('user_query', '')
    )

    if cached_result and execution_mode in ["normal", "reexecute"]:
        with st.expander("Cached Query Info", expanded=False):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Age", f"{cached_result['age_hours']:.1f} hours")
            with col2:
                st.metric("Rows", f"{cached_result['row_count']:,}")
            with col3:
                if cached_result.get('ctas_table_name'):
                    ctas_meta = extract_ctas_metadata(cached_result['ctas_table_name'])
                    if ctas_meta.get('date'):
                        st.metric("CTAS Date", format_ctas_date(ctas_meta['date']))
            
            if execution_mode == "reexecute":
                st.info("✓ Will use cached SQL but create new CTAS with today's date")



    # Execute button
    if st.button("Execute Query", type="primary", width="stretch"):
        with st.status("Processing query...", expanded=True) as status:
            # Run orchestrator with execution mode
            orchestrator = run_orchestrator(
                query=st.session_state['user_query'],
                schema=final_schema_for_sql_agent,
                guardrails=additional_instructions,
                rule_category=st.session_state['rule_category'],
                execution_mode=execution_mode  # Pass execution mode!
            )
            
            result = None
            # Stream progress updates
            for update in orchestrator:
                if isinstance(update, str):
                    status.update(label=update)
                elif isinstance(update, dict):
                    result = update
            
            # Store result in session state
            if result and not result.get("error"):
                st.session_state['last_query_result'] = result
                st.session_state['query_executed'] = True
                status.update(label="Query Completed Successfully!", state="complete")
            elif result:
                status.update(label="Query Failed", state="error")
                st.error(f"Query failed: {result['error']}")
            else:
                status.update(label="Unexpected Error", state="error")
                st.error("Workflow ended unexpectedly without a result.")


# RESULTS DISPLAY (Replace your existing results display section)

# ============================================================================
# RESULTS DISPLAY SECTION
# ============================================================================

if st.session_state.get('query_executed') and st.session_state.get('last_query_result'):
    result = st.session_state['last_query_result']
    
    st.markdown("---")
    st.markdown("## Query Results")
    
    # Cache/Re-execute indicators
    if result.get("cache_hit"):
        age_hours = result.get("cached_age_hours", 0)
        st.success(f"Using cached results from {age_hours:.1f} hours ago")
    elif result.get("reexecuted"):
        st.success("Re-executed cached SQL on current data")
    else:
        st.success("Query executed successfully!")
    
    # Query Metrics
    with st.expander("Query Metrics", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            exec_time_sec = result['execution_time_ms'] / 1000
            st.metric("Execution Time", f"{exec_time_sec:.2f}s")
        with col2:
            data_scanned_mb = result['bytes_scanned'] / (1024 * 1024)
            st.metric("Data Scanned", f"{data_scanned_mb:.2f} MB")
        with col3:
            st.metric("Rows in CTAS", f"{result['row_count']:,}")
        with col4:
            if result.get('execution_id'):
                st.text("Execution ID:")
                st.code(result['execution_id'], language="text")
    
    # Generated SQL
    st.markdown("### Generated SQL")
    st.code(result["final_sql"], language="sql")
    
    # CTAS Information
    if result.get('ctas_table_name'):
        st.markdown("### CTAS Table")
        
        ctas_name = result['ctas_table_name']
        ctas_meta = extract_ctas_metadata(ctas_name)
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.code(ctas_name, language="text")
        with col2:
            if ctas_meta.get('date'):
                st.caption(f"Created: {format_ctas_date(ctas_meta['date'])}")
        
        st.info("""
        **CTAS Created!** The full query results are stored in this table. 
        You can now query this table to filter, aggregate, or explore the data.
        """)
    
    # Preview Results
    st.markdown("### Preview (First 1,000 Rows)")
    st.dataframe(result["result_df"], width="stretch", height=400)
    
    # Download Full Results
    st.markdown("### Download Full Results")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.code(result["s3_path"], language="text")
    with col2:
        st.caption("S3 Location")
    
    st.markdown("""
    **To download complete results:**
    - Use AWS CLI: `aws s3 cp <s3_path> ./results.csv`
    - Or navigate to S3 path in AWS Console
    """)
    
    

    # CTAS QUERY INTERFACE 

    
    if result.get('ctas_table_name'):
        st.markdown("---")
        st.markdown("## Query CTAS Results")
        
        ctas_name = result['ctas_table_name']
        database_name = ctas_name.split('.')[0]
        
        # Show CTAS schema
        # Show CTAS schema
        with st.expander(" View CTAS Schema", expanded=False):
            schema_df = get_ctas_schema(ctas_name, database_name)
            
            if schema_df is not None:
                st.dataframe(schema_df, width="stretch")
            else:
                st.warning("Could not retrieve schema")
                
        # Query Interface Tabs
        tab1, tab2 = st.tabs([" Simple Country Filter", " Query It Yourself"])
        
        with tab1:
            st.markdown("#### Filter by Country")
            
            try:  
                # Check if country column exists (CACHED!)
                has_country_code, country_col = check_country_column_exists(ctas_name, database_name)
                
                if has_country_code and country_col:
                    # Get available countries (CACHED!)
                    available_codes = get_available_countries(ctas_name, database_name, country_col)
                    
                    if available_codes:
                        # Map codes to friendly names
                        country_options = ["All Countries"]
                        for code in sorted(available_codes):
                            if code:  # Skip None/empty
                                name = get_country_name(code)
                                country_options.append(format_country_dropdown_option(name, code))
                        
                        selected_country = st.selectbox(
                            "Select Country:",
                            options=country_options,
                            help="Filter results by country"
                        )
                        
                        # Extract code from selection
                        if selected_country != "All Countries":
                            import re
                            match = re.search(r'\(([A-Z]{3})\)$', selected_country)
                            selected_code = match.group(1) if match else None
                        else:
                            selected_code = None
                        
                        # Limit input
                        col1, col2 = st.columns(2)
                        with col1:
                            limit = st.number_input(
                                "Number of rows:",
                                min_value=10,
                                max_value=10000,
                                value=1000,
                                step=100
                            )
                        
                        # Build query
                        if selected_code:
                            filter_query = f"""
                            SELECT * FROM {ctas_name}
                            WHERE {country_col} = '{selected_code}'
                            LIMIT {limit}
                            """
                        else:
                            filter_query = f"SELECT * FROM {ctas_name} LIMIT {limit}"
                        
                        st.code(filter_query, language="sql")
                        
                        if st.button("Execute Filter", type="primary", key="filter_exec"):
                            with st.spinner("Executing query..."):
                                try:
                                    athena_config = Config()
                                    athena_client = AthenaClient(athena_config)
                                    
                                    filter_request = QueryRequest(
                                        database=database_name,
                                        query=filter_query,
                                        max_rows=limit
                                    )
                                    
                                    filter_result = asyncio.run(athena_client.execute_query(filter_request))
                                    
                                    if not isinstance(filter_result, str):
                                        filtered_df = pd.DataFrame(filter_result.rows, columns=filter_result.columns)
                                        
                                        st.success(f"✓ Retrieved {len(filtered_df):,} rows")
                                        
                                        # Store for visualization BEFORE displaying
                                        st.session_state['filtered_result'] = filtered_df
                                        st.session_state['show_filtered_table'] = True 
                                    else:
                                        st.error(f"Query timed out: {filter_result}")
                                
                                except Exception as e:
                                    st.error(f"Query failed: {str(e)}")
                        if st.session_state.get('show_filtered_table') and 'filtered_result' in st.session_state:
                            st.dataframe(st.session_state['filtered_result'], width="stretch", height=400)
                    else:
                        st.warning("Could not retrieve available countries")
                else:
                    st.info("No country code column found in CTAS. Use Advanced SQL tab for custom queries.")
            
            except Exception as e:  # ← NOW THIS MATCHES THE TRY ABOVE!
                st.error(f"Error: {str(e)}")
        
        with tab2:
            st.markdown("#### Write Custom SQL")
            
            custom_sql = st.text_area(
                "SQL Query:",
                value=f"SELECT * FROM {ctas_name} LIMIT 1000",
                height=150,
                help="Write any SQL query against the CTAS table. Only SELECT queries are allowed."
            )
            
            # Validation
            if custom_sql.strip().upper().startswith('SELECT'):
                st.success("✓ Valid SELECT query")
            else:
                st.error("✗ Only SELECT queries are allowed")
            
            # Dangerous keywords check
            dangerous = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
            if any(kw in custom_sql.upper() for kw in dangerous):
                st.error("✗ Dangerous operations not allowed")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                if st.button("Execute Custom Query", type="primary", key="custom_exec"):
                    if not custom_sql.strip().upper().startswith('SELECT'):
                        st.error("Only SELECT queries allowed")
                    elif any(kw in custom_sql.upper() for kw in dangerous):
                        st.error("Dangerous operations not allowed")
                    else:
                        with st.spinner("Executing query..."):
                            try:
                                athena_config = Config()
                                athena_client = AthenaClient(athena_config)
                                
                                custom_request = QueryRequest(
                                    database=database_name,
                                    query=custom_sql,
                                    max_rows=10000
                                )
                                
                                custom_result = asyncio.run(athena_client.execute_query(custom_request))
                                
                                if not isinstance(custom_result, str):
                                    custom_df = pd.DataFrame(custom_result.rows, columns=custom_result.columns)
                                    
                                    st.success(f"✓ Retrieved {len(custom_df):,} rows")
                                    
                                    # Store for visualization BEFORE displaying
                                    st.session_state['filtered_result'] = custom_df
                                    st.session_state['show_custom_table'] = True 
                                else:
                                    st.error(f"Query timed out: {custom_result}")
                            
                            except Exception as e:
                                st.error(f"Query failed: {str(e)}")

                if st.session_state.get('show_custom_table') and 'filtered_result' in st.session_state:
                    st.dataframe(st.session_state['filtered_result'], width="stretch", height=400)
            with col2:
                st.caption("Tips:")
                st.caption("- Use WHERE for filters")
                st.caption("- Use GROUP BY for aggregations")
                st.caption("- Include LIMIT clause")
    
    

    # VISUALIZATION SECTION 

    
    st.markdown("---")
    st.markdown("### Geospatial Visualization")

# Determine which data to visualize
    if 'filtered_result' in st.session_state:
        df_to_viz = st.session_state['filtered_result']
        st.info("Will visualize the filtered query results above")
    else:
        df_to_viz = result["result_df"]
        st.info("Will visualize the preview results (first 1,000 rows)")

    # ============== NEW: ID SEARCH/FILTER ==============
    st.markdown("####  Filter by ID (Optional)")

    # Find ID columns automatically
    id_columns = [col for col in df_to_viz.columns if 'id' in col.lower()]

    if id_columns:
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            # Let user select which ID column to search
            selected_id_col = st.selectbox(
                "ID Column:",
                options=id_columns,
                help="Select which ID column to filter by"
            )
        
        with col2:
            # ID search input
            id_search = st.text_input(
                "Search by ID:",
                placeholder="Enter ID to highlight (e.g., 12345)",
                help="Enter an ID to center map on that feature and highlight it"
            )
        
        with col3:
            st.write("")  # Spacer
            st.write("")  # Spacer
            if st.button(" Find", type="secondary"):
                if id_search.strip():
                    # Filter data to only the searched ID
                    filtered = df_to_viz[df_to_viz[selected_id_col].astype(str).str.contains(id_search, case=False, na=False)]
                    
                    if len(filtered) > 0:
                        st.success(f"✓ Found {len(filtered)} matching feature(s)")
                        # Mark the searched feature
                        filtered = filtered.copy()
                        filtered['is_highlighted'] = True
                        
                        # Add non-matching features (grayed out)
                        others = df_to_viz[~df_to_viz[selected_id_col].astype(str).str.contains(id_search, case=False, na=False)].copy()
                        others['is_highlighted'] = False
                        
                        # Combine: highlighted first, then others
                        df_to_viz = pd.concat([filtered, others.head(200)], ignore_index=True)
                        st.info(f" Showing highlighted feature + 200 nearby features for context")
                    else:
                        st.error(f" No features found with ID containing '{id_search}'")
                else:
                    st.warning(" Enter an ID to search")

    # ===================================================

    # Check if data has geometry columns
    wkt_columns = [col for col in df_to_viz.columns if 'wkt' in col.lower()]

    if not wkt_columns:
        st.warning("No geometry columns found in data. Cannot visualize.")
        st.markdown("""
        **To visualize data:**
        - Ensure your query includes columns ending in `_wkt`
        - Example: `SELECT vp_id, vehicle_path_wkt, lanegroup_wkt, ... FROM ctas`
        - Aggregation queries like `SELECT COUNT(*) ...` cannot be visualized
        """)
    else:
        st.success(f"✓ Found {len(wkt_columns)} geometry column(s): {', '.join(wkt_columns)}")
        
        if st.button(
        "Visualize Geospatial Data",
        type="secondary",
        key="viz_button",
        use_container_width=True
        ):
            with st.spinner("Creating interactive map..."):
                try:
                    
                    from viz_helper import create_interactive_map
                    
                 
                    html_map = create_interactive_map(df_to_viz)
                    
                    
                    if '<h3' in html_map and ('No WKT' in html_map or 'No valid' in html_map):
                       
                        st.error(html_map)
                    else:
                       
                        st.success("Interactive map generated!")
                        st.components.v1.html(html_map, height=700, scrolling=True)
                    
                except Exception as e:
                    st.error(f" Failed to generate visualization: {str(e)}")
                    if os.getenv("ENV", "dev") == "dev":
                        st.exception(e)