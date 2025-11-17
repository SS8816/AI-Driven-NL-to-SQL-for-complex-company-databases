import asyncio
import re
import hashlib
from typing import TypedDict, Optional, Generator, Dict, Any
from langgraph.graph import StateGraph, END
from openai import AzureOpenAI
import os
import pandas as pd
import io
import boto3
from urllib.parse import urlparse
from pathlib import Path


from langchain_community.vectorstores import FAISS
from langchain_openai import AzureOpenAIEmbeddings

from app.core.prompts import (
    create_sql_generation_prompt,
    create_sql_fixing_prompt,
    create_rag_sql_fixing_prompt,
    create_function_validation_prompt,
    create_syntax_validation_prompt
)
from app.core.config import Config
from app.core.models import QueryRequest
from app.core.athena_client import AthenaClient, AthenaError
from app.core.logger_config import log_llm_interaction, log_query_execution
from app.core.cache_manager import CacheManager
from app.core.ctas_utils import generate_ctas_name


#Helper Functions

def _format_sql_query(raw_response: str) -> str:
    """Clean and format SQL response from LLM."""
    cleaned = raw_response.strip()
    
    #Remove markdown code blocks
    if cleaned.startswith("```sql"):
        cleaned = cleaned[6:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    
    cleaned = cleaned.strip()
    
    
    VALID_SQL_STARTS = ("WITH", "SELECT", "CREATE")
    if cleaned.upper().startswith(VALID_SQL_STARTS):
        return cleaned
    else:
        return "SELECT " + cleaned


def _download_s3_csv_to_df(s3_path: str, nrows: int = 1000) -> pd.DataFrame:
    """Download an S3 CSV and return as DataFrame."""
    parsed = urlparse(s3_path)
    if parsed.scheme != "s3":
        raise ValueError(f"Not an s3 path: {s3_path}")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    s3 = boto3.client("s3")

    # Try direct file access first
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return pd.read_csv(io.BytesIO(body), nrows=nrows)
    except s3.exceptions.NoSuchKey:
        pass
    except Exception as e:
        raise

    # Fallback: list objects under prefix
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key)
    for o in resp.get("Contents", []):
        k = o["Key"]
        if k.endswith(".csv") or k.endswith(".csv.gz") or k.endswith(".parquet"):
            obj = s3.get_object(Bucket=bucket, Key=k)
            body = obj["Body"].read()
            return pd.read_csv(io.BytesIO(body), nrows=nrows)

    raise FileNotFoundError(f"No CSV/Parquet object found under s3://{bucket}/{key}")


def _extract_database_from_ddl(schema_ddl: str) -> str:
    
    # Strict for our-use-case: Pattern: CREATE EXTERNAL TABLE `database.table` or `database_name`
    pattern = r"CREATE EXTERNAL TABLE\s+`?([^.`\s]+)(?:\.|\s)"
    match = re.search(pattern, schema_ddl, re.IGNORECASE)
    
    if match:
        return match.group(1)
    
    # Fallback: try to find anything that looks like database name
    pattern2 = r"`([a-z0-9_]+)\.latest_"
    match2 = re.search(pattern2, schema_ddl, re.IGNORECASE)
    if match2:
        return match2.group(1)
    
    raise ValueError(f"Could not extract database name from DDL. Schema starts with: {schema_ddl[:200]}")



# RAG SETUP - Load vector store at startup


DOCS_VECTORSTORE_PATH = Path("athena_docs_vectorstore")  
FUNCTION_VECTORSTORE_PATH = Path("vectorstores")  

_docs_vectorstore = None  
_function_vectorstore = None  
_embeddings = None
  
def _get_docs_vectorstore():
    """Load documentation vectorstore (for syntax validation & error fixing)."""
    global _docs_vectorstore, _embeddings
    
    if _docs_vectorstore is not None:
        return _docs_vectorstore
    
    if not DOCS_VECTORSTORE_PATH.exists():
        print(" Warning: Docs vector store not found. Syntax validation will be limited.")
        return None
    
    try:
        if _embeddings is None:
            _embeddings = AzureOpenAIEmbeddings(
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                azure_deployment="text-embedding-3-small",
                api_version=os.getenv("AZURE_OPENAI_API_VERSION")
            )
        
        _docs_vectorstore = FAISS.load_local(
            str(DOCS_VECTORSTORE_PATH),
            _embeddings,
            allow_dangerous_deserialization=True
        )
        
        print(" Docs vector store loaded successfully")
        return _docs_vectorstore
        
    except Exception as e:
        print(f" Warning: Failed to load docs vector store: {str(e)}")
        return None


def _get_function_vectorstore():
    """Load function reference vectorstore (for function validation)."""
    global _function_vectorstore, _embeddings
    
    if _function_vectorstore is not None:
        return _function_vectorstore
    
    if not FUNCTION_VECTORSTORE_PATH.exists():
        print(" Warning: Function vector store not found. Function validation will be limited.")
        return None
    
    try:
        if _embeddings is None:
            _embeddings = AzureOpenAIEmbeddings(
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                azure_deployment="text-embedding-3-small",
                api_version=os.getenv("AZURE_OPENAI_API_VERSION")
            )
        
        _function_vectorstore = FAISS.load_local(
            str(FUNCTION_VECTORSTORE_PATH),
            _embeddings,
            allow_dangerous_deserialization=True
        )
        
        print(" Function vector store loaded successfully")
        return _function_vectorstore
        
    except Exception as e:
        print(f"  Warning: Failed to load function vector store: {str(e)}")
        return None


def _determine_optimal_k(nl_query: str, error_context: str = None) -> int:
    
    k = 2  # Base value 
    
    # # Indicators 
    # complexity_indicators = [
    #     "geometry", "spatial", "intersection", "buffer", "distance",
    #     "unnest", "array", "transform", "aggregate", "window",
    #     "multiple", "merge", "union", "join", "complex"
    # ]
    
    # query_lower = nl_query.lower()
    # complexity_score = sum(1 for indicator in complexity_indicators if indicator in query_lower)
    
    # # More complex queris ? More chunks
    # if complexity_score >= 3:
    #     k = 6
    # elif complexity_score >= 2:
    #     k = 5
    # elif complexity_score >= 1:
    #     k = 4
    
    # # if corrrecting an error, k + 1 till 6
    # if error_context:
    #     k = min(k + 1, 6)  # Cap at 6
    
    return k


class GraphState(TypedDict):
    """State that flows through LangGraph nodes."""
    nl_query: str
    final_schema: str
    guardrails: str
    rule_category: str
    database_name: str
    generated_sql: Optional[str]
    validation_performed: Optional[bool]
    query_result: Optional[pd.DataFrame]
    s3_result_path: Optional[str]
    ctas_table_name: Optional[str] 
    error_message: Optional[str]
    retries: int
    execution_id: Optional[str]
    bytes_scanned: int
    execution_time_ms: int
    row_count: int



# LANGGRAPH NODES


def generate_sql_node(state: GraphState) -> Dict:
    """Node 1: Generate SQL using LLM."""
    log_llm_interaction("generate_sql_start", None, None, state["nl_query"])
    
    azure_config = {
        "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
        "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
        "azure_deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        "api_version": os.getenv("AZURE_OPENAI_API_VERSION")
    }
    
    prompt = create_sql_generation_prompt(
        schema=state["final_schema"],
        query=state["nl_query"],
        guardrails=state["guardrails"]
    )
    
    client = AzureOpenAI(**azure_config)
    response = client.chat.completions.create(
        model=azure_config["azure_deployment"],
        messages=[{"role": "user", "content": prompt}],
        temperature=1  
    )
    
    raw_sql = response.choices[0].message.content
    sql_query = _format_sql_query(raw_sql)
    
    log_llm_interaction("generate_sql_complete", prompt, sql_query, state["nl_query"])
    
    return {
        "generated_sql": sql_query,
        "error_message": None
    }


def get_athena_supported_functions():
    """
    Comprehensive list of Athena/Trino supported functions.
    Manually curated from official documentation (500+ functions).
    
    Returns set of uppercase function names.
    """
    return {
        # ===== AGGREGATE FUNCTIONS =====
        'APPROX_DISTINCT', 'APPROX_MOST_FREQUENT', 'APPROX_PERCENTILE',
        'APPROX_SET', 'ARBITRARY', 'ARRAY_AGG', 'AVG', 'BITWISE_AND_AGG',
        'BITWISE_OR_AGG', 'BOOL_AND', 'BOOL_OR', 'CHECKSUM', 'COUNT',
        'COUNT_IF', 'EVERY', 'GEOMETRIC_MEAN', 'HISTOGRAM', 'KURTOSIS',
        'LISTAGG', 'MAP_AGG', 'MAP_UNION', 'MAX', 'MAX_BY', 'MIN', 'MIN_BY',
        'MULTIMAP_AGG', 'NUMERIC_HISTOGRAM', 'QDIGEST_AGG', 'REDUCE_AGG',
        'REGR_INTERCEPT', 'REGR_SLOPE', 'SKEWNESS', 'STDDEV', 'STDDEV_POP',
        'STDDEV_SAMP', 'SUM', 'TDIGEST_AGG', 'VAR_POP', 'VAR_SAMP', 'VARIANCE',
        
        # ===== ARRAY FUNCTIONS =====
        'ALL_MATCH', 'ANY_MATCH', 'ARRAY_DISTINCT', 'ARRAY_EXCEPT',
        'ARRAY_INTERSECT', 'ARRAY_JOIN', 'ARRAY_MAX', 'ARRAY_MIN',
        'ARRAY_POSITION', 'ARRAY_REMOVE', 'ARRAY_SORT', 'ARRAY_UNION',
        'ARRAYS_OVERLAP', 'CARDINALITY', 'COMBINATIONS', 'CONCAT',
        'CONTAINS', 'CONTAINS_SEQUENCE', 'ELEMENT_AT', 'FILTER', 'FLATTEN',
        'NGRAMS', 'NONE_MATCH', 'REDUCE', 'REPEAT', 'REVERSE', 'SEQUENCE',
        'SHUFFLE', 'SLICE', 'TRANSFORM', 'TRIM_ARRAY', 'ZIP', 'ZIP_WITH',
        
        # ===== STRING FUNCTIONS =====
        'CHR', 'CODEPOINT', 'CONCAT', 'CONCAT_WS', 'FORMAT', 'HAMMING_DISTANCE',
        'INITCAP', 'LENGTH', 'LEVENSHTEIN_DISTANCE', 'LOWER', 'LPAD', 'LTRIM',
        'LUHN_CHECK', 'NORMALIZE', 'POSITION', 'REPLACE', 'REVERSE', 'RPAD',
        'RTRIM', 'SOUNDEX', 'SPLIT', 'SPLIT_PART', 'SPLIT_TO_MAP',
        'SPLIT_TO_MULTIMAP', 'STARTS_WITH', 'STRPOS', 'SUBSTR', 'SUBSTRING',
        'TRANSLATE', 'TRIM', 'UPPER', 'WORD_STEM',
        
        # ===== MATH FUNCTIONS =====
        'ABS', 'ACOS', 'ASIN', 'ATAN', 'ATAN2', 'BETA_CDF', 'BINOMIAL_CDF',
        'CAUCHY_CDF', 'CBRT', 'CEIL', 'CEILING', 'CHI_SQUARED_CDF', 'COS',
        'COSH', 'COSINE_SIMILARITY', 'DEGREES', 'E', 'EXP', 'F_CDF', 'FLOOR',
        'GAMMA_CDF', 'INFINITY', 'INVERSE_BETA_CDF', 'INVERSE_NORMAL_CDF',
        'IS_FINITE', 'IS_INFINITE', 'IS_NAN', 'LAPLACE_CDF', 'LN', 'LOG',
        'LOG10', 'LOG2', 'MOD', 'NAN', 'NORMAL_CDF', 'PI', 'POISSON_CDF',
        'POW', 'POWER', 'RADIANS', 'RAND', 'RANDOM', 'ROUND', 'SIGN', 'SIN',
        'SINH', 'SQRT', 'TAN', 'TANH', 'TO_BASE', 'TRUNCATE', 'WEIBULL_CDF',
        'WIDTH_BUCKET', 'WILSON_INTERVAL_LOWER', 'WILSON_INTERVAL_UPPER',
        
        # ===== DATE/TIME FUNCTIONS =====
        'AT_TIMEZONE', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP',
        'CURRENT_TIMEZONE', 'DATE', 'DATE_ADD', 'DATE_DIFF', 'DATE_FORMAT',
        'DATE_PARSE', 'DATE_TRUNC', 'DAY', 'DAY_OF_MONTH', 'DAY_OF_WEEK',
        'DAY_OF_YEAR', 'DOW', 'DOY', 'EXTRACT', 'FORMAT_DATETIME', 'FROM_ISO8601_DATE',
        'FROM_ISO8601_TIMESTAMP', 'FROM_ISO8601_TIMESTAMP_NANOS', 'FROM_UNIXTIME',
        'FROM_UNIXTIME_NANOS', 'HOUR', 'HUMAN_READABLE_SECONDS', 'LAST_DAY_OF_MONTH',
        'LOCALTIME', 'LOCALTIMESTAMP', 'MILLISECOND', 'MINUTE', 'MONTH',
        'NOW', 'PARSE_DATETIME', 'PARSE_DURATION', 'QUARTER', 'SECOND',
        'TIMESTAMP_OBJECTID', 'TIMEZONE_HOUR', 'TIMEZONE_MINUTE', 'TO_ISO8601',
        'TO_MILLISECONDS', 'TO_UNIXTIME', 'WEEK', 'WEEK_OF_YEAR', 'YEAR',
        'YEAR_OF_WEEK', 'YOW',
        
        # ===== GEOSPATIAL FUNCTIONS (CRITICAL!) =====
        'BING_TILE', 'BING_TILE_AT', 'BING_TILE_COORDINATES', 'BING_TILE_POLYGON',
        'BING_TILE_QUADKEY', 'BING_TILE_ZOOM_LEVEL', 'BING_TILES_AROUND',
        'CONVEX_HULL_AGG', 'FROM_ENCODED_POLYLINE', 'FROM_GEOJSON_GEOMETRY',
        'GEOMETRY_FROM_HADOOP_SHAPE', 'GEOMETRY_INVALID_REASON', 'GEOMETRY_NEAREST_POINTS',
        'GEOMETRY_UNION', 'GEOMETRY_UNION_AGG', 'GREAT_CIRCLE_DISTANCE',
        'LINE_INTERPOLATE_POINT', 'LINE_INTERPOLATE_POINTS', 'LINE_LOCATE_POINT',
        'SIMPLIFY_GEOMETRY', 'SPATIAL_PARTITIONING', 'SPATIAL_PARTITIONS',
        'ST_AREA', 'ST_ASBINARY', 'ST_ASTEXT', 'ST_BOUNDARY', 'ST_BUFFER',
        'ST_CENTROID', 'ST_CONTAINS', 'ST_CONVEXHULL', 'ST_COORDDIM',
        'ST_CROSSES', 'ST_DIFFERENCE', 'ST_DIMENSION', 'ST_DISJOINT',
        'ST_DISTANCE', 'ST_ENDPOINT', 'ST_ENVELOPE', 'ST_ENVELOPEASPTS',
        'ST_EQUALS', 'ST_EXTERIORRING', 'ST_GEOMETRIES', 'ST_GEOMETRYFROMTEXT',
        'ST_GEOMETRYN', 'ST_GEOMETRYTYPE', 'ST_GEOMFROMBINARY', 'ST_INTERIORRINGN',
        'ST_INTERSECTION', 'ST_INTERSECTS', 'ST_ISCLOSED', 'ST_ISEMPTY',
        'ST_ISRING', 'ST_ISSIMPLE', 'ST_ISVALID', 'ST_LENGTH', 'ST_LINEFROMTEXT',
        'ST_LINESTRING', 'ST_MULTIPOINT', 'ST_NUMGEOMETRIES', 'ST_NUMINTERIORRINGS',
        'ST_NUMPOINTS', 'ST_OVERLAPS', 'ST_POINT', 'ST_POINTN', 'ST_POINTS',
        'ST_POLYGON', 'ST_RELATE', 'ST_STARTPOINT', 'ST_SYMDIFFERENCE',
        'ST_TOUCHES', 'ST_UNION', 'ST_WITHIN', 'ST_X', 'ST_XMAX', 'ST_XMIN',
        'ST_Y', 'ST_YMAX', 'ST_YMIN', 'TO_ENCODED_POLYLINE', 'TO_GEOJSON_GEOMETRY',
        'TO_GEOMETRY', 'TO_SPHERICAL_GEOGRAPHY'
        
        # ===== MAP FUNCTIONS =====
        'CARDINALITY', 'ELEMENT_AT', 'MAP', 'MAP_CONCAT', 'MAP_ENTRIES',
        'MAP_FILTER', 'MAP_FROM_ENTRIES', 'MAP_KEYS', 'MAP_VALUES',
        'MAP_ZIP_WITH', 'MULTIMAP_FROM_ENTRIES', 'TRANSFORM_KEYS',
        'TRANSFORM_VALUES',
        
        # ===== JSON FUNCTIONS =====
        'IS_JSON_SCALAR', 'JSON_ARRAY_CONTAINS', 'JSON_ARRAY_GET',
        'JSON_ARRAY_LENGTH', 'JSON_EXTRACT', 'JSON_EXTRACT_SCALAR',
        'JSON_FORMAT', 'JSON_PARSE', 'JSON_QUERY', 'JSON_SIZE', 'JSON_VALUE',
        
        # ===== CONVERSION/CAST FUNCTIONS =====
        'CAST', 'FORMAT', 'PARSE_DATA_SIZE', 'PARSE_PRESTO_DATA_SIZE',
        'TYPEOF', 'TRY', 'TRY_CAST',
        
        # ===== CONDITIONAL FUNCTIONS =====
        'COALESCE', 'IF', 'NULLIF', 'TRY',
        
        # ===== BINARY FUNCTIONS =====
        'CRC32', 'FROM_BASE32', 'FROM_BASE64', 'FROM_BASE64URL', 'FROM_BIG_ENDIAN_32',
        'FROM_BIG_ENDIAN_64', 'FROM_HEX', 'FROM_IEEE754_32', 'FROM_IEEE754_64',
        'HMAC_MD5', 'HMAC_SHA1', 'HMAC_SHA256', 'HMAC_SHA512', 'LENGTH',
        'LPAD', 'MD5', 'MURMUR3', 'REVERSE', 'RPAD', 'SHA1', 'SHA256', 'SHA512',
        'SPOOKY_HASH_V2_32', 'SPOOKY_HASH_V2_64', 'TO_BASE32', 'TO_BASE64',
        'TO_BASE64URL', 'TO_BIG_ENDIAN_32', 'TO_BIG_ENDIAN_64', 'TO_HEX',
        'TO_IEEE754_32', 'TO_IEEE754_64', 'XXHASH64',
        
        # ===== WINDOW FUNCTIONS =====
        'CUME_DIST', 'DENSE_RANK', 'FIRST_VALUE', 'LAG', 'LAST_VALUE',
        'LEAD', 'NTH_VALUE', 'NTILE', 'PERCENT_RANK', 'RANK', 'ROW_NUMBER',
        
        # ===== REGEX FUNCTIONS =====
        'REGEXP_COUNT', 'REGEXP_EXTRACT', 'REGEXP_EXTRACT_ALL', 'REGEXP_LIKE',
        'REGEXP_POSITION', 'REGEXP_REPLACE', 'REGEXP_SPLIT',
        
        # ===== URL FUNCTIONS =====
        'URL_DECODE', 'URL_ENCODE', 'URL_EXTRACT_FRAGMENT', 'URL_EXTRACT_HOST',
        'URL_EXTRACT_PARAMETER', 'URL_EXTRACT_PATH', 'URL_EXTRACT_PORT',
        'URL_EXTRACT_PROTOCOL', 'URL_EXTRACT_QUERY',
        
        # ===== OTHER FUNCTIONS =====
        'GREATEST', 'LEAST', 'UNNEST', 'UUID',
    }


def get_known_invalid_functions():
    """
    Functions that are commonly mistaken for Athena functions but are NOT supported.
    Maps function name to suggested alternative.
    
    Based on common errors and MySQL/PostgreSQL/other SQL dialect confusion.
    """
    return {
        # Geospatial - Common mistakes
        'ST_COVERS': 'Not supported. Use ST_CONTAINS or ST_INTERSECTS instead',
        'ST_GEOGRAPHYFROMTEXT': 'Use ST_GeometryFromText + to_spherical_geography',
        'ST_MAKEPOINT': 'Use ST_Point(longitude, latitude) instead',
        'ST_MAKELINE': 'Not supported. Build LINESTRING manually with ST_GeometryFromText',
        'ST_UNION_AGG': 'Use geometry_union_agg (different name in Athena)',
        'ST_COLLECTIONEXTRACT': 'Not supported in Athena',
        'ST_TRANSFORM': 'Coordinate transformation not supported',
        'ST_SETSRID': 'SRID operations not supported',
        'ST_ASGEOJSON': 'Use to_geojson_geometry instead',
        'ST_GEOMFROMGEOJSON': 'Use from_geojson_geometry instead',
        'GEOMETRY_TYPE': 'Not supported in Athena. Use ST_GeometryType instead, or check with ST_Dimension',
        
        # Array operations - Common mistakes
        'ARRAY_EXISTS': 'Not supported. Use CONTAINS(array, element) or filter(array, x -> condition)',
        'ARRAY_APPEND': 'Use array || ARRAY[element] syntax instead',
        'ARRAY_PREPEND': 'Use ARRAY[element] || array syntax instead',
        'ARRAY_CAT': 'Use CONCAT(array1, array2) or array1 || array2',
        'ARRAY_LENGTH': 'Use cardinality(array) instead',
        'ARRAY_TO_STRING': 'Use array_join(array, delimiter) instead',
        'STRING_TO_ARRAY': 'Use split(string, delimiter) instead',
        'UNNEST_WITH_ORDINALITY': 'Athena supports WITH ORDINALITY but syntax is different',
        
        # Date/Time - Common mistakes
        'DATE_FORMAT': 'Use date_format (lowercase) - case sensitive!',
        'STR_TO_DATE': 'Use date_parse(string, format) instead',
        'UNIX_TIMESTAMP': 'Use to_unixtime(timestamp) instead',
        'FROM_DAYS': 'Not supported. Use date_add or date arithmetic',
        'TO_DAYS': 'Not supported. Use date_diff instead',
        'TIMESTAMPDIFF': 'Use date_diff function with unit parameter',
        'TIMESTAMPADD': 'Use date_add function instead',
        'CURDATE': 'Use CURRENT_DATE',
        'CURTIME': 'Use CURRENT_TIME',
        
        # Aggregate - Common mistakes
        'GROUP_CONCAT': 'Use array_agg(column) then array_join(array, delimiter)',
        'STRING_AGG': 'Use listagg(column, delimiter) WITHIN GROUP (ORDER BY ...) instead',
        'MEDIAN': 'Use approx_percentile(column, 0.5) instead',
        'PERCENTILE_CONT': 'Use approx_percentile instead',
        'PERCENTILE_DISC': 'Use approx_percentile instead',
        
        # Type conversion - Common mistakes
        'TO_CHAR': 'Use CAST(value AS VARCHAR) or format() instead',
        'TO_NUMBER': 'Use CAST(value AS DOUBLE) or CAST(value AS BIGINT)',
        'TO_DATE': 'Use CAST(value AS DATE) or date_parse',
        'CONVERT': 'Use CAST instead',
        
        # String - Common mistakes
        'INSTR': 'Use strpos(string, substring) instead',
        'LOCATE': 'Use strpos(string, substring) instead',
        'LEFT': 'Use substr(string, 1, length) instead',
        'RIGHT': 'Use substr(string, -length) instead',
        'MID': 'Use substr(string, start, length) instead',
        'SPACE': 'Use repeat(\' \', n) instead',
        'CHAR_LENGTH': 'Use length(string) instead',
        
        # Conditional - Common mistakes  
        'IFNULL': 'Use COALESCE(value, default) instead',
        'NVL': 'Use COALESCE(value, default) instead',
        'ISNULL': 'Use value IS NULL or COALESCE instead',
        'DECODE': 'Use CASE WHEN ... THEN ... END instead',
    }


def extract_functions_from_sql(sql: str) -> list:
    """
    Extract all function calls from SQL query using regex.
    Filters out SQL keywords to return only actual functions.
    
    Args:
        sql: SQL query string
        
    Returns:
        Sorted list of unique function names (uppercase)
    """
    # Pattern: word followed by opening parenthesis
    sql_cleaned = re.sub(r"'[^']*'", '', sql)
    sql_cleaned = re.sub(r'"[^"]*"', '', sql_cleaned)
    pattern = r'\b([A-Z_][A-Z0-9_]*)\s*\('
    matches = re.findall(pattern, sql_cleaned, re.IGNORECASE)
    
    # Comprehensive SQL keywords to exclude (these aren't functions)
    sql_keywords = {
        # DML
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 
        'OUTER', 'CROSS', 'ON', 'AND', 'OR', 'AS', 'IN', 'EXISTS',
        'NOT', 'BETWEEN', 'LIKE', 'IS', 'NULL', 'FULL',
        
        # DDL
        'CREATE', 'ALTER', 'DROP', 'TABLE', 'VIEW', 'INDEX', 'SCHEMA',
        
        # Control flow
        'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF',
        
        # Aggregation/Grouping
        'WITH', 'HAVING', 'GROUP', 'ORDER', 'PARTITION', 'OVER',
        'WINDOW', 'ROWS', 'RANGE', 'BY',
        
        # Set operations
        'UNION', 'INTERSECT', 'EXCEPT', 'MINUS', 'ALL',
        
        # Subqueries
        'ANY', 'SOME',
        
        # Other
        'INSERT', 'UPDATE', 'DELETE', 'INTO', 'VALUES', 'SET',
        'LIMIT', 'OFFSET', 'FETCH', 'DISTINCT', 'UNIQUE', 'USING'
    }
    
    # Extract unique functions, excluding keywords
    functions = set()
    for func in matches:
        func_upper = func.upper()
        if func_upper not in sql_keywords:
            functions.add(func_upper)
    
    return sorted(functions)


def validate_sql_node(state: GraphState) -> Dict:
    """
    Enhanced two-stage SQL validation:
    Stage 1: Function existence and usage validation
    Stage 2: Syntax and structure validation
    """
    
    print("\n" + "="*80)
    print("SQL VALIDATION PIPELINE - TWO-STAGE")
    print("="*80)
    
    generated_sql = state["generated_sql"]
    
    # STAGE 1: FUNCTION VALIDATION

    print("\n### STAGE 1: FUNCTION VALIDATION")
    print("-" * 80)
    
    # Extract all functions
    functions = extract_functions_from_sql(generated_sql)
    
    if not functions:
        print("   No functions found, skipping function validation")
        function_validated_sql = generated_sql
    else:
        print(f"   Extracted {len(functions)} functions")
        
        # Classify functions
        supported_funcs = get_athena_supported_functions()
        invalid_funcs = get_known_invalid_functions()
        
        validation_result = {
            'supported': [],
            'suspicious': [],
            'invalid': []
        }
        
        for func in functions:
            func_upper = func.upper()
            if func_upper in invalid_funcs:
                validation_result['invalid'].append({
                    'function': func,
                    'issue': invalid_funcs[func_upper]
                })
            elif func_upper in supported_funcs:
                validation_result['supported'].append(func)
            else:
                validation_result['suspicious'].append(func)
        
        print(f"   Supported: {len(validation_result['supported'])}")
        print(f"   Suspicious: {len(validation_result['suspicious'])}")
        print(f"   Invalid: {len(validation_result['invalid'])}")
        
        # RAG ALL functions for usage validation
        vectorstore = _get_function_vectorstore()
        all_functions_with_docs = {}
        
        if vectorstore:
            print(f"\n   Retrieving usage docs for ALL {len(functions)} functions...")
            
            for func in functions:
                try:
                    # Query vectorstore for this function
                    search_query = f"{func} Athena SQL function syntax parameters usage example"
                    
                    retriever = vectorstore.as_retriever(
                        search_type="similarity",
                        search_kwargs={"k": 2}  
                    )
                    docs = retriever.invoke(search_query)

                    if docs and len(docs) > 0:
                        content_preview = docs[0].page_content[:100]
                        print(f"   ✓ {func}: {content_preview}...")
                    
                    all_functions_with_docs[func] = docs
                    
                except Exception as e:
                    print(f"   Warning: RAG failed for {func}: {str(e)[:40]}")
                    all_functions_with_docs[func] = []
            
            docs_retrieved = sum(1 for docs in all_functions_with_docs.values() if docs)
            print(f"   Retrieved docs for {docs_retrieved}/{len(functions)} functions")
        else:
            print("   Warning: No vectorstore available")
        
        # Call LLM for function validation
        if validation_result['suspicious'] or validation_result['invalid'] or all_functions_with_docs:
            print("\n   Calling LLM for function validation...")
            
            azure_config = {
                "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
                "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
                "azure_deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
                "api_version": os.getenv("AZURE_OPENAI_API_VERSION")
            }
            
            function_prompt = create_function_validation_prompt(
                generated_sql=generated_sql,
                all_functions_with_docs=all_functions_with_docs,
                suspicious_functions=validation_result['suspicious'],
                invalid_functions=validation_result['invalid'],
                schema=state["final_schema"]
            )
            
            log_llm_interaction(
                step_name="function_validation_start",
                prompt=None,
                response=None,
                context=f"Validating {len(functions)} functions"
            )
            
            client = AzureOpenAI(**azure_config)
            
            try:
                response = client.chat.completions.create(
                    model=azure_config["azure_deployment"],
                    messages=[
                        {
                            "role": "system",
                            "content": "You are an AWS Athena SQL function validator. Validate function existence and correct usage."
                        },
                        {
                            "role": "user",
                            "content": function_prompt
                        }
                    ],
                    temperature=1
                )
                
                function_validated_sql = response.choices[0].message.content.strip()
                function_validated_sql = _format_sql_query(function_validated_sql)
                
                log_llm_interaction(
                    step_name="function_validation_complete",
                    prompt=function_prompt[:500] + "...",
                    response=function_validated_sql[:500] + "...",
                    context=f"Validated {len(functions)} functions"
                )
                
                # Check if SQL changed
                if function_validated_sql.strip() != generated_sql.strip():
                    print("   SQL modified during function validation")
                else:
                    print("   SQL passed function validation unchanged")
                
            except Exception as e:
                print(f"   Error in function validation: {str(e)}")
                function_validated_sql = generated_sql
        else:
            print("   All functions valid, skipping LLM call")
            function_validated_sql = generated_sql
    

    # STAGE 2: SYNTAX VALIDATION

    print("\n### STAGE 2: SYNTAX VALIDATION")
    print("-" * 80)
    
    # Load dynamic errors from errors.txt
    errors_txt_path = Path("errors.txt")
    errors_txt_content = ""
    
    if errors_txt_path.exists():
        try:
            with open(errors_txt_path, 'r', encoding='utf-8') as f:
                errors_txt_content = f.read()
            
            # Count errors in file
            error_count = errors_txt_content.count('[')
            print(f"   Loaded {error_count} production error patterns from errors.txt")
        except Exception as e:
            print(f"   Warning: Failed to load errors.txt: {str(e)}")
    else:
        print("   errors.txt not found (no production errors yet)")
    
    # Call LLM for syntax validation
    print("\n   Calling LLM for syntax validation...")
    
    azure_config = {
        "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
        "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
        "azure_deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        "api_version": os.getenv("AZURE_OPENAI_API_VERSION")
    }
    
    syntax_prompt = create_syntax_validation_prompt(
        function_validated_sql=function_validated_sql,
        errors_txt_content=errors_txt_content,
        schema=state["final_schema"]
    )
    
    log_llm_interaction(
        step_name="syntax_validation_start",
        prompt=None,
        response=None,
        context="Validating SQL syntax and structure"
    )
    
    client = AzureOpenAI(**azure_config)
    
    try:
        response = client.chat.completions.create(
            model=azure_config["azure_deployment"],
            messages=[
                {
                    "role": "system",
                    "content": "You are an AWS Athena SQL syntax validator. Fix ONLY syntax issues, do NOT modify functions."
                },
                {
                    "role": "user",
                    "content": syntax_prompt
                }
            ],
            temperature=1
        )
        
        final_validated_sql = response.choices[0].message.content.strip()
        final_validated_sql = _format_sql_query(final_validated_sql)
        
        log_llm_interaction(
            step_name="syntax_validation_complete",
            prompt=syntax_prompt[:500] + "...",
            response=final_validated_sql[:500] + "...",
            context="Syntax validation complete"
        )
        
        # Check if SQL changed
        if final_validated_sql.strip() != function_validated_sql.strip():
            print("   SQL modified during syntax validation")
        else:
            print("   SQL passed syntax validation unchanged")
        
    except Exception as e:
        print(f"   Error in syntax validation: {str(e)}")
        final_validated_sql = function_validated_sql
    

    # VALIDATION SUMMARY

    print("\n### VALIDATION SUMMARY")
    print("-" * 80)
    
    sql_changed = final_validated_sql.strip() != generated_sql.strip()
    
    if sql_changed:
        orig_lines = len(generated_sql.split('\n'))
        new_lines = len(final_validated_sql.split('\n'))
        print(f"   SQL was modified: {orig_lines} -> {new_lines} lines")
    else:
        print("   SQL passed all validation unchanged")
    
    print("="*80 + "\n")
    
    return {
        "generated_sql": final_validated_sql,
        "validation_performed": True,
        "error_message": None
    }


def execute_sql_node(state: GraphState) -> Dict:
    """
    Node 2: Execute SQL on Athena.
    
    Creates CTAS table instead of direct SELECT for result persistence.
    """
    log_query_execution(
        rule_category=state["rule_category"],
        database=state["database_name"],
        sql=state["generated_sql"],
        status="executing"
    )
    
    try:
        athena_config = Config()
        athena_client = AthenaClient(athena_config)
        
        # Extract database name
        database_name = _extract_database_from_ddl(state["final_schema"])
        
        # Generate CTAS table name
        ctas_table_name = generate_ctas_name(
            rule_category=state["rule_category"],
            database=database_name
        )
        
        print(f"[CTAS] Creating table: {ctas_table_name}")
        
        # Step 1: Create CTAS
        ctas_sql = f"""
CREATE TABLE {ctas_table_name} AS
{state["generated_sql"]}
"""
        
        print(f"[CTAS] Executing CTAS SQL:")
        print(ctas_sql[:500] + "...")
        
        ctas_request = QueryRequest(
            database=database_name,
            query=ctas_sql,
            max_rows=1  # CTAS doesn't return rows
        )
        
        # Execute CTAS creation
        ctas_result = asyncio.run(athena_client.execute_query(ctas_request))
        
        if isinstance(ctas_result, str):
            # Timeout
            error_msg = f"CTAS creation timed out. Execution ID: {ctas_result}"
            log_query_execution(
                rule_category=state["rule_category"],
                database=database_name,
                sql=ctas_sql,
                status="timeout",
                error=error_msg
            )
            return {
                "error_message": error_msg,
                "query_result": None,
                "execution_id": ctas_result
            }
        
        print(f"[CTAS] Table created successfully: {ctas_table_name}")
        
        # Step 2: Query CTAS for preview (top 1000)
        preview_sql = f"SELECT * FROM {ctas_table_name} LIMIT 1000"
        
        print(f"[CTAS] Querying preview: {preview_sql}")
        
        preview_request = QueryRequest(
            database=database_name,
            query=preview_sql,
            max_rows=1000
        )
        
        preview_result = asyncio.run(athena_client.execute_query(preview_request))
        
        if isinstance(preview_result, str):
            error_msg = f"Preview query timed out. Execution ID: {preview_result}"
            return {
                "error_message": error_msg,
                "query_result": None,
                "execution_id": preview_result,
                "ctas_table_name": ctas_table_name
            }
        
        # Success
        df = pd.DataFrame(preview_result.rows, columns=preview_result.columns)
        
        # S3 path for CTAS data
        s3_base = athena_config.s3_output_location.rstrip('/')
        s3_path = f"{s3_base}/{ctas_result.query_execution_id}.csv"
        
        print(f"[CTAS] Preview retrieved: {len(df)} rows")
        print(f"[CTAS] S3 path: {s3_path}")
        
        log_query_execution(
            rule_category=state["rule_category"],
            database=database_name,
            sql=state["generated_sql"],
            status="success",
            execution_id=ctas_result.query_execution_id,
            bytes_scanned=ctas_result.bytes_scanned,
            execution_time_ms=ctas_result.execution_time_ms,
            row_count=len(df)
        )
        
        return {
            "query_result": df,
            "error_message": None,
            "s3_result_path": s3_path,
            "ctas_table_name": ctas_table_name,
            "execution_id": ctas_result.query_execution_id,
            "bytes_scanned": ctas_result.bytes_scanned,
            "execution_time_ms": ctas_result.execution_time_ms,
            "row_count": len(preview_result.rows),
            "database_name": database_name
        }
    
    except AthenaError as e:
        log_query_execution(
            rule_category=state["rule_category"],
            database=state.get("database_name", "unknown"),
            sql=state["generated_sql"],
            status="failed",
            error=e.message
        )
        return {
            "error_message": e.message,
            "query_result": None
        }
    
    except Exception as e:
        error_msg = str(e)
        log_query_execution(
            rule_category=state["rule_category"],
            database=state.get("database_name", "unknown"),
            sql=state["generated_sql"],
            status="failed",
            error=error_msg
        )
        return {
            "error_message": error_msg,
            "query_result": None
        }


def fix_sql_node(state: GraphState) -> Dict:
    """Node: Fix SQL based on error with RAG enhancement."""
    retry_num = state["retries"] + 1
    log_llm_interaction("fix_sql_start", None, None, f"Retry {retry_num}: {state['error_message'][:200]}")
    
    azure_config = {
        "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
        "azure_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
        "azure_deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        "api_version": os.getenv("AZURE_OPENAI_API_VERSION")
    }
    
    
    relevant_docs = []
    vectorstore = _get_docs_vectorstore() 
    
    if vectorstore is not None:
        try:
            #docs found on basis of NL query + error message
            error_search_query = f"{state['nl_query']} {state['error_message'][:500]}"
            
            # Determine optimal k (errors usually need more context)
            optimal_k = _determine_optimal_k(state["nl_query"], error_context=state["error_message"])
            
            retriever = vectorstore.as_retriever(
                search_type="similarity",
                search_kwargs={"k": optimal_k}
            )
            relevant_docs = retriever.invoke(error_search_query)
            
            if relevant_docs:
                print(f"RAG (Fix): Retrieved {len(relevant_docs)} relevant doc chunks for error (k={optimal_k})")
            else:
                print("RAG (Fix): No relevant docs found for this error")
                
        except Exception as e:
            print(f"RAG (Fix) retrieval failed: {str(e)}")
            relevant_docs = []
    
    # RAG enhanced
    if relevant_docs:
        prompt = create_rag_sql_fixing_prompt(
            schema=state["final_schema"],
            query=state["nl_query"],
            broken_sql=state["generated_sql"],
            error_message=state["error_message"],
            relevant_docs=relevant_docs
        )
    else:
        # Fallback to original fixing prompt
        prompt = create_sql_fixing_prompt(
            schema=state["final_schema"],
            query=state["nl_query"],
            broken_sql=state["generated_sql"],
            error_message=state["error_message"]
        )
    
    client = AzureOpenAI(**azure_config)
    response = client.chat.completions.create(
        model=azure_config["azure_deployment"],
        messages=[{"role": "user", "content": prompt}],
        temperature=1
    )
    
    raw_sql = response.choices[0].message.content
    fixed_sql = _format_sql_query(raw_sql)
    
    log_llm_interaction("fix_sql_complete", prompt, fixed_sql, f"Retry {retry_num}")
    
    return {
        "generated_sql": fixed_sql,
        "retries": retry_num
    }


def decide_what_to_do_next(state: GraphState) -> str:
    """Router: retry, end, or fail."""
    if state["error_message"]:
        if state["retries"] < 5:
            return "fix_sql"
        else:
            return END
    else:
        return END



# BUILD LANGGRAPH WORKFLOW


workflow = StateGraph(GraphState)
workflow.add_node("generate_sql", generate_sql_node)
workflow.add_node("validate_sql", validate_sql_node)
workflow.add_node("execute_sql", execute_sql_node)
workflow.add_node("fix_sql", fix_sql_node)

workflow.set_entry_point("generate_sql")
workflow.add_edge("generate_sql", "validate_sql")  
workflow.add_edge("validate_sql", "execute_sql")   
workflow.add_edge("fix_sql", "execute_sql")
workflow.add_conditional_edges(
    "execute_sql",
    decide_what_to_do_next,
    {"fix_sql": "fix_sql", END: END}
)

langgraph_app = workflow.compile()



# ORCHESTRATOR ENTRY POINT


def run_orchestrator(
    query: str,
    schema: str,
    guardrails: str,
    rule_category: str,
    execution_mode: str = "normal"  #'normal', 'reexecute', 'force'
) -> Generator[str | Dict[str, Any], None, None]:
    """
    Run the orchestration workflow with progress updates.
    
    execution_mode:
        - 'normal': Use cache if available
        - 'reexecute': Use cached SQL but create new CTAS
        - 'force': Ignore cache, generate new SQL + CTAS
    
    Yields progress strings for UI display, then yields final result dict.
    """
    # Extract database name
    try:
        database_name = _extract_database_from_ddl(schema)
    except ValueError as e:
        yield f"Error: {str(e)}"
        yield {
            "result_df": None,
            "final_sql": None,
            "error": str(e),
            "s3_path": None,
            "ctas_table_name": None,
            "execution_id": None,
            "bytes_scanned": 0,
            "execution_time_ms": 0,
            "row_count": 0,
            "cache_hit": False,
            "rag_used": False
        }
        return
    
    cache_mgr = CacheManager()
    
    # Check cache (unless force mode)
    cached_result = None
    if execution_mode != "force":
        yield "Checking cache..."
        cached_result = cache_mgr.get_cached_result(rule_category, database_name, query)
    
    # Handle different execution modes
    if execution_mode == "normal" and cached_result:
        # Mode 1: Use cached CTAS
        yield f"Cache hit! Using existing CTAS from {cached_result['age_hours']:.1f} hours ago"
        
        try:
            # Query the cached CTAS for preview
            ctas_name = cached_result['ctas_table_name']
            preview_sql = f"SELECT * FROM {ctas_name} LIMIT 1000"
            
            athena_config = Config()
            athena_client = AthenaClient(athena_config)
            
            preview_request = QueryRequest(
                database=database_name,
                query=preview_sql,
                max_rows=1000
            )
            
            preview_result = asyncio.run(athena_client.execute_query(preview_request))
            df = pd.DataFrame(preview_result.rows, columns=preview_result.columns)
            
            yield {
                "result_df": df,
                "final_sql": cached_result['sql'],
                "error": None,
                "s3_path": cached_result['s3_path'],
                "ctas_table_name": ctas_name,
                "execution_id": cached_result['execution_id'],
                "bytes_scanned": cached_result['bytes_scanned'],
                "execution_time_ms": cached_result['execution_time_ms'],
                "row_count": cached_result['row_count'],
                "cache_hit": True,
                "cached_age_hours": cached_result['age_hours'],
                "rag_used": False
            }
            return
        except Exception as e:
            yield f"Cache read failed: {str(e)}. Running fresh query..."
    
    elif execution_mode == "reexecute" and cached_result:
        # Mode 2: Re-execute cached SQL (create new CTAS with new date)
        yield f"Re-executing cached SQL on current data..."
        yield f"Using SQL from {cached_result['age_hours']:.1f} hours ago"
        
        # Use cached SQL directly (skip generation)
        inputs = {
            "nl_query": query,
            "final_schema": schema,
            "guardrails": guardrails,
            "rule_category": rule_category,
            "database_name": database_name,
            "generated_sql": cached_result['sql'],  # Use cached SQL!
            "validation_performed": True,  # Skip validation too
            "retries": 0,
            "bytes_scanned": 0,
            "execution_time_ms": 0,
            "row_count": 0
        }
        
        # Jump directly to execution node
        yield "Creating new CTAS with cached SQL..."
        exec_result = execute_sql_node(inputs)
        
        if exec_result.get("error_message"):
            yield {
                "result_df": None,
                "final_sql": cached_result['sql'],
                "error": exec_result["error_message"],
                "s3_path": None,
                "ctas_table_name": None,
                "execution_id": exec_result.get("execution_id"),
                "bytes_scanned": 0,
                "execution_time_ms": 0,
                "row_count": 0,
                "cache_hit": False,
                "rag_used": False
            }
            return
        
        # Cache the new result
        cache_mgr.cache_result(
            rule_category=rule_category,
            database=database_name,
            nl_query=query,
            sql=cached_result['sql'],
            execution_id=exec_result["execution_id"],
            s3_path=exec_result["s3_result_path"],
            ctas_table_name=exec_result["ctas_table_name"],
            execution_type='ctas',
            bytes_scanned=exec_result.get("bytes_scanned", 0),
            execution_time_ms=exec_result.get("execution_time_ms", 0),
            row_count=exec_result.get("row_count", 0)
        )
        
        yield {
            "result_df": exec_result["query_result"],
            "final_sql": cached_result['sql'],
            "error": None,
            "s3_path": exec_result["s3_result_path"],
            "ctas_table_name": exec_result["ctas_table_name"],
            "execution_id": exec_result["execution_id"],
            "bytes_scanned": exec_result.get("bytes_scanned", 0),
            "execution_time_ms": exec_result.get("execution_time_ms", 0),
            "row_count": exec_result.get("row_count", 0),
            "cache_hit": False,
            "reexecuted": True,
            "rag_used": False
        }
        return
    
    else:
        # Mode 3: Force refresh OR no cache - run full workflow
        if execution_mode == "force":
            yield "Force refresh - generating new SQL..."
        else:
            yield "No cache found - generating SQL..."
        
        inputs = {
            "nl_query": query,
            "final_schema": schema,
            "guardrails": guardrails,
            "rule_category": rule_category,
            "database_name": database_name,
            "retries": 0,
            "bytes_scanned": 0,
            "execution_time_ms": 0,
            "row_count": 0
        }
        
        accumulated_state = {}
        
        # Stream through graph
        for event in langgraph_app.stream(inputs):
            if "generate_sql" in event:
                accumulated_state.update(event["generate_sql"])
                yield "SQL generated successfully"
                yield "Validating SQL..."
            
            elif "validate_sql" in event:
                accumulated_state.update(event["validate_sql"])
                if event["validate_sql"].get("validation_performed"):
                    yield "SQL validation complete"
                yield "Creating CTAS on AWS Athena..."
            
            elif "execute_sql" in event:
                exec_state = event["execute_sql"]
                accumulated_state.update(exec_state)
                
                if exec_state.get("error_message"):
                    error = exec_state["error_message"][:100]
                    yield f"Execution failed: {error}..."
                else:
                    yield "CTAS created successfully!"
            
            elif "fix_sql" in event:
                accumulated_state.update(event["fix_sql"])
                retry_count = event["fix_sql"]["retries"]
                yield f"Attempting fix (Retry {retry_count}/5)..."
                yield "Re-executing on Athena..."
            
            if "__end__" in event:
                accumulated_state.update(event["__end__"])
                break
        
        final_state = accumulated_state
        
        if not final_state or not final_state.get("generated_sql"):
            yield "Error: Workflow completed without generating SQL"
            yield {
                "result_df": None,
                "final_sql": None,
                "error": "Workflow ended without generating SQL",
                "s3_path": None,
                "ctas_table_name": None,
                "execution_id": None,
                "bytes_scanned": 0,
                "execution_time_ms": 0,
                "row_count": 0,
                "cache_hit": False,
                "rag_used": False
            }
            return
        
        # Cache successful result
        if not final_state.get("error_message") and final_state.get("ctas_table_name"):
            cache_mgr.cache_result(
                rule_category=rule_category,
                database=database_name,
                nl_query=query,
                sql=final_state["generated_sql"],
                execution_id=final_state["execution_id"],
                s3_path=final_state["s3_result_path"],
                ctas_table_name=final_state["ctas_table_name"],
                execution_type='ctas',
                bytes_scanned=final_state.get("bytes_scanned", 0),
                execution_time_ms=final_state.get("execution_time_ms", 0),
                row_count=final_state.get("row_count", 0)
            )
        
        vectorstore = _get_docs_vectorstore()
        rag_was_used = vectorstore is not None
        
        # Yield final result
        yield {
            "result_df": final_state.get("query_result"),
            "final_sql": final_state.get("generated_sql"),
            "error": final_state.get("error_message"),
            "s3_path": final_state.get("s3_result_path"),
            "ctas_table_name": final_state.get("ctas_table_name"),
            "execution_id": final_state.get("execution_id"),
            "bytes_scanned": final_state.get("bytes_scanned", 0),
            "execution_time_ms": final_state.get("execution_time_ms", 0),
            "row_count": final_state.get("row_count", 0),
            "cache_hit": False,
            "rag_used": rag_was_used
        }