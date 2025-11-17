import sqlite3
from pathlib import Path
from datetime import datetime
import pandas as pd
import streamlit as st
import io


CACHE_DB = Path("query_cache.db")
LOG_DB = Path("query_logs.db")


@st.cache_resource(show_spinner=False)
def get_conn(db_path: Path):
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def read_df(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    return pd.read_sql_query(sql, conn, params=params)

def exec_sql(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    return cur.rowcount

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

def to_json_bytes(df: pd.DataFrame, ndjson: bool = False) -> bytes:
    if ndjson:
        content = df.to_json(orient="records", lines=True, force_ascii=False)
    else:
        content = df.to_json(orient="records", force_ascii=False)
    return content.encode("utf-8")


def tag(text, color="#0366d6"):
    st.markdown(
        f"<span style='background:{color}20;padding:2px 6px;border-radius:6px;color:{color};font-size:0.85rem;'>{text}</span>",
        unsafe_allow_html=True
    )

def line():
    st.markdown("<hr style='margin:8px 0;'>", unsafe_allow_html=True)


def page_cache():
    st.header("Cache: query_cache")

    try:
        conn = get_conn(CACHE_DB)
    except FileNotFoundError as e:
        st.error(str(e))
        return

    # Cache statistics at top
    st.subheader("📊 Cache Statistics")
    try:
        stats_sql = """
        SELECT 
            COUNT(*) as total_entries,
            COUNT(CASE WHEN ctas_table_name IS NOT NULL THEN 1 END) as ctas_entries,
            COUNT(CASE WHEN execution_type = 'direct' THEN 1 END) as direct_entries,
            AVG(bytes_scanned) / (1024*1024) as avg_mb_scanned,
            AVG(execution_time_ms) / 1000 as avg_seconds
        FROM query_cache
        """
        stats_df = read_df(conn, stats_sql)
        if not stats_df.empty:
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Entries", int(stats_df['total_entries'].iloc[0]))
            with col2:
                st.metric("CTAS Tables", int(stats_df['ctas_entries'].iloc[0]))
            with col3:
                st.metric("Direct Queries", int(stats_df['direct_entries'].iloc[0]))
            with col4:
                st.metric("Avg Data Scanned", f"{stats_df['avg_mb_scanned'].iloc[0]:.1f} MB")
            with col5:
                st.metric("Avg Exec Time", f"{stats_df['avg_seconds'].iloc[0]:.1f}s")
    except Exception as e:
        st.warning(f"Could not load statistics: {str(e)}")
    
    line()

    # Filters
    with st.expander("🔍 Filters", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            rule_filter = st.text_input("Rule category contains", "", key="cache_rule_filter")
        with col2:
            db_filter = st.text_input("Database contains", "", key="cache_db_filter")
        with col3:
            exec_type_filter = st.selectbox(
                "Execution Type",
                options=["(any)", "ctas", "direct"],
                index=0,
                key="cache_exec_type"
            )
        
        limit = st.number_input("Max rows", min_value=10, max_value=10000, value=200, step=10, key="cache_limit")

    # Build query with new columns
    params = [f"%{rule_filter}%", rule_filter, f"%{db_filter}%", db_filter]
    exec_type_clause = ""
    if exec_type_filter != "(any)":
        exec_type_clause = "AND execution_type = ?"
        params.append(exec_type_filter)
    params.append(int(limit))

    sql = f"""
    SELECT
      id,
      rule_category,
      database_name,
      execution_type,
      ctas_table_name,
      execution_id,
      s3_result_path,
      row_count,
      bytes_scanned,
      execution_time_ms,
      created_at
    FROM query_cache
    WHERE (rule_category LIKE ? OR ? = '')
      AND (database_name LIKE ? OR ? = '')
      {exec_type_clause}
    ORDER BY created_at DESC
    LIMIT ?
    """
    df = read_df(conn, sql, tuple(params))

    st.subheader("📋 Cache Entries")
    if df.empty:
        st.info("No cache rows matched.")
    else:
        # Add visual indicators
        display_df = df.copy()
        if 'execution_type' in display_df.columns:
            display_df['execution_type'] = display_df['execution_type'].apply(
                lambda x: f"🏗️ {x}" if x == 'ctas' else f"⚡ {x}" if x else "❓"
            )
        
        st.dataframe(display_df, use_container_width=True, height=420)

        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "📥 Export CSV",
                data=to_csv_bytes(df),
                file_name=f"cache_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="cache_export_csv"
            )
        with col2:
            st.download_button(
                "📥 Export JSON",
                data=to_json_bytes(df),
                file_name=f"cache_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                key="cache_export_json"
            )

        line()
        st.subheader("🔍 Inspect a Cache Entry")
        row_id = st.number_input("Row id", min_value=0, value=0, step=1, key="cache_row_id")
        if st.button("Show cache row", key="cache_show_row_btn"):
            if row_id > 0:
                r = read_df(conn, "SELECT * FROM query_cache WHERE id = ?", (row_id,))
                if r.empty:
                    st.warning("No row with that id.")
                else:
                    rec = r.iloc[0].to_dict()
                    
                    # Show metadata
                    meta_cols = st.columns(3)
                    with meta_cols[0]:
                        st.metric("Rule Category", rec.get('rule_category', 'N/A'))
                    with meta_cols[1]:
                        st.metric("Execution Type", rec.get('execution_type', 'N/A').upper())
                    with meta_cols[2]:
                        age = (datetime.now() - datetime.fromisoformat(rec['created_at']))
                        age_hours = age.total_seconds() / 3600
                        st.metric("Age", f"{age_hours:.1f} hours")
                    
                    # Show CTAS info if exists
                    if rec.get('ctas_table_name'):
                        st.info(f"**CTAS Table:** `{rec['ctas_table_name']}`")
                    
                    # Show key metrics
                    metric_cols = st.columns(3)
                    with metric_cols[0]:
                        st.metric("Rows", f"{rec.get('row_count', 0):,}")
                    with metric_cols[1]:
                        mb = rec.get('bytes_scanned', 0) / (1024 * 1024)
                        st.metric("Data Scanned", f"{mb:.2f} MB")
                    with metric_cols[2]:
                        secs = rec.get('execution_time_ms', 0) / 1000
                        st.metric("Exec Time", f"{secs:.2f}s")
                    
                    # Show S3 path
                    if rec.get('s3_result_path'):
                        with st.expander("S3 Result Path"):
                            st.code(rec['s3_result_path'], language="text")
                    
                    # Show queries
                    with st.expander("NL Query", expanded=False):
                        st.code(rec.get("nl_query_text") or "", language="text")
                    with st.expander("Final SQL", expanded=True):
                        st.code(rec.get("final_sql") or "", language="sql")
            else:
                st.info("Enter a valid id.")

        line()
        st.subheader("⚠️ Danger Zone")
        col1, col2 = st.columns(2)
        with col1:
            del_id = st.number_input("Delete cache row id", min_value=0, value=0, step=1, key="del_cache")
            if st.button("🗑️ Delete row from cache", key="del_cache_btn"):
                if del_id > 0:
                    n = exec_sql(conn, "DELETE FROM query_cache WHERE id = ?", (del_id,))
                    st.success(f"Deleted {n} row(s).")
                    st.rerun()
                else:
                    st.info("Enter a valid id.")
        
        with col2:
            if st.button("🗑️ Clear ALL cache (destructive!)", key="clear_all_cache"):
                confirm = st.checkbox("I understand this will delete ALL cache entries", key="confirm_clear_cache")
                if confirm:
                    n = exec_sql(conn, "DELETE FROM query_cache", ())
                    st.success(f"Cleared {n} cache entries.")
                    st.rerun()


def page_logs():
    st.header("Logs: query_executions and llm_interactions")

    try:
        conn = get_conn(LOG_DB)
    except FileNotFoundError as e:
        st.error(str(e))
        return

    tab1, tab2, tab3 = st.tabs(["📊 Query Executions", "🤖 LLM Interactions", "🔍 Search"])

    # ----------------- Query executions -----------------
    with tab1:
        # Statistics
        st.subheader("📈 Execution Statistics")
        try:
            stats_sql = """
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as success,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                AVG(CASE WHEN status = 'success' THEN bytes_scanned END) / (1024*1024) as avg_mb,
                AVG(CASE WHEN status = 'success' THEN execution_time_ms END) / 1000 as avg_sec
            FROM query_executions
            """
            stats_df = read_df(conn, stats_sql)
            if not stats_df.empty:
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Total", int(stats_df['total'].iloc[0]))
                with col2:
                    success = int(stats_df['success'].iloc[0])
                    st.metric("Success", success, delta=None if success == 0 else "✓")
                with col3:
                    failed = int(stats_df['failed'].iloc[0])
                    st.metric("Failed", failed, delta=None if failed == 0 else "✗")
                with col4:
                    st.metric("Avg Scanned", f"{stats_df['avg_mb'].iloc[0]:.1f} MB")
                with col5:
                    st.metric("Avg Time", f"{stats_df['avg_sec'].iloc[0]:.1f}s")
        except Exception as e:
            st.warning(f"Could not load statistics: {str(e)}")
        
        line()

        with st.expander("🔍 Filters", expanded=False):
            col1, col2, col3 = st.columns(3)
            with col1:
                rule_filter = st.text_input("Rule category contains", "", key="logs_rule_filter")
            with col2:
                db_filter = st.text_input("Database contains", "", key="logs_db_filter")
            with col3:
                status_filter = st.selectbox(
                    "Status",
                    options=["(any)", "executing", "success", "failed", "timeout"],
                    index=0,
                    key="logs_status_filter"
                )
            
            limit = st.number_input("Max rows", min_value=10, max_value=10000, value=300, step=10, key="logs_limit")

        params = [f"%{rule_filter}%", rule_filter, f"%{db_filter}%", db_filter]
        status_clause = ""
        if status_filter != "(any)":
            status_clause = "AND status = ?"
            params.append(status_filter)
        params.append(int(limit))

        q = f"""
        SELECT
          id,
          timestamp,
          rule_category,
          database_name,
          status,
          execution_id,
          row_count,
          bytes_scanned,
          execution_time_ms
        FROM query_executions
        WHERE (rule_category LIKE ? OR ? = '')
          AND (database_name LIKE ? OR ? = '')
          {status_clause}
        ORDER BY timestamp DESC
        LIMIT ?
        """
        df = read_df(conn, q, tuple(params))

        st.subheader("📋 Executions")
        if df.empty:
            st.info("No execution rows matched.")
        else:
            # Add status emoji
            display_df = df.copy()
            if 'status' in display_df.columns:
                display_df['status'] = display_df['status'].apply(
                    lambda x: f"✅ {x}" if x == 'success' else f"❌ {x}" if x == 'failed' else f"⏳ {x}" if x == 'executing' else f"⏱️ {x}"
                )
            
            st.dataframe(display_df, use_container_width=True, height=420)

            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "📥 Export CSV",
                    data=to_csv_bytes(df),
                    file_name=f"executions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="exec_export_csv"
                )
            with col2:
                st.download_button(
                    "📥 Export JSON",
                    data=to_json_bytes(df),
                    file_name=f"executions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    key="exec_export_json"
                )

        line()
        st.subheader("🔍 Inspect an Execution")
        row_id = st.number_input("Execution row id", min_value=0, value=0, step=1, key="logs_row_id")
        if st.button("Show execution details", key="exec_show_row_btn"):
            if row_id > 0:
                r = read_df(conn, "SELECT * FROM query_executions WHERE id = ?", (row_id,))
                if r.empty:
                    st.warning("No row with that id.")
                else:
                    rec = r.iloc[0].to_dict()
                    
                    # Status badge
                    status = rec.get('status', 'unknown')
                    if status == 'success':
                        st.success(f"Status: {status.upper()}")
                    elif status == 'failed':
                        st.error(f"Status: {status.upper()}")
                    else:
                        st.info(f"Status: {status.upper()}")
                    
                    # Metrics
                    metric_cols = st.columns(4)
                    with metric_cols[0]:
                        st.metric("Rule", rec.get('rule_category', 'N/A'))
                    with metric_cols[1]:
                        st.metric("Rows", f"{rec.get('row_count', 0):,}")
                    with metric_cols[2]:
                        mb = rec.get('bytes_scanned', 0) / (1024 * 1024)
                        st.metric("Scanned", f"{mb:.2f} MB")
                    with metric_cols[3]:
                        secs = rec.get('execution_time_ms', 0) / 1000
                        st.metric("Time", f"{secs:.2f}s")
                    
                    # Execution ID
                    if rec.get('execution_id'):
                        st.code(f"Execution ID: {rec['execution_id']}", language="text")
                    
                    # Queries
                    with st.expander("NL Query", expanded=False):
                        st.code(rec.get("nl_query") or "", language="text")
                    with st.expander("Generated SQL", expanded=True):
                        st.code(rec.get("generated_sql") or "", language="sql")
                    if rec.get("error_message"):
                        with st.expander("❌ Error Message", expanded=True):
                            st.code(rec["error_message"], language="text")
            else:
                st.info("Enter a valid id.")

        line()
        st.subheader("⚠️ Danger Zone")
        del_id = st.number_input("Delete execution row id", min_value=0, value=0, step=1, key="del_exec")
        if st.button("🗑️ Delete row from executions", key="del_exec_btn"):
            if del_id > 0:
                n = exec_sql(conn, "DELETE FROM query_executions WHERE id = ?", (del_id,))
                st.success(f"Deleted {n} row(s).")
                st.rerun()
            else:
                st.info("Enter a valid id.")

    # ----------------- LLM interactions -----------------
    with tab2:
        # Statistics
        st.subheader("🤖 LLM Interaction Statistics")
        try:
            stats_sql = """
            SELECT 
                COUNT(*) as total,
                SUM(token_count) as total_tokens,
                AVG(token_count) as avg_tokens,
                COUNT(DISTINCT step_name) as unique_steps
            FROM llm_interactions
            """
            stats_df = read_df(conn, stats_sql)
            if not stats_df.empty:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Calls", int(stats_df['total'].iloc[0]))
                with col2:
                    total_tok = int(stats_df['total_tokens'].iloc[0])
                    st.metric("Total Tokens", f"{total_tok:,}")
                with col3:
                    avg_tok = int(stats_df['avg_tokens'].iloc[0])
                    st.metric("Avg Tokens", f"{avg_tok:,}")
                with col4:
                    st.metric("Unique Steps", int(stats_df['unique_steps'].iloc[0]))
        except Exception as e:
            st.warning(f"Could not load statistics: {str(e)}")
        
        line()

        with st.expander("🔍 Filters", expanded=False):
            step_filter = st.text_input("Step name contains", "", key="llm_step_filter")
            limit = st.number_input("Max rows", min_value=10, max_value=10000, value=300, step=10, key="llm_limit")

        q = """
        SELECT
          id,
          timestamp,
          step_name,
          token_count
        FROM llm_interactions
        WHERE (step_name LIKE ? OR ? = '')
        ORDER BY timestamp DESC
        LIMIT ?
        """
        df = read_df(conn, q, (f"%{step_filter}%", step_filter, int(limit)))
        
        st.subheader("📋 LLM Interactions")
        if df.empty:
            st.info("No LLM rows matched.")
        else:
            st.dataframe(df, use_container_width=True, height=420)

            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "📥 Export CSV",
                    data=to_csv_bytes(df),
                    file_name=f"llm_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="llm_export_csv"
                )
            with col2:
                st.download_button(
                    "📥 Export JSON",
                    data=to_json_bytes(df),
                    file_name=f"llm_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    key="llm_export_json"
                )

        line()
        st.subheader("🔍 Inspect an LLM Interaction")
        llm_id = st.number_input("LLM row id", min_value=0, value=0, step=1, key="llm_row_id")
        if st.button("Show LLM details", key="llm_show_row_btn"):
            if llm_id > 0:
                r = read_df(conn, "SELECT * FROM llm_interactions WHERE id = ?", (llm_id,))
                if r.empty:
                    st.warning("No row with that id.")
                else:
                    rec = r.iloc[0].to_dict()
                    
                    # Metadata
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Step", rec.get('step_name', 'N/A'))
                    with col2:
                        st.metric("Tokens", f"{rec.get('token_count', 0):,}")
                    with col3:
                        st.metric("Timestamp", rec.get('timestamp', 'N/A'))
                    
                    # Content
                    with st.expander("📝 Prompt", expanded=True):
                        st.code(rec.get("prompt") or "", language="markdown")
                    with st.expander("💬 Response", expanded=False):
                        st.code(rec.get("response") or "", language="markdown")
                    if rec.get("context"):
                        with st.expander("🔗 Context", expanded=False):
                            st.code(rec["context"], language="text")
            else:
                st.info("Enter a valid id.")

        line()
        st.subheader("⚠️ Danger Zone")
        del_llm_id = st.number_input("Delete LLM row id", min_value=0, value=0, step=1, key="del_llm")
        if st.button("🗑️ Delete row from LLM interactions", key="del_llm_btn"):
            if del_llm_id > 0:
                n = exec_sql(conn, "DELETE FROM llm_interactions WHERE id = ?", (del_llm_id,))
                st.success(f"Deleted {n} row(s).")
                st.rerun()
            else:
                st.info("Enter a valid id.")

    # ----------------- Search tools -----------------
    with tab3:
        st.subheader("🔍 Text Search")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Search Query Executions")
            search_text = st.text_input(
                "Search in SQL/queries/errors:",
                "",
                key="exec_search",
                placeholder="Enter keyword..."
            )
            if st.button("🔎 Search executions", key="exec_search_btn"):
                if search_text.strip():
                    q = """
                    SELECT id, timestamp, rule_category, database_name, status, execution_id
                    FROM query_executions
                    WHERE (generated_sql LIKE ? OR nl_query LIKE ? OR error_message LIKE ?)
                    ORDER BY timestamp DESC
                    LIMIT 200
                    """
                    df = read_df(conn, q, (f"%{search_text}%", f"%{search_text}%", f"%{search_text}%"))
                    if df.empty:
                        st.info("No results found.")
                    else:
                        st.success(f"Found {len(df)} results")
                        st.dataframe(df, use_container_width=True, height=400)
                else:
                    st.info("Enter search text.")

        with col2:
            st.markdown("#### Search LLM Interactions")
            search_llm = st.text_input(
                "Search in prompts/responses:",
                "",
                key="llm_search",
                placeholder="Enter keyword..."
            )
            if st.button("🔎 Search LLM", key="llm_search_btn"):
                if search_llm.strip():
                    q = """
                    SELECT id, timestamp, step_name, token_count
                    FROM llm_interactions
                    WHERE (prompt LIKE ? OR response LIKE ? OR context LIKE ?)
                    ORDER BY timestamp DESC
                    LIMIT 200
                    """
                    df = read_df(conn, q, (f"%{search_llm}%", f"%{search_llm}%", f"%{search_llm}%"))
                    if df.empty:
                        st.info("No results found.")
                    else:
                        st.success(f"Found {len(df)} results")
                        st.dataframe(df, use_container_width=True, height=400)
                else:
                    st.info("Enter search text.")


# ---------------------------------------------------------------------
# App
# ---------------------------------------------------------------------
st.set_page_config(page_title="Logs & Cache Viewer", layout="wide", page_icon="📊")
st.title("📊 Logs & Cache Viewer")
st.caption("Monitor your NL-to-SQL pipeline's cache and execution logs")

tabs = st.tabs(["🗄️ Cache DB", "📝 Logs DB"])
with tabs[0]:
    page_cache()
with tabs[1]:
    page_logs()