from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────
# DARK THEME
# ──────────────────────────────────────────────

DARK_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Inter:wght@300;400;500;600;700&display=swap');

:root {
    --bg:         #0d1117;
    --bg-card:    #161b22;
    --border:     #21262d;
    --green:      #23c45e;
    --green-glow: rgba(35,196,94,0.12);
    --red:        #f85149;
    --yellow:     #e3b341;
    --blue:       #388bfd;
    --text:       #e6edf3;
    --muted:      #8b949e;
    --dim:        #6e7681;
}

/* Force dark on EVERYTHING */
html, body { background-color: #0d1117 !important; color: #e6edf3 !important; }

*, *::before, *::after {
    font-family: 'Inter', sans-serif !important;
}

/* Streamlit root wrappers */
.stApp,
.stApp > div,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewBlockContainer"],
[data-testid="stVerticalBlock"],
[data-testid="stVerticalBlockBorderWrapper"],
.main,
.main > div {
    background-color: #0d1117 !important;
    color: #e6edf3 !important;
}

.main .block-container {
    background-color: #0d1117 !important;
    padding: 1.5rem 2rem !important;
    max-width: 100% !important;
    color: #e6edf3 !important;
}

/* Every generic div/span/p inside main */
.main p, .main span, .main div, .main label { color: #e6edf3 !important; }

#MainMenu, footer, header, .stDeployButton { visibility: hidden !important; display: none !important; }

/* Sidebar */
section[data-testid="stSidebar"],
section[data-testid="stSidebar"] > div {
    background-color: #0d1117 !important;
    border-right: 1px solid #21262d !important;
}
section[data-testid="stSidebar"] *,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] div { color: #e6edf3 !important; }

.sidebar-title {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.05rem; font-weight: 600;
    padding: 0 1rem 0.6rem;
}
.sidebar-title span { color: var(--green); }

.badge-ok {
    background: var(--green); color: #000 !important;
    font-weight: 600; font-size: 0.8rem;
    border-radius: 6px; padding: 0.4rem 1rem;
    margin: 0.5rem 0; text-align: center;
}
.badge-err {
    background: var(--red); color: #fff !important;
    font-weight: 600; font-size: 0.8rem;
    border-radius: 6px; padding: 0.4rem 1rem;
    margin: 0.5rem 0; text-align: center;
}
.conn-chip {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem; color: var(--green) !important;
    background: var(--green-glow);
    border: 1px solid rgba(35,196,94,0.2);
    border-radius: 4px; padding: 0.25rem 0.5rem;
    margin-top: 0.25rem; word-break: break-all;
}

/* Title */
h1 {
    font-size: 2rem !important; font-weight: 700 !important;
    color: var(--text) !important; letter-spacing: -0.04em !important;
    margin-bottom: 0 !important;
}

/* Metrics */
[data-testid="metric-container"] {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    padding: 1rem 1.2rem !important;
}
[data-testid="stMetricLabel"] {
    font-size: 0.74rem !important; color: var(--muted) !important;
    text-transform: uppercase; letter-spacing: 0.06em; font-weight: 500;
}
[data-testid="stMetricValue"] {
    font-size: 1.6rem !important; font-weight: 700 !important;
    color: var(--text) !important;
    font-family: 'JetBrains Mono', monospace !important;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid var(--border) !important;
    gap: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: var(--muted) !important;
    border: none !important;
    border-bottom: 2px solid transparent !important;
    padding: 0.55rem 1rem !important;
    font-size: 0.84rem !important; font-weight: 500 !important;
}
.stTabs [aria-selected="true"] {
    color: var(--green) !important;
    border-bottom-color: var(--green) !important;
    background: transparent !important;
}
.stTabs [data-baseweb="tab-panel"] { padding-top: 1.2rem !important; }

/* DataFrames */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: 8px !important; overflow: hidden;
}

/* Buttons */
.stButton > button {
    background: transparent !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    border-radius: 6px !important;
    font-size: 0.82rem !important;
    transition: border-color 0.15s, background 0.15s;
}
.stButton > button:hover {
    border-color: var(--green) !important;
    background: var(--green-glow) !important;
}

/* Inputs */
.stTextInput input, .stSelectbox select {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    border-radius: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.8rem !important;
}

/* Misc */
hr { border-color: var(--border) !important; margin: 1rem 0 !important; }
h2, h3 { color: var(--text) !important; font-weight: 600 !important; }
h2 { font-size: 1.1rem !important; }
h3 { font-size: 0.95rem !important; }
.stCaption, [data-testid="stCaptionContainer"] {
    color: var(--dim) !important; font-size: 0.74rem !important;
}
details {
    background: var(--bg-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
}
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
</style>
"""

PLOTLY_BASE = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#161b22",
    font_color="#e6edf3",
    font_family="Inter",
    margin=dict(l=0, r=0, t=36, b=0),
    title_font_size=13,
)

GREEN  = "#23c45e"
RED    = "#f85149"
BLUE   = "#388bfd"
YELLOW = "#e3b341"

st.set_page_config(page_title="Postgres Observe", page_icon="🐘",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(DARK_CSS, unsafe_allow_html=True)


# ──────────────────────────────────────────────
# CONFIG / CONNECTION
# ──────────────────────────────────────────────

@dataclass(frozen=True)
class PgConfig:
    host: str; port: int; dbname: str
    user: str; password: str; sslmode: str = "prefer"

    def dsn(self):
        return (f"host={self.host} port={self.port} dbname={self.dbname} "
                f"user={self.user} password={self.password} sslmode={self.sslmode}")


def config_from_env() -> PgConfig:
    return PgConfig(
        host=os.getenv("LOCAL_PG_HOST", "postgres"),
        port=int(os.getenv("LOCAL_PG_PORT", "5432")),
        dbname=os.getenv("LOCAL_PG_DB", "observe"),
        user=os.getenv("LOCAL_PG_USER", "observe"),
        password=os.getenv("LOCAL_PG_PASSWORD", "observe"),
        sslmode=os.getenv("LOCAL_PG_SSLMODE", "disable"),
    )


@contextmanager
def get_conn(cfg: PgConfig):
    conn = psycopg2.connect(cfg.dsn(), connect_timeout=5)
    try:
        conn.set_session(readonly=True, autocommit=True)
        yield conn
    finally:
        conn.close()


@st.cache_data(ttl=30, show_spinner=False)
def query(sql: str, key: str) -> pd.DataFrame:
    cfg: PgConfig = st.session_state["pg_config"]
    with get_conn(cfg) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return pd.DataFrame(cur.fetchall())


def test_conn(cfg: PgConfig) -> tuple[bool, str]:
    try:
        with get_conn(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                return True, cur.fetchone()[0]
    except Exception as e:
        return False, str(e)


# ──────────────────────────────────────────────
# SQL
# ──────────────────────────────────────────────

SQL = dict(
    overview="""
        SELECT current_database() AS database,
               pg_size_pretty(pg_database_size(current_database())) AS db_size,
               (SELECT count(*) FROM pg_stat_activity) AS total_connections,
               (SELECT setting::int FROM pg_settings WHERE name='max_connections') AS max_connections,
               (SELECT count(*) FROM pg_stat_activity WHERE state='active') AS active_queries,
               (SELECT count(*) FROM pg_stat_activity WHERE wait_event_type IS NOT NULL) AS waiting,
               round(100.0*sum(blks_hit)/nullif(sum(blks_hit+blks_read),0),2) AS cache_hit_pct
        FROM pg_stat_database WHERE datname=current_database()
    """,
    connections="""
        SELECT coalesce(state,'unknown') AS state, count(*) AS n
        FROM pg_stat_activity GROUP BY state ORDER BY n DESC
    """,
    long_running="""
        SELECT pid, now()-query_start AS duration, left(query,200) AS query,
               state, wait_event_type, wait_event
        FROM pg_stat_activity
        WHERE state!='idle' AND query_start IS NOT NULL
          AND now()-query_start > interval '1 second'
        ORDER BY duration DESC LIMIT 15
    """,
    slow_queries="""
        SELECT left(query,300) AS query, calls,
               round(total_exec_time::numeric,2) AS total_ms,
               round(mean_exec_time::numeric,2)  AS mean_ms,
               round(stddev_exec_time::numeric,2) AS stddev_ms,
               round(min_exec_time::numeric,2)   AS min_ms,
               round(max_exec_time::numeric,2)   AS max_ms,
               rows,
               round(100.0*shared_blks_hit/nullif(shared_blks_hit+shared_blks_read,0),1) AS cache_hit_pct
        FROM pg_stat_statements
        WHERE query NOT LIKE '%pg_stat%'
        ORDER BY mean_exec_time DESC LIMIT 25
    """,
    unused_indexes="""
        SELECT schemaname AS schema, relname AS "table", indexrelname AS "index",
               pg_size_pretty(pg_relation_size(indexrelid)) AS index_size, idx_scan AS scans
        FROM pg_stat_user_indexes
        WHERE idx_scan=0 AND schemaname NOT IN ('pg_catalog','information_schema')
        ORDER BY pg_relation_size(indexrelid) DESC
    """,
    low_indexes="""
        SELECT schemaname AS schema, relname AS "table", indexrelname AS "index",
               pg_size_pretty(pg_relation_size(indexrelid)) AS index_size, idx_scan AS scans
        FROM pg_stat_user_indexes
        WHERE idx_scan<50 AND schemaname NOT IN ('pg_catalog','information_schema')
        ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC LIMIT 30
    """,
    seq_scans="""
        SELECT schemaname AS schema, relname AS "table", seq_scan, idx_scan,
               CASE WHEN (seq_scan+idx_scan)>0
                    THEN round(100.0*idx_scan/(seq_scan+idx_scan),1) ELSE 0 END AS index_use_pct,
               pg_size_pretty(pg_relation_size(relid)) AS table_size
        FROM pg_stat_user_tables WHERE seq_scan>0 ORDER BY seq_scan DESC LIMIT 20
    """,
    bloat="""
        SELECT relname AS "table", n_live_tup AS live_rows, n_dead_tup AS dead_rows,
               CASE WHEN n_live_tup>0
                    THEN round(100.0*n_dead_tup/n_live_tup,1) ELSE 0 END AS dead_pct,
               pg_size_pretty(pg_relation_size(relid)) AS table_size,
               to_char(last_autovacuum,'YYYY-MM-DD HH24:MI') AS last_autovacuum,
               to_char(last_autoanalyze,'YYYY-MM-DD HH24:MI') AS last_autoanalyze
        FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 20
    """,
    locks="""
        SELECT pid, relation::regclass AS relation, locktype, mode, granted, left(query,120) AS query
        FROM pg_locks l LEFT JOIN pg_stat_activity a USING(pid)
        WHERE relation IS NOT NULL ORDER BY granted, pid LIMIT 30
    """,
    databases="""
        SELECT datname AS database, pg_size_pretty(pg_database_size(datname)) AS size,
               numbackends AS connections, xact_commit AS commits, xact_rollback AS rollbacks,
               round(100.0*blks_hit/nullif(blks_hit+blks_read,0),1) AS cache_hit_pct,
               conflicts, deadlocks
        FROM pg_stat_database
        WHERE datname NOT IN ('template0','template1','postgres')
        ORDER BY pg_database_size(datname) DESC
    """,
)


# ──────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────

def render_sidebar():
    with st.sidebar:
        st.markdown('<div class="sidebar-title">🐘 Postgres <span>Observe</span></div>',
                    unsafe_allow_html=True)
        st.markdown('<p style="font-size:0.74rem;color:#8b949e;margin:0 0 0.5rem;">Target database ⓘ</p>',
                    unsafe_allow_html=True)

        mode = st.radio("", ["Local (docker-compose)", "External"], label_visibility="collapsed")
        cfg = config_from_env() if mode == "Local (docker-compose)" else PgConfig(
            host=st.text_input("Host", os.getenv("EXT_PG_HOST", "localhost")),
            port=int(st.text_input("Port", os.getenv("EXT_PG_PORT", "5432"))),
            dbname=st.text_input("Database", os.getenv("EXT_PG_DB", "postgres")),
            user=st.text_input("User", os.getenv("EXT_PG_USER", "postgres")),
            password=st.text_input("Password", os.getenv("EXT_PG_PASSWORD", ""), type="password"),
            sslmode=st.selectbox("SSL", ["prefer", "require", "disable"]),
        )

        ok, ver = test_conn(cfg)
        if ok:
            st.markdown('<div class="badge-ok">● Connected</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="conn-chip">Connecting to {cfg.host}:{cfg.port}/{cfg.dbname} as {cfg.user}</div>',
                unsafe_allow_html=True)
            short_ver = ver.split(",")[0]
            st.markdown(f'<p style="font-size:0.7rem;color:#8b949e;margin:0.4rem 0">{short_ver}</p>',
                        unsafe_allow_html=True)
        else:
            st.markdown('<div class="badge-err">✕ Disconnected</div>', unsafe_allow_html=True)
            st.caption(ver[:140])

        st.divider()
        if st.button("↺  Refresh now", use_container_width=True):
            st.cache_data.clear(); st.rerun()
        if st.checkbox("Auto-refresh every 15s"):
            import time; time.sleep(15)
            st.cache_data.clear(); st.rerun()

    return cfg, ok


# ──────────────────────────────────────────────
# OVERVIEW
# ──────────────────────────────────────────────

def render_overview():
    df = query(SQL["overview"], "overview")
    r = df.iloc[0]
    cache = float(r["cache_hit_pct"] or 0)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Database",      r["database"])
    c2.metric("Size",          r["db_size"])
    c3.metric("Connections",   f"{r['total_connections']} / {r['max_connections']}")
    c4.metric("Active Queries", r["active_queries"])
    return cache


# ──────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────

def tab_activity(cache_pct):
    col_l, col_r = st.columns([1.5, 1])

    with col_l:
        st.markdown("#### Connections by state")
        df = query(SQL["connections"], "conns")
        color_map = {"idle": GREEN, "active": BLUE, "idle in transaction": YELLOW}
        colors = [color_map.get(s, "#8b949e") for s in df["state"]]
        fig = go.Figure(go.Bar(x=df["state"], y=df["n"],
                               marker_color=colors, marker_line_width=0))
        fig.update_layout(**PLOTLY_BASE, height=260)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("#### Buffer cache hit ratio")
        color = GREEN if cache_pct >= 90 else YELLOW if cache_pct >= 75 else RED
        pct_str = f"{cache_pct:.1f}%"
        gauge_color = color
        st.markdown(f"""
        <div style="text-align:center; padding: 1rem 0 0.5rem;">
            <div style="font-size:3rem; font-weight:700; font-family:'JetBrains Mono',monospace;
                        color:{gauge_color}; line-height:1;">{pct_str}</div>
            <div style="font-size:0.75rem; color:#8b949e; margin-top:0.3rem;">buffer cache hit ratio</div>
        </div>
        """, unsafe_allow_html=True)
        fig = go.Figure(go.Indicator(
            mode="gauge",
            value=cache_pct,
            domain={"x": [0, 1], "y": [0, 1]},
            gauge={
                "axis": {
                    "range": [0, 100],
                    "tickvals": [0, 25, 50, 75, 100],
                    "ticktext": ["0", "25", "50", "75", "100"],
                    "tickcolor": "#8b949e",
                    "tickfont": {"color": "#8b949e", "size": 11},
                    "tickwidth": 1,
                },
                "bar": {"color": color, "thickness": 0.3},
                "bgcolor": "#0d1117",
                "borderwidth": 0,
                "steps": [
                    {"range": [0, 75],  "color": "#1a1f27"},
                    {"range": [75, 90], "color": "#1e2820"},
                    {"range": [90, 100],"color": "#132318"},
                ],
            },
        ))
        fig.update_layout(**{**PLOTLY_BASE, "margin": dict(l=30, r=30, t=0, b=0)}, height=180)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Current sessions")
    df = query(SQL["long_running"], "sessions")
    if df.empty:
        st.markdown('<p style="color:#8b949e;font-size:0.84rem;">No active sessions.</p>',
                    unsafe_allow_html=True)
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def tab_tables_indexes():
    # ── Unused indexes ──
    st.markdown("#### 🗑️ Never-Used Indexes")
    st.caption("idx_scan = 0 — waste space and slow writes")
    df = query(SQL["unused_indexes"], "unused")
    if df.empty:
        st.success("✅ No never-used indexes found.")
    else:
        st.warning(f"**{len(df)}** index(es) never scanned.")
        st.dataframe(df, use_container_width=True, hide_index=True)
        with st.expander("📋 DROP statements — review carefully before running"):
            st.code("\n".join(
                f"DROP INDEX CONCURRENTLY {r['schema']}.{r['index']};"
                for _, r in df.iterrows()
            ), language="sql")

    st.divider()

    # ── Low usage indexes ──
    st.markdown("#### 📉 Low-Usage Indexes  (idx_scan < 50)")
    df = query(SQL["low_indexes"], "low")
    if not df.empty:
        fig = px.bar(df.head(15), x="scans", y="index", orientation="h",
                     color="scans", color_continuous_scale="Blues", title="Scans per index")
        fig.update_layout(**PLOTLY_BASE, coloraxis_showscale=False,
                          yaxis={"categoryorder":"total ascending"})
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()

    # ── Missing indexes ──
    st.markdown("#### 🔍 Tables with High Sequential Scan Rate")
    st.caption("High seq_scan on large tables often signals a missing index")
    df = query(SQL["seq_scans"], "seq")
    if not df.empty:
        fig = px.scatter(df, x="index_use_pct", y="seq_scan", size="seq_scan",
                         hover_name="table", color="index_use_pct",
                         color_continuous_scale=["#f85149","#e3b341","#23c45e"],
                         range_color=[0,100], title="Index usage % vs seq scans")
        fig.update_layout(**PLOTLY_BASE)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()

    # ── Bloat ──
    st.markdown("#### 🧹 Table Bloat & Vacuum")
    df = query(SQL["bloat"], "bloat")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True,
                     column_config={"dead_pct": st.column_config.ProgressColumn(
                         "Dead %", min_value=0, max_value=100, format="%.1f%%")})


def tab_queries():
    st.markdown("#### 🐢 Slowest Queries")
    st.caption("Source: `pg_stat_statements` — sorted by mean execution time")
    df = query(SQL["slow_queries"], "slow")
    if df.empty:
        st.info("No data — ensure `pg_stat_statements` extension is enabled.")
        return

    top = df.head(10).copy()
    top["label"] = top["query"].str[:55] + "…"
    fig = px.bar(top, x="mean_ms", y="label", orientation="h",
                 color="mean_ms", color_continuous_scale="Reds",
                 title="Top 10 — mean execution time (ms)")
    fig.update_layout(**PLOTLY_BASE, coloraxis_showscale=False,
                      yaxis={"categoryorder":"total ascending"})
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(df, use_container_width=True, hide_index=True,
                 column_config={
                     "query":    st.column_config.TextColumn("Query", width="large"),
                     "mean_ms":  st.column_config.NumberColumn("Mean (ms)",   format="%.2f"),
                     "total_ms": st.column_config.NumberColumn("Total (ms)",  format="%.2f"),
                     "min_ms":   st.column_config.NumberColumn("Min (ms)",    format="%.2f"),
                     "max_ms":   st.column_config.NumberColumn("Max (ms)",    format="%.2f"),
                     "cache_hit_pct": st.column_config.ProgressColumn(
                         "Cache hit %", min_value=0, max_value=100, format="%.1f%%"),
                 })


def tab_locks():
    st.markdown("#### 🔒 Active Locks")
    df = query(SQL["locks"], "locks")
    if df.empty:
        st.success("✅ No locks detected.")
        return
    blocked = df[df["granted"] == False]  # noqa
    if not blocked.empty:
        st.error(f"⚠️ {len(blocked)} query(ies) blocked waiting for a lock.")
    st.dataframe(df, use_container_width=True, hide_index=True)


def tab_databases():
    st.markdown("#### 🗄️ All Databases")
    df = query(SQL["databases"], "dbs")
    if df.empty:
        st.info("No data available.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True,
                 column_config={"cache_hit_pct": st.column_config.ProgressColumn(
                     "Cache hit %", min_value=0, max_value=100, format="%.1f%%")})


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    cfg, connected = render_sidebar()
    st.session_state["pg_config"] = cfg

    st.title("Postgres Observability Dashboard")
    st.markdown("<div style='height:.6rem'></div>", unsafe_allow_html=True)

    if not connected:
        st.error("Cannot reach Postgres. Check connection settings in the sidebar.")
        st.stop()

    cache_pct = render_overview()
    st.markdown("<div style='height:.8rem'></div>", unsafe_allow_html=True)

    t1, t2, t3, t4, t5 = st.tabs(
        ["Activity", "Tables & Indexes", "Queries", "Locks", "Databases"])

    with t1: tab_activity(cache_pct)
    with t2: tab_tables_indexes()
    with t3: tab_queries()
    with t4: tab_locks()
    with t5: tab_databases()


if __name__ == "__main__":
    main()
