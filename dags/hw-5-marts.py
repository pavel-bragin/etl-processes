"""
DAG: hw5_build_analytical_marts
Builds 2 analytical data marts in PostgreSQL from the replicated tables.

Mart 1 – dm_user_activity
  Behavioural analysis: sessions, time-on-site, popular pages/actions per user.

Mart 2 – dm_support_efficiency
  Support statistics: ticket counts by status/type, average resolution time,
  open ticket backlog.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "p_bragin",
    "start_date": datetime(2026, 3, 1),
    "catchup": False,
}

# ─────────────────────────────────────────────
# DDL for the two data marts
# ─────────────────────────────────────────────

DDL_MARTS = """
CREATE TABLE IF NOT EXISTS dm_user_activity (
    user_id                 TEXT PRIMARY KEY,
    total_sessions          INT,
    total_duration_min      NUMERIC(12,2),
    avg_duration_min        NUMERIC(10,2),
    total_pages_visited     INT,
    avg_pages_per_session   NUMERIC(10,2),
    unique_pages_visited    INT,
    top_device              TEXT,
    total_actions           INT,
    first_session_at        TIMESTAMPTZ,
    last_session_at         TIMESTAMPTZ,
    active_days             INT,
    updated_at              TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dm_support_efficiency (
    issue_type              TEXT,
    status                  TEXT,
    ticket_count            INT,
    avg_resolution_hours    NUMERIC(10,2),
    min_resolution_hours    NUMERIC(10,2),
    max_resolution_hours    NUMERIC(10,2),
    open_ticket_count       INT,
    resolved_ticket_count   INT,
    updated_at              TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (issue_type, status)
);
"""

# ─────────────────────────────────────────────
# Mart 1: dm_user_activity
# ─────────────────────────────────────────────

REFRESH_USER_ACTIVITY = """
TRUNCATE TABLE dm_user_activity;

INSERT INTO dm_user_activity (
    user_id,
    total_sessions,
    total_duration_min,
    avg_duration_min,
    total_pages_visited,
    avg_pages_per_session,
    unique_pages_visited,
    top_device,
    total_actions,
    first_session_at,
    last_session_at,
    active_days,
    updated_at
)
WITH session_stats AS (
    SELECT
        user_id,
        COUNT(*)                                    AS total_sessions,
        SUM(COALESCE(duration_min, 0))              AS total_duration_min,
        AVG(COALESCE(duration_min, 0))              AS avg_duration_min,
        SUM(pages_count)                            AS total_pages_visited,
        AVG(pages_count)                            AS avg_pages_per_session,
        COUNT(DISTINCT unnested_page)               AS unique_pages_visited,
        SUM(array_length(actions, 1))               AS total_actions,
        MIN(start_time)                             AS first_session_at,
        MAX(start_time)                             AS last_session_at,
        COUNT(DISTINCT start_time::date)            AS active_days
    FROM user_sessions
    CROSS JOIN LATERAL unnest(pages_visited) AS unnested_page
    GROUP BY user_id
),
device_rank AS (
    SELECT
        user_id,
        device,
        ROW_NUMBER() OVER (
            PARTITION BY user_id ORDER BY COUNT(*) DESC
        ) AS rn
    FROM user_sessions
    GROUP BY user_id, device
)
SELECT
    s.user_id,
    s.total_sessions,
    ROUND(s.total_duration_min, 2),
    ROUND(s.avg_duration_min, 2),
    s.total_pages_visited,
    ROUND(s.avg_pages_per_session, 2),
    s.unique_pages_visited,
    d.device                                        AS top_device,
    COALESCE(s.total_actions, 0),
    s.first_session_at,
    s.last_session_at,
    s.active_days,
    now()
FROM session_stats s
LEFT JOIN device_rank d ON d.user_id = s.user_id AND d.rn = 1;
"""

# ─────────────────────────────────────────────
# Mart 2: dm_support_efficiency
# ─────────────────────────────────────────────

REFRESH_SUPPORT_EFFICIENCY = """
TRUNCATE TABLE dm_support_efficiency;

INSERT INTO dm_support_efficiency (
    issue_type,
    status,
    ticket_count,
    avg_resolution_hours,
    min_resolution_hours,
    max_resolution_hours,
    open_ticket_count,
    resolved_ticket_count,
    updated_at
)
SELECT
    issue_type,
    status,
    COUNT(*)                                           AS ticket_count,
    ROUND(AVG(resolution_hours), 2)                    AS avg_resolution_hours,
    ROUND(MIN(resolution_hours), 2)                    AS min_resolution_hours,
    ROUND(MAX(resolution_hours), 2)                    AS max_resolution_hours,
    COUNT(*) FILTER (WHERE status = 'open')            AS open_ticket_count,
    COUNT(*) FILTER (WHERE status IN ('resolved','closed')) AS resolved_ticket_count,
    now()
FROM support_tickets
GROUP BY issue_type, status;
"""

# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────

with DAG(
    dag_id="hw5_build_analytical_marts",
    default_args=default_args,
    schedule_interval="@daily",
    tags=["hw5", "marts", "analytics"],
) as dag:

    t_ddl = PostgresOperator(
        task_id="create_mart_tables",
        postgres_conn_id="my_db_conn",
        sql=DDL_MARTS,
    )

    t_user_activity = PostgresOperator(
        task_id="refresh_dm_user_activity",
        postgres_conn_id="my_db_conn",
        sql=REFRESH_USER_ACTIVITY,
    )

    t_support = PostgresOperator(
        task_id="refresh_dm_support_efficiency",
        postgres_conn_id="my_db_conn",
        sql=REFRESH_SUPPORT_EFFICIENCY,
    )

    t_ddl >> [t_user_activity, t_support]
