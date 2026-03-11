from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient

MONGO_URI = "mongodb://mongo_user:mongo_password@mongodb:27017/app_db?authSource=admin"
MONGO_DB = "app_db"
PG_CONN = "my_db_conn"

default_args = {
    "owner": "p_bragin",
    "start_date": datetime(2026, 3, 1),
    "catchup": False,
}


# ─────────────────────────────────────────────
# DDL helpers
# ─────────────────────────────────────────────

DDL_STATEMENTS = """
-- User sessions (partitioned by month on start_time)
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id      TEXT        NOT NULL,
    user_id         TEXT        NOT NULL,
    start_time      TIMESTAMPTZ NOT NULL,
    end_time        TIMESTAMPTZ,
    duration_min    NUMERIC(10,2),
    pages_visited   TEXT[],
    pages_count     INT,
    device          TEXT,
    actions         TEXT[],
    loaded_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (session_id, start_time)
) PARTITION BY RANGE (start_time);

CREATE TABLE IF NOT EXISTS user_sessions_2024
    PARTITION OF user_sessions
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS user_sessions_2025
    PARTITION OF user_sessions
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS user_sessions_2026
    PARTITION OF user_sessions
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- Event logs (partitioned by month on event_date)
CREATE TABLE IF NOT EXISTS event_logs (
    event_id        TEXT        NOT NULL,
    event_date      DATE        NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_type      TEXT,
    details         TEXT,
    user_id         TEXT,
    session_id      TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

CREATE TABLE IF NOT EXISTS event_logs_2024
    PARTITION OF event_logs
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS event_logs_2025
    PARTITION OF event_logs
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS event_logs_2026
    PARTITION OF event_logs
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- Support tickets
CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id       TEXT PRIMARY KEY,
    user_id         TEXT,
    status          TEXT,
    issue_type      TEXT,
    message_count   INT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    resolution_hours NUMERIC(10,2),
    loaded_at       TIMESTAMPTZ DEFAULT now()
);

-- User recommendations
CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id                 TEXT PRIMARY KEY,
    recommended_products    TEXT[],
    product_count           INT,
    last_updated            TIMESTAMPTZ,
    loaded_at               TIMESTAMPTZ DEFAULT now()
);

-- Moderation queue
CREATE TABLE IF NOT EXISTS moderation_queue (
    review_id           TEXT PRIMARY KEY,
    user_id             TEXT,
    product_id          TEXT,
    review_text         TEXT,
    review_length       INT,
    rating              SMALLINT,
    moderation_status   TEXT,
    flags               TEXT[],
    submitted_at        TIMESTAMPTZ,
    loaded_at           TIMESTAMPTZ DEFAULT now()
);
"""


def init_tables():
    pg = PostgresHook(postgres_conn_id=PG_CONN)
    pg.run(DDL_STATEMENTS)
    print("Tables created / verified.")


# ─────────────────────────────────────────────
# Replication functions
# ─────────────────────────────────────────────

def replicate_user_sessions():
    client = MongoClient(MONGO_URI)
    docs = list(client[MONGO_DB]["user_sessions"].find({}, {"_id": 0}))
    client.close()

    pg = PostgresHook(postgres_conn_id=PG_CONN)

    rows = []
    seen = set()
    for d in docs:
        sid = d.get("session_id", "").strip()
        if not sid or sid in seen:
            continue
        seen.add(sid)

        start = _parse_ts(d.get("start_time"))
        end = _parse_ts(d.get("end_time"))
        if start is None:
            continue

        duration = round((end - start).total_seconds() / 60, 2) if end else None
        pages = d.get("pages_visited") or []
        actions = d.get("actions") or []

        rows.append((
            sid,
            (d.get("user_id") or "").strip(),
            start,
            end,
            duration,
            pages,
            len(pages),
            (d.get("device") or "unknown").strip().lower(),
            actions,
        ))

    if rows:
        pg.run("""
            INSERT INTO user_sessions
              (session_id, user_id, start_time, end_time, duration_min,
               pages_visited, pages_count, device, actions)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (session_id, start_time) DO NOTHING
        """, parameters=rows)

    print(f"user_sessions: {len(rows)} rows upserted.")


def replicate_event_logs():
    client = MongoClient(MONGO_URI)
    docs = list(client[MONGO_DB]["event_logs"].find({}, {"_id": 0}))
    client.close()

    pg = PostgresHook(postgres_conn_id=PG_CONN)

    rows = []
    seen = set()
    for d in docs:
        eid = d.get("event_id", "").strip()
        if not eid or eid in seen:
            continue
        seen.add(eid)

        ts = _parse_ts(d.get("timestamp"))
        if ts is None:
            continue

        rows.append((
            eid,
            ts.date(),
            ts,
            (d.get("event_type") or "unknown").strip().lower(),
            (d.get("details") or "").strip(),
            (d.get("user_id") or "").strip(),
            (d.get("session_id") or "").strip(),
        ))

    if rows:
        pg.run("""
            INSERT INTO event_logs
              (event_id, event_date, event_timestamp, event_type, details, user_id, session_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id, event_date) DO NOTHING
        """, parameters=rows)

    print(f"event_logs: {len(rows)} rows upserted.")


def replicate_support_tickets():
    client = MongoClient(MONGO_URI)
    docs = list(client[MONGO_DB]["support_tickets"].find({}, {"_id": 0}))
    client.close()

    pg = PostgresHook(postgres_conn_id=PG_CONN)

    rows = []
    seen = set()
    for d in docs:
        tid = d.get("ticket_id", "").strip()
        if not tid or tid in seen:
            continue
        seen.add(tid)

        created = _parse_ts(d.get("created_at"))
        updated = _parse_ts(d.get("updated_at"))
        resolution_hours = None
        if created and updated and updated > created:
            resolution_hours = round((updated - created).total_seconds() / 3600, 2)

        msgs = d.get("messages") or []

        rows.append((
            tid,
            (d.get("user_id") or "").strip(),
            (d.get("status") or "unknown").strip().lower(),
            (d.get("issue_type") or "unknown").strip().lower(),
            len(msgs),
            created,
            updated,
            resolution_hours,
        ))

    if rows:
        pg.run("""
            INSERT INTO support_tickets
              (ticket_id, user_id, status, issue_type, message_count,
               created_at, updated_at, resolution_hours)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticket_id) DO NOTHING
        """, parameters=rows)

    print(f"support_tickets: {len(rows)} rows upserted.")


def replicate_user_recommendations():
    client = MongoClient(MONGO_URI)
    docs = list(client[MONGO_DB]["user_recommendations"].find({}, {"_id": 0}))
    client.close()

    pg = PostgresHook(postgres_conn_id=PG_CONN)

    rows = []
    seen = set()
    for d in docs:
        uid = d.get("user_id", "").strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)

        products = d.get("recommended_products") or []
        updated = _parse_ts(d.get("last_updated"))

        rows.append((
            uid,
            products,
            len(products),
            updated,
        ))

    if rows:
        pg.run("""
            INSERT INTO user_recommendations
              (user_id, recommended_products, product_count, last_updated)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (user_id) DO UPDATE
              SET recommended_products = EXCLUDED.recommended_products,
                  product_count        = EXCLUDED.product_count,
                  last_updated         = EXCLUDED.last_updated,
                  loaded_at            = now()
        """, parameters=rows)

    print(f"user_recommendations: {len(rows)} rows upserted.")


def replicate_moderation_queue():
    client = MongoClient(MONGO_URI)
    docs = list(client[MONGO_DB]["moderation_queue"].find({}, {"_id": 0}))
    client.close()

    pg = PostgresHook(postgres_conn_id=PG_CONN)

    rows = []
    seen = set()
    for d in docs:
        rid = d.get("review_id", "").strip()
        if not rid or rid in seen:
            continue
        seen.add(rid)

        text = (d.get("review_text") or "").strip()
        rating = d.get("rating")
        if rating is not None:
            rating = max(1, min(5, int(rating)))

        rows.append((
            rid,
            (d.get("user_id") or "").strip(),
            (d.get("product_id") or "").strip(),
            text,
            len(text),
            rating,
            (d.get("moderation_status") or "pending").strip().lower(),
            d.get("flags") or [],
            _parse_ts(d.get("submitted_at")),
        ))

    if rows:
        pg.run("""
            INSERT INTO moderation_queue
              (review_id, user_id, product_id, review_text, review_length,
               rating, moderation_status, flags, submitted_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (review_id) DO NOTHING
        """, parameters=rows)

    print(f"moderation_queue: {len(rows)} rows upserted.")


# ─────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────

def _parse_ts(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(value, fmt)
        except (ValueError, TypeError):
            pass
    return None


# ─────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────

with DAG(
    dag_id="hw5_mongo_to_postgres_replication",
    default_args=default_args,
    schedule_interval="@daily",
    description="Replicate MongoDB collections to PostgreSQL with transformation",
    tags=["hw5", "replication", "mongodb", "postgres"],
) as dag:

    t_init = PythonOperator(
        task_id="init_tables",
        python_callable=init_tables,
    )

    t_sessions = PythonOperator(
        task_id="replicate_user_sessions",
        python_callable=replicate_user_sessions,
    )

    t_events = PythonOperator(
        task_id="replicate_event_logs",
        python_callable=replicate_event_logs,
    )

    t_tickets = PythonOperator(
        task_id="replicate_support_tickets",
        python_callable=replicate_support_tickets,
    )

    t_recommendations = PythonOperator(
        task_id="replicate_user_recommendations",
        python_callable=replicate_user_recommendations,
    )

    t_moderation = PythonOperator(
        task_id="replicate_moderation_queue",
        python_callable=replicate_moderation_queue,
    )

    t_init >> [t_sessions, t_events, t_tickets, t_recommendations, t_moderation]
