from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

INCREMENTAL_LOOKBACK_DAYS = 7

default_args = {
    'owner': 'p_bragin',
    'start_date': datetime(2026, 2, 1),
    'catchup': False,
}


def full_load(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_db_conn')

    pg_hook.run("TRUNCATE TABLE temp_final")
    pg_hook.run("""
        INSERT INTO temp_final (id, room_id, noted_date, temp, location)
        SELECT id, room_id, noted_date, temp, location
        FROM temp_cleaned
    """)

    max_date = pg_hook.get_first("SELECT MAX(noted_date) FROM temp_final")[0]
    count = pg_hook.get_first("SELECT COUNT(*) FROM temp_final")[0]

    pg_hook.run("""
        INSERT INTO load_watermark (dag_id, last_loaded_date)
        VALUES ('etl_iot_full_load', %s)
        ON CONFLICT (dag_id) DO UPDATE SET last_loaded_date = EXCLUDED.last_loaded_date
    """, parameters=[max_date])

    print(f"Full load complete: {count} rows loaded, watermark set to {max_date}")


def incremental_load(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_db_conn')

    row = pg_hook.get_first(
        "SELECT last_loaded_date FROM load_watermark WHERE dag_id = 'etl_iot_full_load'"
    )
    if row is None:
        raise RuntimeError("Watermark not found. Run etl_iot_full_load first.")

    watermark = row[0]
    window_start = watermark - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)

    print(f"Incremental window: {window_start} → now (watermark was {watermark})")

    deleted = pg_hook.get_first(
        "SELECT COUNT(*) FROM temp_final WHERE noted_date > %s",
        parameters=[window_start]
    )[0]

    pg_hook.run(
        "DELETE FROM temp_final WHERE noted_date > %s",
        parameters=[window_start]
    )

    pg_hook.run("""
        INSERT INTO temp_final (id, room_id, noted_date, temp, location)
        SELECT id, room_id, noted_date, temp, location
        FROM temp_cleaned
        WHERE noted_date > %s
    """, parameters=[window_start])

    new_max = pg_hook.get_first(
        "SELECT MAX(noted_date) FROM temp_final WHERE noted_date > %s",
        parameters=[window_start]
    )[0]

    inserted = pg_hook.get_first(
        "SELECT COUNT(*) FROM temp_final WHERE noted_date > %s",
        parameters=[window_start]
    )[0]

    if new_max and new_max > watermark:
        pg_hook.run("""
            UPDATE load_watermark SET last_loaded_date = %s
            WHERE dag_id = 'etl_iot_full_load'
        """, parameters=[new_max])
        print(f"Watermark updated: {watermark} → {new_max}")
    else:
        print(f"No new data beyond watermark {watermark}")

    print(f"Incremental load complete: removed {deleted} rows, inserted {inserted} rows")



with DAG(
    dag_id='etl_iot_full_load',
    default_args=default_args,
    schedule_interval=None,
) as full_dag:

    init_tables = PostgresOperator(
        task_id='init_tables',
        postgres_conn_id='my_db_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS temp_final (
                id         TEXT,
                room_id    TEXT,
                noted_date DATE,
                temp       INT,
                location   TEXT
            );

            CREATE TABLE IF NOT EXISTS load_watermark (
                dag_id           TEXT PRIMARY KEY,
                last_loaded_date DATE NOT NULL
            );
        """,
    )

    run_full_load = PythonOperator(
        task_id='full_load',
        python_callable=full_load,
    )

    init_tables >> run_full_load



with DAG(
    dag_id='etl_iot_incremental_load',
    default_args=default_args,
    schedule_interval='@daily',
) as incremental_dag:

    run_incremental_load = PythonOperator(
        task_id='incremental_load',
        python_callable=incremental_load,
    )
