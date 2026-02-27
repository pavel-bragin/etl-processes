import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_PATH = '/opt/airflow/dags/hw-3'

default_args = {
    'owner': 'p_bragin',
    'start_date': datetime(2026, 2, 1),
    'catchup': False,
}


def transform_and_load(**kwargs):
    import pandas as pd

    df = pd.read_csv(f'{DATA_PATH}/IOT-temp.csv')
    df.columns = ['id', 'room_id', 'noted_date', 'temp', 'location']

    df = df[df['location'] == 'In'].copy()

    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M').dt.date

    p5 = df['temp'].quantile(0.05)
    p95 = df['temp'].quantile(0.95)
    df = df[(df['temp'] >= p5) & (df['temp'] <= p95)]

    print(f"Percentile bounds: p5={p5}, p95={p95}")
    print(f"Rows after cleaning: {len(df)}")

    pg_hook = PostgresHook(postgres_conn_id='my_db_conn')
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql('temp_cleaned', engine, if_exists='append', index=False)

    print(f"Loaded {len(df)} rows into temp_cleaned")


def find_extreme_days(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_db_conn')

    pg_hook.run("""
        INSERT INTO hottest_days (noted_date, avg_temp)
        SELECT noted_date, ROUND(AVG(temp)::numeric, 2) AS avg_temp
        FROM temp_cleaned
        GROUP BY noted_date
        ORDER BY avg_temp DESC
        LIMIT 5;
    """)

    pg_hook.run("""
        INSERT INTO coldest_days (noted_date, avg_temp)
        SELECT noted_date, ROUND(AVG(temp)::numeric, 2) AS avg_temp
        FROM temp_cleaned
        GROUP BY noted_date
        ORDER BY avg_temp ASC
        LIMIT 5;
    """)

    pg_hook = PostgresHook(postgres_conn_id='my_db_conn')
    hottest = pg_hook.get_records("SELECT noted_date, avg_temp FROM hottest_days ORDER BY avg_temp DESC")
    coldest = pg_hook.get_records("SELECT noted_date, avg_temp FROM coldest_days ORDER BY avg_temp ASC")

    print("Top 5 hottest days:")
    for row in hottest:
        print(f"  {row[0]}  {row[1]}°C")

    print("Top 5 coldest days:")
    for row in coldest:
        print(f"  {row[0]}  {row[1]}°C")


with DAG(
    dag_id='etl_iot_temperature',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='my_db_conn',
        sql="""
            DROP TABLE IF EXISTS temp_cleaned, hottest_days, coldest_days;

            CREATE TABLE temp_cleaned (
                id       TEXT,
                room_id  TEXT,
                noted_date DATE,
                temp     INT,
                location TEXT
            );

            CREATE TABLE hottest_days (
                noted_date DATE,
                avg_temp   NUMERIC
            );

            CREATE TABLE coldest_days (
                noted_date DATE,
                avg_temp   NUMERIC
            );
        """,
    )

    transform_data = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load,
    )

    extreme_days = PythonOperator(
        task_id='find_extreme_days',
        python_callable=find_extreme_days,
    )

    create_tables >> transform_data >> extreme_days
