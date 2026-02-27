import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_PATH = '/opt/airflow/dags/hw-2'

default_args = {
    'owner': 'p_bragin',
    'start_date': datetime(2026, 2, 1),
    'catchup': False,
}

def load_file_to_raw(filename, table_name, **kwargs):
    filepath = os.path.join(DATA_PATH, filename)
    
    with open(filepath, 'r', encoding='utf-8') as f:
        file_content = f.read()
    
    pg_hook = PostgresHook(postgres_conn_id='my_db_conn')
    pg_hook.run(
        f"INSERT INTO {table_name} (file_content) VALUES (%s)",
        parameters=[file_content]
    )
    print(f"File {filename} loaded into {table_name}")

with DAG(
    dag_id ='elt_pets_nutrition',
    default_args=default_args,
    schedule_interval=None,
    template_searchpath=[DATA_PATH]
) as dag:

    create_tables = PostgresOperator(
        task_id='init_tables',
        postgres_conn_id='my_db_conn',
        sql="""
            DROP TABLE IF EXISTS raw_pets, raw_nutrition, pets, food_items;
            
            CREATE TABLE raw_pets (file_content JSONB);
            CREATE TABLE raw_nutrition (file_content XML);
            
            CREATE TABLE pets (
                name TEXT, species TEXT, fav_foods TEXT[], 
                birth_year INT, photo_url TEXT
            );
            
            CREATE TABLE food_items (
                name TEXT, manufacturer TEXT, serving_amount NUMERIC, serving_unit TEXT,
                calories_total INT, calories_from_fat INT,
                total_fat NUMERIC, saturated_fat NUMERIC, cholesterol NUMERIC, sodium NUMERIC,
                carb NUMERIC, fiber NUMERIC, protein NUMERIC,
                vitamin_a INT, vitamin_c INT, mineral_ca INT, mineral_fe INT
            );
        """
    )

    load_pets = PythonOperator(
        task_id='load_pets_raw',
        python_callable=load_file_to_raw,
        op_kwargs={'filename': 'pets-data.json', 'table_name': 'raw_pets'}
    )

    load_nutrition = PythonOperator(
        task_id='load_nutrition_raw',
        python_callable=load_file_to_raw,
        op_kwargs={'filename': 'nutrition.xml', 'table_name': 'raw_nutrition'}
    )

    parse_pets = PostgresOperator(
        task_id='parse_pets_sql',
        postgres_conn_id='my_db_conn',
        sql="""
            INSERT INTO pets (name, species, fav_foods, birth_year, photo_url)
            SELECT
                item->>'name',
                item->>'species',
                (SELECT array_agg(x) FROM jsonb_array_elements_text(item->'favFoods') t(x)),
                (item->>'birthYear')::int,
                item->>'photo'
            FROM raw_pets,
            -- Разворачиваем массив "pets" из корня JSON
            LATERAL jsonb_array_elements(file_content->'pets') AS item;
        """
    )
    
    parse_nutrition = PostgresOperator(
        task_id='parse_nutrition_sql',
        postgres_conn_id='my_db_conn',
        sql="""
            INSERT INTO food_items (
                name, manufacturer, serving_amount, serving_unit,
                calories_total, calories_from_fat,
                total_fat, saturated_fat, cholesterol, sodium,
                carb, fiber, protein,
                vitamin_a, vitamin_c, mineral_ca, mineral_fe
            )
            SELECT 
                (xpath('food/name/text()', food))[1]::text,
                (xpath('food/mfr/text()', food))[1]::text,
                (xpath('food/serving/text()', food))[1]::text::numeric,
                (xpath('food/serving/@units', food))[1]::text,
                
                (xpath('food/calories/@total', food))[1]::text::int,
                (xpath('food/calories/@fat', food))[1]::text::int,
                
                (xpath('food/total-fat/text()', food))[1]::text::numeric,
                (xpath('food/saturated-fat/text()', food))[1]::text::numeric,
                (xpath('food/cholesterol/text()', food))[1]::text::numeric,
                (xpath('food/sodium/text()', food))[1]::text::numeric,
                (xpath('food/carb/text()', food))[1]::text::numeric,
                (xpath('food/fiber/text()', food))[1]::text::numeric,
                (xpath('food/protein/text()', food))[1]::text::numeric,
                
                (xpath('food/vitamins/a/text()', food))[1]::text::int,
                (xpath('food/vitamins/c/text()', food))[1]::text::int,
                (xpath('food/minerals/ca/text()', food))[1]::text::int,
                (xpath('food/minerals/fe/text()', food))[1]::text::int
            FROM raw_nutrition,
            LATERAL unnest(xpath('//food', file_content)) AS food;
        """
    )

    create_tables >> [load_pets, load_nutrition]
    load_pets >> parse_pets
    load_nutrition >> parse_nutrition