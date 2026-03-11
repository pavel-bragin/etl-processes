import sys
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "p_bragin",
    "start_date": datetime(2026, 3, 1),
    "catchup": False,
}


def generate():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "hw-5"))
    from generate_mongo_data import generate_all
    generate_all()


with DAG(
    dag_id="hw5_generate_mongo_data",
    default_args=default_args,
    schedule_interval=None,
    description="Generate synthetic data into MongoDB (run once before replication)",
    tags=["hw5", "mongodb", "generate"],
) as dag:

    PythonOperator(
        task_id="generate_mongo_data",
        python_callable=generate,
    )
