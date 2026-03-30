from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'abay',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

DBT_PROJECT_DIR = "/home/abayturar/airflow-project/sales_dwh"
DBT_PROFILES_DIR = "/home/abayturar/.dbt"
VENV_PATH = "/home/abayturar/airflow-project/venv/bin/activate"

with DAG(
    dag_id='sales_dwh_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dwh'],
    template_searchpath=["/home/abayturar/airflow-project/dags/sql"]
) as dag:

    # 1. staging загрузка (оставляем)
    load_staging = PostgresOperator(
        task_id='load_staging',
        postgres_conn_id='postgres_default',
        sql='pipeline/load_staging.sql'
    )

    # 2. dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"""
        source {VENV_PATH} && \
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \
        cd {DBT_PROJECT_DIR} && \
        dbt run
        """
    )

    # 3. dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"""
        source {VENV_PATH} && \
        export DBT_PROFILES_DIR={DBT_PROFILES_DIR} && \
        cd {DBT_PROJECT_DIR} && \
        dbt test
        """
    )

    # зависимости
    load_staging >> dbt_run >> dbt_test