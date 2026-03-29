from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'abay',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='sales_dwh_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dwh'],
    template_searchpath=["/home/abayturar/airflow-project/dags/sql"]  # 🔥 ключевая строка
) as dag:

    load_staging = PostgresOperator(
        task_id='load_staging',
        postgres_conn_id='postgres_default',
        sql='pipeline/load_staging.sql'
    )

    scd2_update = PostgresOperator(
        task_id='scd2_update',
        postgres_conn_id='postgres_default',
        sql='pipeline/scd2_update.sql'
    )

    scd2_insert = PostgresOperator(
        task_id='scd2_insert',
        postgres_conn_id='postgres_default',
        sql='pipeline/scd2_insert.sql'
    )

    dim_product = PostgresOperator(
        task_id='dim_product',
        postgres_conn_id='postgres_default',
        sql='pipeline/dim_product.sql'
    )

    dim_ship_mode = PostgresOperator(
        task_id='dim_ship_mode',
        postgres_conn_id='postgres_default',
        sql='pipeline/dim_ship_mode.sql'
    )

    load_fact = PostgresOperator(
        task_id='load_fact',
        postgres_conn_id='postgres_default',
        sql='pipeline/load_fact.sql'
    )

    # зависимости
    load_staging >> scd2_update >> scd2_insert >> dim_product >> dim_ship_mode >> load_fact