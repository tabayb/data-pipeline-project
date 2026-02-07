from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello():
    print("Hello Abay! ✅")

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="hello_task", python_callable=hello)
