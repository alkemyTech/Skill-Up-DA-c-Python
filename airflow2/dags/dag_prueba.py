from datetime import datetime, timedelta

from airflow.decorators import dag, task 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

with DAG(
    'empty_dag',
    description = 'Dag con task dummyoperator, comentados los que se van a usar luego',
    scheduled_interval = @hourly,
    start_date = datetime(2022, 11, 3),
    retries = 5
    ) as dag:
    task_1 = DummyOperator(task_id='task_1') #PostgresOperator(task_id= palermo_extract, sql=C-UNpalermo.sql, postgres_conn_id=)
    task_2 = DummyOperator(task_id='task_2') #PostgresOperator(task_id= jujuy_extract, sql=C-UNjujuy.sql, postgres_conn_id=)
    task_3 = DummyOperator(task_id='task_3') #PythonOperator(task_id=transform, python_callable=transform_data)
    task_4 = DummyOperator(task_id='task_4') #PythonOperator(task_id=load, python_callable=s3_load)

[task_1, task_2] >> task_3 >> task_4