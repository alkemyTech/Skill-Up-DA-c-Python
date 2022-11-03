from datetime import datetime, timedelta

from airflow.decorators import dag, task 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

@dag(
    'empty_dag',
    description = 'Dag con task dummyoperator, comentados los que se van a usar luego',
    schedule_interval = "@hourly",
    start_date = datetime(2022, 11, 3)
)
def empty_dag():
    task_1 = DummyOperator(task_id='task_1') #PostgresOperator(task_id= palermo_extract, sql=C-UNpalermo.sql, retries = 5, postgres_conn_id=)
    task_2 = DummyOperator(task_id='task_2') #PostgresOperator(task_id= jujuy_extract, sql=C-UNjujuy.sql, retries = 5, postgres_conn_id=)
    task_3 = DummyOperator(task_id='task_3') #PythonOperator(task_id=transform, retries = 5, python_callable=transform_data)
    task_4 = DummyOperator(task_id='task_4') #PythonOperator(task_id=load, retries = 5, python_callable=s3_load)

    [task_1, task_2] >> task_3 >> task_4

dag = empty_dag()