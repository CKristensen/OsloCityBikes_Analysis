from datetime import datetime, date, timedelta
import time
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import requests
import runners.airflow_op as tasks

default_args = {
    'owner': 'camaca',
    'depends_on_past': False,
    'email': ['cjckris@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1200),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    'wait_for_downstream': True,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'daily_update',
    default_args=default_args,
    description='A simple ping DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    # tags=['example'],
)

print_start = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

rides = PythonOperator(task_id='get_yesterday_rides', python_callable=tasks.get_yesterday_rides,
    dag=dag,)

wind = PythonOperator(task_id='tasks.get_wind_yesterday', python_callable=tasks.get_wind_yesterday,
    dag=dag,)

temperature = PythonOperator(task_id='tasks.get_temperature_yesterday', python_callable=tasks.get_temperature_yesterday,
    dag=dag,)

precipitation = PythonOperator(task_id='tasks.get_precipitation_yesterday', python_callable=tasks.get_precipitation_yesterday,
    dag=dag,)

transform = PythonOperator(task_id='tasks.transform_yesterdays_data', python_callable=tasks.transform_yesterdays_data,
    dag=dag,)

send_to_db = PythonOperator(task_id='tasks.data_into_db', python_callable=tasks.data_into_db,
    dag=dag,)

transform_in_db = PythonOperator(task_id='tasks.transform_in_db', python_callable=tasks.transform_in_db,
    dag=dag,)

print_start >> rides >> temperature >> precipitation >> wind >> transform >> send_to_db >> transform_in_db