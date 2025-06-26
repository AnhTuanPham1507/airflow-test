from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.dynamic_scheduler_utils import call_scheduler

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

task_dead_letter_dead_letter_retry = DAG(
    dag_id='dead_letter_dead_letter_retry',
    default_args=default_args,
    description='Scheduler DAG for dead_letter_dead_letter_retry',
    schedule_interval='0 */10 * * * *',
    catchup=False,
    tags=['scheduler', 'Dead Letter'],
)

with task_dead_letter_dead_letter_retry:
    task_dead_letter_dead_letter_retry = PythonOperator(
        task_id='task_dead_letter_dead_letter_retry',
        python_callable=lambda: call_scheduler('dead_letter_dead_letter_retry', 'http://10.20.50.83:3001/internal/schedulers/dead-letter/retry'),
        provide_context=True,
    )
