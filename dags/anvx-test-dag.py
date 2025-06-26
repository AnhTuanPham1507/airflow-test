from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def print_var():
    my_var = Variable.get("variables")
    print(f'My variable is: {my_var}')

with DAG('example_secrets_dags', start_date=datetime(2022, 1, 1), schedule=None) as dag:

  test_task = PythonOperator(
      task_id='anvx-test-task',
      python_callable=print_var,
)
