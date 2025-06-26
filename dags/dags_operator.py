import os
import time
from datetime import datetime

from utils.get_dag_id import get_dag_id

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator

script_dir = os.path.dirname(__file__)
current_file = os.path.basename(__file__)


DAGS_FOLDER = os.path.join(script_dir, ".")
excludedDags = ("superset_sync_dashboard",)


# List all Python files in DAGS_FOLDER excluding the current file and those in excludedDags
dags = sorted(
    os.path.splitext(filename)[0]
    for filename in os.listdir(DAGS_FOLDER)
    if filename.endswith(".py")
    and filename != current_file
    and os.path.splitext(filename)[0] not in excludedDags
)


def delay_execution(delay_seconds):
    time.sleep(delay_seconds)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

dags_operator = DAG(
    get_dag_id(__file__),
    default_args=default_args,
    description="A dags that schedules the others to run one by one after 5 minutes",
    schedule_interval="0 12 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=1,
)

dag_operator_tasks = {}

previous_task = None

for i, dag_id in enumerate(dags):
    # Create a task to trigger the DAG
    schedule_task = TriggerDagRunOperator(
        task_id=f"schedule_{dag_id}",
        trigger_dag_id=dag_id,
        execution_date="{{ logical_date + macros.timedelta(minutes="
        + str(i * 5)
        + ") }}",
        dag=dags_operator,
    )

    # Set dependencies between tasks
    if previous_task:
        previous_task >> schedule_task

    # Update previous_task to the current trigger_task
    previous_task = schedule_task
