def get_scheduler_dag_template(
    dag_id: str,
    url: str,
    service_name: str,
    cron_time: str = '@daily',
    max_attempts: int = 3,
    retry_delay: int = 5
) -> str:
    """
    Generate a scheduler DAG template string with the provided parameters.
    
    Args:
        dag_id: Unique identifier for the DAG
        config_id: Configuration ID for the scheduler
        url: URL to call for the scheduler
        service_name: Name of the service
        cron_time: Cron expression for scheduling (default: '@daily')
        max_attempts: Maximum retry attempts (default: 3)
        retry_delay: Retry delay in minutes (default: 5)
    
    Returns:
        str: Complete DAG definition as a Python string
    """
    # Create a valid Python variable name by replacing invalid characters with underscores
    task_id_name = f"task_{dag_id}"
    
    template = f'''from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.dynamic_scheduler_utils import call_scheduler

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': {max_attempts},
    'retry_delay': timedelta(minutes={retry_delay}),
}}

{task_id_name} = DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='Scheduler DAG for {dag_id}',
    schedule_interval='{cron_time}',
    catchup=False,
    tags=['scheduler', '{service_name}'],
)

with {task_id_name}:
    {task_id_name} = PythonOperator(
        task_id='{task_id_name}',
        python_callable=lambda: call_scheduler('{dag_id}', '{url}'),
        provide_context=True,
    )
'''
    return template


def get_inactive_scheduler_dag_template(
    dag_id: str,
    url: str,
    service_name: str,
    original_cron_time: str = '@daily',
    max_attempts: int = 3,
    retry_delay: int = 5,
    deactivated_timestamp: str = None
) -> str:
    """
    Generate an inactive scheduler DAG template string with the provided parameters.
    
    Args:
        dag_id: Unique identifier for the DAG
        url: URL to call for the scheduler
        service_name: Name of the service
        original_cron_time: Original cron expression for scheduling (default: '@daily')
        max_attempts: Maximum retry attempts (default: 3)
        retry_delay: Retry delay in minutes (default: 5)
        deactivated_timestamp: Timestamp when the DAG was deactivated
    
    Returns:
        str: Complete inactive DAG definition as a Python string
    """
    # Create a valid Python variable name by replacing invalid characters with underscores
    task_id = f"{dag_id}"
    
    template = f'''# ===== DAG MARKED AS INACTIVE =====
# Last deactivated: {deactivated_timestamp}
# Reason: Configuration no longer exists in source services
# This DAG is paused and will not run automatically
# Original schedule: {original_cron_time}
# ===================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.dynamic_scheduler_utils import call_scheduler

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': {max_attempts},
    'retry_delay': timedelta(minutes={retry_delay}),
}}

{task_id} = DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='INACTIVE - Scheduler DAG for {dag_id}',
    schedule_interval=None,  # INACTIVE - Original: {original_cron_time}
    catchup=False,
    is_paused_upon_creation=True,  # MARKED AS INACTIVE
    tags=['scheduler', '{service_name}', 'removed'],
)

with {task_id}:
    {task_id} = PythonOperator(
        task_id='{task_id}',
        python_callable=lambda: call_scheduler('{dag_id}', '{url}'),
        provide_context=True,
    )
'''
    return template 