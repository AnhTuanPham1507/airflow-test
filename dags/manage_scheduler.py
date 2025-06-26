from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.session import provide_session

from utils.dynamic_scheduler_utils import fetch_scheduler_configs
from utils.file_manager import FileDagProvider
from utils.git_manager import GitManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# # load all service base urls 
# ENV_SERVICE_HOSTS = os.environ.get('SERVICE_HOSTS', '').split(',')
# ENV = os.environ.get('ENVIRONMENTS', 'develop,staging,production,release').split(',')
ENV = ['develop']

def sync_dags(**context):
    """
    Sync scheduler from all services to files and commit to multiple environment branches.
    This function gets all configurations, delegates the comprehensive
    DAG file management to FileDagProvider, and handles Git operations for multiple environments.
    """
    try:
        for env in ENV:
            # SERVICE_BASE_URLS = [
            #     f"http://{service_name}.{env}.svc.cluster.local:{port}/internal/schedulers" 
            #     for service_host in ENV_SERVICE_HOSTS if service_host.strip()
            #     for service_name, port in [service_host.strip().split(':')]
            # ]
            SERVICE_BASE_URLS = [
                f"http://10.20.50.83:3001/internal/schedulers"
            ]

            logger.info(f"Starting dynamic DAG file synchronization for environment: {env}")
            
            # Get configurations from all services
            configs = []
            for url in SERVICE_BASE_URLS:
                config = fetch_scheduler_configs(url)
                configs.extend(config)

            # Comprehensive DAG file management with change tracking
            changes = FileDagProvider.create_or_update_dag_file(configs)
            
            # Git operations for multiple environments
            git_manager = GitManager(env)            
            git_success = git_manager.commit_and_push_changes(changes)
                
            if git_success:
                logger.info("DAG file synchronization and Git operations completed successfully")
            else:
                logger.warning("DAG file synchronization completed but some Git operations failed")
            
    except Exception as e:
        logger.error(f"Error synchronizing DAG files: {str(e)}")

manager_dags = DAG(
    "manage_dags",
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime.now(),
        "email_on_failure": False,
        "email_on_retry": False,
        "owner": "airflow",
    },
    description="Dynamic Scheduler DAG Creator",
    schedule_interval="0 0 * * *",  # Run at midnight every day
    catchup=False,
    tags=["scheduler"],
)

with manager_dags:
    # Create task for syncing DAGs to files
    sync_dags_task = PythonOperator(
        task_id='sync_dags',
        python_callable=sync_dags,
        provide_context=True,
    )
