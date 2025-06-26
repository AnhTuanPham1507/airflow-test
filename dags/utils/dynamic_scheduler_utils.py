import requests
import re
import logging
import os
from typing import Dict, Any, List

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def sanitize_dag_id(id: str, serviceName: str) -> str:
    """
    Sanitize the name to be a valid Airflow DAG ID.
    Only allows alphanumeric characters, dashes, dots and underscores.
    """
    sanitized = re.sub(r'_+', '_', re.sub(r'[^a-zA-Z0-9\-\._]', '_', f'{serviceName}_{id}'.replace('/', '_').replace(' ', '_').replace('-', '_'))).strip('_').lower()
    return sanitized

def fetch_scheduler_configs(url: str) -> List[Dict[str, Any]]:
    """Fetch scheduler configurations from the API"""
    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f'API call failed with status code: {response.status_code}')
        configs = response.json()['data']['taskList']
        logger.info(f"Successfully fetched {len(configs)} scheduler configs")

        mapped_configs = []
        for config in configs:
            service_name = response.json()['data']['serviceName']
            sanitized_id = sanitize_dag_id(config['id'], service_name)
            detail_url = f"{url}/{config['id']}"   
            cron_time = config['cronTime']
            max_attempts = config.get('maxAttempts', 3)
            retry_delay = config.get('retryDelay', 5)

            config['id'] = sanitized_id
            config['url'] = detail_url
            config['service_name'] = service_name
            config['cron_time'] = cron_time
            config['max_attempts'] = max_attempts
            config['retry_delay'] = retry_delay
            mapped_configs.append(config)
        return mapped_configs
    except Exception as e:
        logger.error(f'Error fetching scheduler configs: {str(e)}')
        # Return empty list instead of raising exception to prevent DAG loading failure
        raise

def call_scheduler(scheduler_id, url: str):
    """Callable for the service task"""
    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f'Call scheduler {scheduler_id} failed with status code: {response.status_code}')
        return f'Successfully called scheduler {scheduler_id}'
    except Exception as e:
        logger.error(f'Error calling scheduler {scheduler_id}: {str(e)}') 