import os
import re
import logging
from datetime import datetime
from utils.dag_template import get_scheduler_dag_template, get_inactive_scheduler_dag_template

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class FileDagProvider:
    """File DAG provider that creates and manages DAGs as Python files"""
    
    DAGS_DIR = 'dags'  # Generate DAGs directly in dags folder
    PREFIX = 'dynamic_scheduler_'

    @staticmethod
    def create_or_update_dag_file(configs):
        """
        Comprehensive DAG file management:
        - Create new DAG files if they don't exist
        - Update DAG files if they exist in dags folder AND in configs
        - Mark inactive if they exist in dags folder but NOT in configs
        
        Args:
            configs: List of all active DAG configurations
            
        Returns:
            Dict with 'created', 'updated', 'deleted' file lists for Git operations
        """
        logger.info("Starting comprehensive DAG file management")
        
        # Track changes for Git operations
        changes = {
            'created': [],
            'updated': [],
            'deleted': []
        }
        
        for config in configs:       
            id = config['id']      
            try:
                file_path = f"{FileDagProvider.DAGS_DIR}/{FileDagProvider.__create_file_name_from_id(id)}"
                file_name = FileDagProvider.__create_file_name_from_id(id)
                
                # Check if file exists to determine if it's create or update
                file_exists = os.path.exists(file_path)
                
                FileDagProvider.__upsert_dag_content(config, file_path)
                
                if file_exists:
                    changes['updated'].append(file_name)
                    logger.info(f"Updated DAG file: {id}")
                else:
                    changes['created'].append(file_name)
                    logger.info(f"Created new DAG file: {id}")
                    
            except Exception as e:
                logger.error(f"Error creating/updating DAG file {id}: {str(e)}")
        
        # mark inactive files(if exists in dags folder but not in configs)
        existing_scheduler_files = FileDagProvider.__get_existing_scheduler_files()        
        ids = [config['id'] for config in configs]
        for file_name in existing_scheduler_files:
            dag_id = FileDagProvider.__get_id_from_file_name(file_name)
            
            if dag_id not in ids:
                # Scenario 3: Mark inactive (exists in folder but not in configs)
                try:
                    logger.info(f"Marking scheduler DAG file as inactive: {file_name}")
                    FileDagProvider.__mark_inactive_dag(file_name)
                    changes['deleted'].append(file_name)
                except Exception as e:
                    logger.error(f"Error marking DAG file {file_name} as inactive: {str(e)}")
        
        logger.info("Comprehensive DAG file management completed")
        return changes
    
    @staticmethod
    def __get_existing_scheduler_files():
        """Get all existing scheduler DAG files (with scheduler_ prefix) in the dags folder"""
        scheduler_files = []
        
        if os.path.exists(FileDagProvider.DAGS_DIR):
            for filename in os.listdir(FileDagProvider.DAGS_DIR):
                if (filename.startswith(FileDagProvider.PREFIX) and filename.endswith('.py')):
                    scheduler_files.append(filename)
        
        logger.info(f"Found {len(scheduler_files)} existing scheduler files: {scheduler_files}")
        return scheduler_files
    
    @staticmethod
    def __mark_inactive_dag(file_name):
        """Update DAG file content to mark it as inactive using the inactive template"""
        file_path = f"{FileDagProvider.DAGS_DIR}/{file_name}"
        
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Skip if already marked as inactive
            if "# ===== DAG MARKED AS INACTIVE =====" in content:
                logger.info(f"DAG file {file_name} is already marked as inactive")
                return
            
            # Extract original configuration from existing file
            dag_id = FileDagProvider.__get_id_from_file_name(file_name)
            original_config = FileDagProvider.__extract_config_from_content(content, dag_id)
            
            # Generate inactive content using template
            inactive_content = get_inactive_scheduler_dag_template(
                dag_id=original_config['dag_id'],
                url=original_config['url'],
                service_name=original_config['service_name'],
                original_cron_time=original_config['cron_time'],
                max_attempts=original_config['max_attempts'],
                retry_delay=original_config['retry_delay'],
                deactivated_timestamp=datetime.now().isoformat()
            )
            
            with open(file_path, 'w') as f:
                f.write(inactive_content)
            
            logger.info(f"Updated DAG content to mark as inactive using template: {file_name}")
        else:
            logger.warning(f"File not found for deactivation: {file_path}")
    
    @staticmethod
    def __extract_config_from_content(content, dag_id):
        """Extract configuration parameters from existing DAG file content"""
        
        # Extract schedule_interval
        schedule_match = re.search(r"schedule_interval='([^']*)'", content)
        cron_time = schedule_match.group(1) if schedule_match else '@daily'
        
        # Extract retries
        retries_match = re.search(r"'retries': (\d+)", content)
        max_attempts = int(retries_match.group(1)) if retries_match else 3
        
        # Extract retry_delay
        retry_delay_match = re.search(r"timedelta\(minutes=(\d+)\)", content)
        retry_delay = int(retry_delay_match.group(1)) if retry_delay_match else 5
        
        # Extract URL from call_scheduler call
        url_match = re.search(r"call_scheduler\('[^']*', '([^']*)'\)", content)
        url = url_match.group(1) if url_match else 'unknown'
        
        # Extract service name from tags
        service_match = re.search(r"tags=\['scheduler', '([^']*)'\]", content)
        service_name = service_match.group(1) if service_match else 'unknown'
        
        return {
            'dag_id': dag_id,
            'url': url,
            'service_name': service_name,
            'cron_time': cron_time,
            'max_attempts': max_attempts,
            'retry_delay': retry_delay
        }
    
    @staticmethod
    def __create_file_name_from_id(id):
        return f"{FileDagProvider.PREFIX}{id}.py"
    
    @staticmethod
    def __get_id_from_file_name(file_name: str):
        return file_name.replace(FileDagProvider.PREFIX, '').replace('.py', '')
    
    @staticmethod
    def __upsert_dag_content(config, file_path):
        """Generate DAG file from template function with call_scheduler task"""
        dag_id = config['id']
        
        try:
            # Generate DAG content using template function
            dag_content = get_scheduler_dag_template(
                dag_id=dag_id,
                url=config['url'],
                service_name=config['service_name'],
                cron_time=config.get('cron_time', '@daily'),
                max_attempts=config.get('max_attempts', 3),
                retry_delay=config.get('retry_delay', 5)
            )
            
            # Write DAG file
            with open(file_path, 'w') as f:
                f.write(dag_content)
            
            logger.info(f"Generated DAG file: {file_path}")
            
        except Exception as e:
            logger.error(f"Error generating DAG file {file_path}: {str(e)}")
            raise 