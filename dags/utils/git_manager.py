import os
import subprocess
import logging
from typing import List, Dict, Optional
from datetime import datetime

class GitManager:
    """Manages Git operations for DAG files across multiple environments"""
    
    # Environment to branch mapping
    ENVIRONMENT_BRANCH_MAPPING = {
        'develop': 'anhtuan/testing-develop',
        'staging': 'staging', 
        'production': 'main',
        'release': 'release'
    }
    
    def __init__(self, current_environment: str):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        self.current_environment = current_environment
        self.target_branch = self.ENVIRONMENT_BRANCH_MAPPING.get(current_environment)
        
        if not self.target_branch:
            self.logger.warning(f"No branch mapping found for environment: {current_environment}")
            self.target_branch = 'develop'  # fallback
        
        self.logger.info(f"Initialized GitManager for environment: {self.current_environment}")
        self.logger.info(f"Target branch: {self.target_branch}")
        
    def commit_and_push_changes(self, operation_summary: Dict[str, List[str]]) -> bool:
        """
        Commit and push DAG file changes to the target branch for current environment
        
        Args:
            operation_summary: Dict with 'created', 'updated', 'deleted' file lists
        """
        try:
            # Check if there are any changes
            if not any(operation_summary.values()):
                self.logger.info("No DAG file changes detected, skipping Git operations")
                return True
            
            # If no target branch, skip Git operations
            if not self.target_branch:
                self.logger.warning("No target branch configured, skipping Git operations")
                return True
                
            self.logger.info(f"Processing Git operations for environment '{self.current_environment}' -> branch '{self.target_branch}'")
            
            # Ensure we're on the correct branch
            self._ensure_correct_branch(self.target_branch)
            
            # Stage changes
            self._stage_dag_changes()
            
            # Check if there are actually staged changes
            if not self._has_staged_changes():
                self.logger.info(f"No staged changes for branch {self.target_branch}, skipping commit")
                return True
            
            # Create commit message
            commit_message = self._generate_commit_message(operation_summary, self.current_environment)
            
            # Commit changes
            self._commit_changes(commit_message)
            
            # Push to remote
            self._push_to_remote(self.target_branch)
            
            self.logger.info(f"Successfully committed and pushed DAG changes to {self.target_branch} for environment {self.current_environment}")
            return True
                
        except Exception as e:
            self.logger.error(f"Git operation failed for environment {self.current_environment}: {str(e)}")
            return False
    
    def _ensure_correct_branch(self, target_branch: str):
        """Switch to the target branch, create if it doesn't exist"""
        try:
            # Fetch latest changes
            self._run_git_command(['git', 'fetch', 'origin'])
            
            # Check if branch exists locally
            result = self._run_git_command(['git', 'branch', '--list', target_branch], capture_output=True)
            branch_exists_locally = target_branch in result.stdout
            
            # Check if branch exists on remote
            result = self._run_git_command(['git', 'branch', '-r', '--list', f'origin/{target_branch}'], capture_output=True)
            branch_exists_remotely = f'origin/{target_branch}' in result.stdout
            
            if branch_exists_locally:
                # Switch to existing local branch
                self._run_git_command(['git', 'checkout', target_branch])
                # Pull latest changes if remote exists
                if branch_exists_remotely:
                    self._run_git_command(['git', 'pull', 'origin', target_branch])
            elif branch_exists_remotely:
                # Create local branch from remote
                self._run_git_command(['git', 'checkout', '-b', target_branch, f'origin/{target_branch}'])
            else:
                # Create new branch
                self._run_git_command(['git', 'checkout', '-b', target_branch])
                
            self.logger.info(f"Successfully switched to branch: {target_branch}")
            
        except Exception as e:
            self.logger.error(f"Failed to switch to branch {target_branch}: {str(e)}")
            raise
    
    def _stage_dag_changes(self):
        """Stage only DAG file changes"""
        try:
            # Stage all DAG files (both new and modified)
            self._run_git_command(['git', 'add', 'dags/dynamic_scheduler_*.py'])
            self.logger.info("Staged DAG file changes")
        except Exception as e:
            self.logger.error(f"Failed to stage changes: {str(e)}")
            raise
    
    def _has_staged_changes(self) -> bool:
        """Check if there are staged changes"""
        try:
            result = self._run_git_command(['git', 'diff', '--cached', '--name-only'], capture_output=True)
            return bool(result.stdout.strip())
        except Exception as e:
            self.logger.error(f"Failed to check staged changes: {str(e)}")
            return False
    
    def _generate_commit_message(self, operation_summary: Dict[str, List[str]], environment: str) -> str:
        """Generate descriptive commit message"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        message_parts = [f"chore: auto-sync DAG files for {environment} environment"]
        message_parts.append(f"Timestamp: {timestamp}")
        message_parts.append("")
        
        if operation_summary['created']:
            message_parts.append(f"Created files ({len(operation_summary['created'])}):")
            for file_name in operation_summary['created']:
                message_parts.append(f"  + {file_name}")
            message_parts.append("")
        
        if operation_summary['updated']:
            message_parts.append(f"Updated files ({len(operation_summary['updated'])}):")
            for file_name in operation_summary['updated']:
                message_parts.append(f"  ~ {file_name}")
            message_parts.append("")
        
        if operation_summary['deleted']:
            message_parts.append(f"Deactivated files ({len(operation_summary['deleted'])}):")
            for file_name in operation_summary['deleted']:
                message_parts.append(f"  - {file_name}")
        
        return "\n".join(message_parts)
    
    def _commit_changes(self, message: str):
        """Commit staged changes"""
        try:
            self._run_git_command(['git', 'commit', '-m', message])
            self.logger.info("Successfully committed changes")
        except Exception as e:
            self.logger.error(f"Failed to commit changes: {str(e)}")
            raise
    
    def _push_to_remote(self, branch: str):
        """Push changes to remote repository"""
        try:
            self._run_git_command(['git', 'push', 'origin', branch])
            self.logger.info(f"Successfully pushed to origin/{branch}")
        except Exception as e:
            self.logger.error(f"Failed to push to origin/{branch}: {str(e)}")
            raise
    
    def _run_git_command(self, command: List[str], capture_output: bool = False):
        """Run git command with proper error handling"""
        try:
            if capture_output:
                result = subprocess.run(
                    command, 
                    cwd=os.getcwd(), 
                    capture_output=True, 
                    text=True, 
                    check=True
                )
                return result
            else:
                subprocess.run(
                    command, 
                    cwd=os.getcwd(), 
                    check=True
                )
        except subprocess.CalledProcessError as e:
            error_msg = f"Git command failed: {' '.join(command)}"
            if capture_output and e.stderr:
                error_msg += f" - Error: {e.stderr}"
            raise Exception(error_msg) from e 