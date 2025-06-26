import os

def get_dag_id(current_file_path: str):
    current_file_name_with_extension = os.path.basename(current_file_path)
    current_file_name = os.path.splitext(current_file_name_with_extension)[0]

    return current_file_name
