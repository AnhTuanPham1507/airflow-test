import logging
import json
import os
import zipfile
import requests
import io
import yaml
from datetime import datetime
from utils.variables import (
    warehouse_db,
    warehouse_host,
    warehouse_password,
    warehouse_port,
    warehouse_user,
    superset_api_host,
    superset_password,
    superset_user,
)
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils.get_dag_id import get_dag_id

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}
import zipfile
import io


script_dir = os.path.dirname(__file__)
dashboard_directory = os.path.join(script_dir, "dashboards/dashboard_export")
zip_file_path = dashboard_directory + ".zip"
yaml_file_path = f"dashboard_export/databases/{warehouse_db}.yaml"


# Function to update the YAML file content in memory
def update_yaml_file(zip_data):
    with zipfile.ZipFile(io.BytesIO(zip_data), "r") as z:
        with z.open(yaml_file_path) as f:
            yaml_content = yaml.safe_load(f)

    # Modify the YAML content
    sqlalchemy_uri_line = f"postgresql+psycopg2://{warehouse_user}:{warehouse_password}@{warehouse_host}:{warehouse_port}/{warehouse_db}?sslmode=require"
    yaml_content["sqlalchemy_uri"] = sqlalchemy_uri_line

    # Write back the updated YAML to a new zip file in memory
    updated_zip_data = io.BytesIO()
    with zipfile.ZipFile(updated_zip_data, "w", zipfile.ZIP_DEFLATED) as z:
        for item in zipfile.ZipFile(io.BytesIO(zip_data)).infolist():
            if item.filename != yaml_file_path:
                z.writestr(
                    item.filename,
                    zipfile.ZipFile(io.BytesIO(zip_data)).read(item.filename),
                )
        # Add the updated yaml file back
        z.writestr(
            yaml_file_path,
            yaml.dump(yaml_content),
        )

    return updated_zip_data.getvalue()


def login_to_superset(session):
    login_url = f"{superset_api_host}/security/login"
    login_data = {
        "password": superset_password,
        "provider": "db",
        "username": superset_user,
    }
    response = session.post(login_url, json=login_data)
    response.raise_for_status()
    access_token = response.json().get("access_token")
    if not access_token:
        logging.info("Response from Superset login: %s", response.text)
        raise ValueError("Failed to retrieve access token")
    print("Logged in to Superset successfully")
    return f"Bearer {access_token}"


def get_csrf_token(session):
    csrf_url = f"{superset_api_host}/security/csrf_token"
    response = session.get(csrf_url)
    response.raise_for_status()
    csrf_token = response.json().get("result")
    print("Get Superset CSRF token successfully")

    if not csrf_token:
        logging.info("Response from Superset CSRF token: %s", response.text)
        raise ValueError("Failed to retrieve CSRF token")
    return csrf_token


def import_dashboard_to_superset(zip_data):
    import_url = f"{superset_api_host}/dashboard/import/"

    session = requests.Session()
    session.headers.update({"Authorization": login_to_superset(session)})
    session.headers.update({"X-CSRFToken": get_csrf_token(session)})

    files = [
        (
            "formData",
            (
                "dashboard_export.zip",
                zip_data,
                "application/zip",
            ),
        )
    ]
    data = {
        "passwords": json.dumps({"databases/cdp.yaml": warehouse_password}),
        "overwrite": "true",
    }

    response = session.post(import_url, data=data, files=files)
    response.raise_for_status()
    logging.info("Response from Superset import: %s", response.text)


with DAG(
    get_dag_id(__file__),
    default_args=default_args,
    description="Import dashboard into Superset. Only run manually when the dashboard needs to be synced",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def import_dashboard(**context):
        # Load the zip file into memory
        with open(zip_file_path, "rb") as f:
            zip_data = f.read()

        # Update the YAML file in memory and get the updated zip data
        updated_zip_data = update_yaml_file(zip_data)

        import_dashboard_to_superset(updated_zip_data)

    task_import_dashboard = PythonOperator(
        task_id="import_dashboard",
        python_callable=import_dashboard,
        provide_context=True,
    )
