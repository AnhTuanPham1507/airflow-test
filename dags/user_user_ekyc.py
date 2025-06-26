from datetime import datetime

from utils.get_dag_id import get_dag_id
from utils.variables import (
    user_postgres_db,
    user_postgres_host,
    user_postgres_password,
    user_postgres_port,
    user_postgres_schema,
    user_postgres_user,
    warehouse_db,
    warehouse_host,
    warehouse_password,
    warehouse_port,
    warehouse_schema_staging,
    warehouse_user,
)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

table_name = "user_ekyc"

with DAG(
    get_dag_id(__file__),
    default_args=default_args,
    description="Export data from User",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
) as dag:

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="warehouse_db_id",
        show_return_value_in_logs=True,
        sql=f"CREATE SCHEMA IF NOT EXISTS {warehouse_schema_staging};",
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="warehouse_db_id",
        show_return_value_in_logs=True,
        sql=f"""
          DO $$
          BEGIN
              IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}') THEN
                CREATE TABLE {warehouse_schema_staging}.{table_name} (
                    user_id uuid NOT NULL,
                    identity_type text NOT NULL,
                    front_id_attempts int4 DEFAULT 0 NOT NULL,
                    back_id_attempts int4 DEFAULT 0 NOT NULL,
                    face_attempts int4 DEFAULT 0 NOT NULL,
                    ekyc_attempts int4 DEFAULT 0 NOT NULL,
                    status text NOT NULL,
                    step text NOT NULL,
                    created_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at timestamptz NULL
                );
              ELSE
                  TRUNCATE TABLE {warehouse_schema_staging}.{table_name};
              END IF;
          END $$;
        """,
    )

    columns = "user_id, identity_type, front_id_attempts, back_id_attempts, face_attempts, ekyc_attempts, status, step, created_at, updated_at"

    postgres_to_postgres = BashOperator(
        task_id="postgres_to_postgres",
        bash_command=f"""
            PGPASSWORD={user_postgres_password} psql -h {user_postgres_host} -p {user_postgres_port} \\
            -U {user_postgres_user} -d {user_postgres_db} -c \\
            "COPY (SELECT {columns} FROM {user_postgres_schema}.{table_name}) TO STDOUT" | \\
            PGPASSWORD={warehouse_password} psql -h {warehouse_host} -p {warehouse_port} \\
            -U {warehouse_user} -d {warehouse_db} -c \\
            "COPY {warehouse_schema_staging}.{table_name} ( {columns} ) FROM STDIN"
        """,
    )

    (create_schema >> create_table >> postgres_to_postgres)
