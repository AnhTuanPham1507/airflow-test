from datetime import datetime

from utils.get_dag_id import get_dag_id
from utils.variables import (
    content_postgres_db,
    content_postgres_host,
    content_postgres_password,
    content_postgres_port,
    content_postgres_schema,
    content_postgres_user,
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

source_table_name = "posts"
destination_table_name = "post"

with DAG(
    get_dag_id(__file__),
    default_args=default_args,
    description="Export data",
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
              IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{destination_table_name}') THEN
                CREATE TABLE {warehouse_schema_staging}.{destination_table_name} (
                    id uuid DEFAULT gen_random_uuid() NOT NULL
                    , comments_count int4 DEFAULT 0 NOT NULL
                    , is_important bool DEFAULT false NULL
                    , privacy varchar(50) NULL
                    , total_users_seen int4 DEFAULT 0 NULL
                    , lang varchar(3) NULL
                    , "type" varchar(50) DEFAULT 'POST' NOT NULL
                    , is_hidden bool DEFAULT false NOT NULL
                    , status varchar(50) DEFAULT 'DRAFT' NULL
                    , word_count int4 DEFAULT 0 NULL
                    , created_by uuid NULL
                    , updated_by uuid NULL
                    , updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL
                    , deleted_at timestamptz NULL
                );
              ELSE
                  TRUNCATE TABLE {warehouse_schema_staging}.{destination_table_name};
              END IF;
          END $$;
        """,
    )

    columns = 'id, comments_count, is_important, privacy, total_users_seen, lang, "type", is_hidden, status, word_count, created_by, updated_by, updated_at, deleted_at'

    postgres_to_postgres = BashOperator(
        task_id="postgres_to_postgres",
        bash_command=f"""
            PGPASSWORD={content_postgres_password} psql -h {content_postgres_host} -p {content_postgres_port} \\
            -U {content_postgres_user} -d {content_postgres_db} -c \\
            "COPY (SELECT {columns} FROM {content_postgres_schema}.{source_table_name}) TO STDOUT" | \\
            PGPASSWORD={warehouse_password} psql -h {warehouse_host} -p {warehouse_port} \\
            -U {warehouse_user} -d {warehouse_db} -c \\
            "COPY {warehouse_schema_staging}.{destination_table_name} ( {columns} ) FROM STDIN"
        """,
    )

    (create_schema >> create_table >> postgres_to_postgres)
