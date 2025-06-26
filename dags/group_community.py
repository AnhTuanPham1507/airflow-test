from datetime import datetime

from utils.get_dag_id import get_dag_id
from utils.variables import (
    group_postgres_db,
    group_postgres_host,
    group_postgres_password,
    group_postgres_port,
    group_postgres_schema,
    group_postgres_user,
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

table_name = "community"

with DAG(
    get_dag_id(__file__),
    default_args=default_args,
    description="Export data from Group",
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
                    id uuid NOT NULL,
                    created_at timestamptz NOT NULL,
                    deleted_at timestamptz NULL,
                    group_id uuid NULL,
                    owner_id uuid NULL,
                    settings jsonb DEFAULT '{{}}'::jsonb NOT NULL,
                    categories VARCHAR[] DEFAULT '{{}}' NULL,
                    member_count int4 DEFAULT 0 NULL,
                    setups VARCHAR[] DEFAULT '{{}}' NULL,
                    is_linked_nft bool DEFAULT false NOT NULL,
                    languages VARCHAR[] DEFAULT '{{}}' NULL,
                    state varchar(100) DEFAULT 'ACTIVE'::character varying NOT NULL
                );
              ELSE
                  TRUNCATE TABLE {warehouse_schema_staging}.{table_name};
              END IF;
          END $$;
        """,
    )

    columns = "id, created_at, deleted_at, group_id, owner_id, settings, categories, member_count, setups, is_linked_nft, languages, state"

    postgres_to_postgres = BashOperator(
        task_id="postgres_to_postgres",
        bash_command=f"""
            PGPASSWORD={group_postgres_password} psql -h {group_postgres_host} -p {group_postgres_port} \\
            -U {group_postgres_user} -d {group_postgres_db} -c \\
            "COPY (SELECT {columns} FROM {group_postgres_schema}.{table_name}) TO STDOUT" | \\
            PGPASSWORD={warehouse_password} psql -h {warehouse_host} -p {warehouse_port} \\
            -U {warehouse_user} -d {warehouse_db} -c \\
            "COPY {warehouse_schema_staging}.{table_name} ( {columns} ) FROM STDIN"
        """,
    )

    (create_schema >> create_table >> postgres_to_postgres)
