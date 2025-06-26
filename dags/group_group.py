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

table_name = "group"

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
                id uuid NOT NULL
                , "name" TEXT NOT NULL
                , "level" int2 NOT NULL
                , created_at timestamp(3) DEFAULT CURRENT_TIMESTAMP NOT NULL
                , deleted_at timestamp(3) NULL
                , "privacy" varchar(255) NOT NULL
                , parent_id uuid NULL
                , parents _uuid DEFAULT ARRAY[]::uuid[] NOT NULL
                , created_by uuid NULL
                , settings jsonb DEFAULT '{{}}'::jsonb NOT NULL
                , community_id uuid NOT NULL
                , referral_code varchar(255) NULL
                , state varchar(255) DEFAULT 'ACTIVE'::CHARACTER VARYING NOT NULL
                  );
              ELSE
                  TRUNCATE TABLE {warehouse_schema_staging}.{table_name};
              END IF;
          END $$;
        """,
    )

    columns = "id, name, level, created_at, deleted_at, privacy, parent_id, parents, created_by, settings, community_id, referral_code, state"

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
