import logging
from datetime import datetime

from utils.get_dag_id import get_dag_id
from utils.variables import user_postgres_schema, warehouse_schema_staging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

table_name = "snapshot"

# Define the DAG
with DAG(
    get_dag_id(__file__),
    default_args=default_args,
    description="Export data",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="warehouse_db_id",
        show_return_value_in_logs=True,
        sql=f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}') THEN
                CREATE TABLE {warehouse_schema_staging}.{table_name} (
                    created_at date DEFAULT now() NOT NULL
                    , user_drop_off_rate float DEFAULT 0 NOT NULL
                    , CONSTRAINT snapshot_key UNIQUE (created_at)
                );
                END IF;
            END $$;
        """,
    )

    def calculate_and_insert_drop_off_rate():
        # Set up logging
        logging.basicConfig(level=logging.INFO)

        # Connect to the source database
        source_pg_hook = PostgresHook(postgres_conn_id="user_postgres_db_id")

        # Connect to the target database
        target_pg_hook = PostgresHook(postgres_conn_id="warehouse_db_id")

        # Define the query to calculate drop-off rate
        drop_off_rate_query = f"""
        SELECT
            count(*)::float / (
                SELECT
                    count(*)
                FROM
                    {user_postgres_schema}."user" u
                WHERE
                    u.is_deactivated IS NOT TRUE
            )::float AS drop_off_rate
        FROM
            {user_postgres_schema}."user" u
        WHERE
            u.is_deactivated IS NOT TRUE
            AND id IN (
                SELECT
                    user_id
                FROM
                    {user_postgres_schema}.login_log ll
                GROUP BY
                    user_id
                HAVING
                    max(created_at) < now() - INTERVAL '30 DAYS'
            );
        """

        # Execute the query on the source database
        drop_off_rate = source_pg_hook.get_first(drop_off_rate_query)[0]

        print(f"drop_off_rate: {drop_off_rate}")

        # Log the drop-off rate
        logging.info(f"Calculated drop-off rate: {drop_off_rate}")

        # Define the insert query for the target database
        insert_query = f"""
        INSERT INTO {warehouse_schema_staging}."snapshot" (created_at, user_drop_off_rate)
        VALUES (now(), %s)
        ON CONFLICT (created_at) DO UPDATE SET user_drop_off_rate = EXCLUDED.user_drop_off_rate;
        """

        # Execute the insert query on the target database
        target_pg_hook.run(insert_query, parameters=(drop_off_rate,))

    # Create a PythonOperator to execute the function
    calculate_and_insert_task = PythonOperator(
        task_id="calculate_and_insert_drop_off_rate",
        python_callable=calculate_and_insert_drop_off_rate,
    )
