from datetime import datetime

from utils.get_dag_id import get_dag_id
from utils.variables import (
    mission_db,
    mission_mongo_uri,
    mission_user_rewards,
    tools_path,
    warehouse_db,
    warehouse_host,
    warehouse_password,
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

with DAG(
    get_dag_id(__file__),
    default_args=default_args,
    description="Export data from Mission",
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
              IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'user_reward') THEN
                  CREATE TABLE {warehouse_schema_staging}.user_reward (
                      id VARCHAR(24) PRIMARY KEY,
                      user_id UUID NOT NULL,
                      mission_key VARCHAR(50) NOT NULL,
                      point INTEGER NOT NULL,
                      is_issued BOOLEAN NOT NULL DEFAULT FALSE,
                      created_at TIMESTAMP WITH TIME ZONE,
                      updated_at TIMESTAMP WITH TIME ZONE
                  );
              ELSE
                  TRUNCATE TABLE {warehouse_schema_staging}.user_reward;
              END IF;
          END $$;
        """,
    )

    # Define the new header that will be used in the CSV
    new_header = "id,user_id,mission_key,point,is_issued,created_at,updated_at"

    mission_mongo_to_postgres_command = f"""
    {tools_path}/mongoexport \
    --uri='{mission_mongo_uri}' \
    --db={mission_db} \
    --collection={mission_user_rewards} \
    --type=csv \
    --fields="_id,userId,missionKey,point,isIssued,createdAt,updatedAt" \
    | sed 's/ObjectId(\\([[:alnum:]]*\\))/\\1/g' \
    | awk -v new_header="{new_header}" 'NR==1 {{$0=new_header}} {{print}}' \
    | PGPASSWORD={warehouse_password} psql -h {warehouse_host} -U {warehouse_user} -d {warehouse_db} \
    -c "COPY {warehouse_schema_staging}.user_reward (id, user_id, mission_key, point, is_issued, created_at, updated_at) FROM STDIN WITH CSV HEADER"
    """

    execute_mongo_to_postgres = BashOperator(
        task_id="mission_mongo_to_postgres_command",
        bash_command=mission_mongo_to_postgres_command,
        env={"new_header": new_header},
    )

    (create_schema >> create_table >> execute_mongo_to_postgres)
