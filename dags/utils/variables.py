# common_airflow_utils.py

import os

from airflow.models import Variable

superset_api_host = Variable.get("SUPERSET_API_HOST")
superset_user = Variable.get("SUPERSET_USER")
superset_password = Variable.get("SUPERSET_PASSWORD")

warehouse_schema_staging = Variable.get("WAREHOUSE_SCHEMA_STAGING")
warehouse_host = Variable.get("WAREHOUSE_HOST")
warehouse_port = Variable.get("WAREHOUSE_PORT")
warehouse_db = Variable.get("WAREHOUSE_DB")
warehouse_user = Variable.get("WAREHOUSE_USER")
warehouse_password = Variable.get("WAREHOUSE_PASSWORD")


user_postgres_host = Variable.get("USER_POSTGRES_HOST")
user_postgres_port = Variable.get("USER_POSTGRES_PORT")
user_postgres_db = Variable.get("USER_POSTGRES_DB")
user_postgres_user = Variable.get("USER_POSTGRES_USER")
user_postgres_password = Variable.get("USER_POSTGRES_PASSWORD")
user_postgres_schema = Variable.get("USER_POSTGRES_SCHEMA")


group_postgres_host = Variable.get("GROUP_POSTGRES_HOST")
group_postgres_port = Variable.get("GROUP_POSTGRES_PORT")
group_postgres_db = Variable.get("GROUP_POSTGRES_DB")
group_postgres_user = Variable.get("GROUP_POSTGRES_USER")
group_postgres_password = Variable.get("GROUP_POSTGRES_PASSWORD")
group_postgres_schema = Variable.get("GROUP_POSTGRES_SCHEMA")

content_postgres_host = Variable.get("CONTENT_POSTGRES_HOST")
content_postgres_port = Variable.get("CONTENT_POSTGRES_PORT")
content_postgres_db = Variable.get("CONTENT_POSTGRES_DB")
content_postgres_user = Variable.get("CONTENT_POSTGRES_USER")
content_postgres_password = Variable.get("CONTENT_POSTGRES_PASSWORD")
content_postgres_schema = Variable.get("CONTENT_POSTGRES_SCHEMA")


mission_mongo_uri = Variable.get("MISSION_MONGO_URI")
mission_db = Variable.get("MISSION_DB")
mission_user_rewards = Variable.get("MISSION_USER_REWARDS")

chain_mongo_uri = Variable.get("CHAIN_MONGO_URI")
chain_db = Variable.get("CHAIN_DB")
chain_nft = Variable.get("CHAIN_NFT")


script_dir = os.path.dirname(__file__)
tools_path = os.path.join(script_dir, "../../tools")
