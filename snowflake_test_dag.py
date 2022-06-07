import logging

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="snowflake_connector", start_date=datetime(2022,6,5), schedule_interval="@daily"
)

create_insert_query = [
    """create table public.test_table (amount number);""",
    """insert into public.test_table values (1),(2),(3);""",
]


def row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("select count(*) from public.test_table")
    logging.info("Number of rows in `public.test_table`  - %s", result[0])


with dag:
    create_insert = SnowflakeOperator(
        task_id="snowfalke_create",
        sql=create_insert_query,
        snowflake_conn_id="snowflake_conn",
    )

    get_count = PythonOperator(task_id="get_count", python_callable=row_count)
create_insert >> get_count
