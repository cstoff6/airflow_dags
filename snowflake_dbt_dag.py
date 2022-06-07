import logging

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.helpers import chain
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



#chain(*task_list)


dag = DAG(
    dag_id="snowflake_dbt_customer_orders"
    ,start_date=datetime(2022,6,5)
    ,schedule_interval="*/5 * * * *"
    ,catchup=False
)

new_records_query = [
"""
WITH records AS(
    SELECT *
    FROM
        SOURCE.RAW_CUSTOMER_TRANSACTIONS
    WHERE
        date>(SELECT
                MAX(date) AS max_date
              FROM
                FINAL.FNL_CUST_ORDER_KEEP)
)
SELECT
    COUNT(*) AS record_count
FROM
    records
"""
]

def new_record_count(ti,**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first(new_records_query[0])
    logging.info("Number of new records in RAW_CUSTOMER_TRANSACTIONS  - %s", result[0])
    #pushes the record count to be accessed later in another task

    ti.xcom_push(key = 'new_record_count', value = int(result[0]))


def count_validation(ti):
    #pulls value from query that was stored when value was pushed
    cnt = ti.xcom_pull(key = 'new_record_count',task_ids = 'queries_raw_table_for_new_records')

    if cnt==0:
        logging.info("No new records observed, all remaining objects in pipeline will be killed")
        return False
    else:
        logging.info(f"New Records observed in the amount of {cnt}, downstream processes will now continue")
        return True

def test_string():
    print("THIS IS A TEST")

with dag:

    checks_new_records = PythonOperator(
                        task_id="queries_raw_table_for_new_records"
                        ,python_callable=new_record_count
    )

    new_records_true = ShortCircuitOperator(
                        task_id="new_records_true"
                        ,python_callable=count_validation
    )

    _test_task = PythonOperator(
                        task_id = "_test_task"
                        ,python_callable=test_string
    )
    dbt_run = BashOperator(
                        task_id = 'initiate_dbt_run'
                        ,bash_command='cd /Users/collinstoffel/Documents/dbt_repo/dbt_demo_new && dbt run'
    )


checks_new_records >> new_records_true >> _test_task >> dbt_run
