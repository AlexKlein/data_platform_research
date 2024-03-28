from plugins.parse_and_store import parse_and_store_metadata, parse_and_store_ratings
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'aleksandr.klein',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


dag_config = {
    'dag_id': '021_load_staging',
    'default_args': DEFAULT_ARGS,
    'description': 'An Airflow DAG to run paring and storing data in a DB staging schema',
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2024, 3, 19)
}

with DAG(**dag_config) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    parse_and_store_metadata = PythonOperator(
        task_id='parse_and_store_metadata',
        python_callable=parse_and_store_metadata,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    parse_and_store_ratings = PythonOperator(
        task_id='parse_and_store_ratings',
        python_callable=parse_and_store_ratings,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    trigger_dbt_processing = TriggerDagRunOperator(
        task_id="trigger_dbt_processing",
        trigger_dag_id="030_dbt_processing",
        wait_for_completion=False
    )

    start >> [parse_and_store_metadata, parse_and_store_ratings] >> trigger_dbt_processing >> end
