from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


DEFAULT_ARGS = {
    'owner': 'aleksandr.klein',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag_config = {
    'dag_id': '031_dbt_docs_generation',
    'default_args': DEFAULT_ARGS,
    'description': 'An Airflow DAG to run generation of DBT documentation',
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2024, 3, 19)
}

with DAG(**dag_config) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generation',
        bash_command='cd /opt/airflow/dbt && dbt docs generate && dbt docs serve --port 8081',
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    start >> dbt_docs_generate >> end
