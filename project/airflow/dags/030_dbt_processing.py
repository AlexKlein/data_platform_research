from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


DEFAULT_ARGS = {
    'owner': 'aleksandr.klein',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag_config = {
    'dag_id': '030_dbt_processing',
    'default_args': DEFAULT_ARGS,
    'description': 'An Airflow DAG to run DBT models',
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2024, 3, 19)
}

with DAG(**dag_config) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    dbt_processing = BashOperator(
        task_id='dbt_processing',
        bash_command='cd /opt/airflow/dbt && dbt run',
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    trigger_dq_checking = TriggerDagRunOperator(
        task_id="trigger_dbt_processing",
        trigger_dag_id="040_data_quality_checking",
        wait_for_completion=False
    )

    start >> dbt_processing >> trigger_dq_checking >> end
