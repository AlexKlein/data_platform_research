from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


MY_SMALL_DWH_HOST = os.getenv('POSTGRES_HOST')
MY_SMALL_DWH_PORT = os.getenv('POSTGRES_PORT')
MY_SMALL_DWH_DB = os.getenv('POSTGRES_DB')
MY_SMALL_DWH_USER = os.getenv('POSTGRES_USER')
MY_SMALL_DWH_PASSWORD = os.getenv('POSTGRES_PASSWORD')


DEFAULT_ARGS = {
    'owner': 'aleksandr.klein',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag_config = {
    'dag_id': '020_run_migration',
    'default_args': DEFAULT_ARGS,
    'description': 'An Airflow DAG to run migrations in Flyway',
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2024, 3, 21)
}

with DAG(**dag_config) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    command = (f'flyway -url=jdbc:postgresql://{MY_SMALL_DWH_HOST}:{MY_SMALL_DWH_PORT}/{MY_SMALL_DWH_DB} '
               f'-user={MY_SMALL_DWH_USER} -password={MY_SMALL_DWH_PASSWORD} '
               f'-locations=filesystem:/opt/airflow/migrations migrate')

    run_migration = BashOperator(
        task_id='run_migration',
        bash_command=command,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    trigger_hdfs_to_pg = TriggerDagRunOperator(
        task_id="trigger_hdfs_to_pg",
        trigger_dag_id="021_load_staging",
        wait_for_completion=False
    )

    start >> run_migration >> trigger_hdfs_to_pg >> end
