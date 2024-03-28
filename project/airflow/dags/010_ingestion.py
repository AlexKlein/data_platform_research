from plugins.ingest import data_consumption, data_storage
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
    'dag_id': '010_ingest_data',
    'default_args': DEFAULT_ARGS,
    'description': 'An Airflow DAG to run data consuming and ingestion',
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2024, 3, 13)
}

with DAG(**dag_config) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    data_consuming = PythonOperator(
        task_id='data_consuming',
        python_callable=data_consumption,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    data_storing = PythonOperator(
        task_id='data_storing',
        python_callable=data_storage,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    trigger_migration = TriggerDagRunOperator(
        task_id="trigger_migration",
        trigger_dag_id="020_run_migration",
        wait_for_completion=False
    )

    start >> data_consuming >> data_storing >> trigger_migration >> end
