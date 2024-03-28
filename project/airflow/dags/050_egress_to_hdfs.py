from plugins.egress_to_hdfs import egress_to_hdfs
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'aleksandr.klein',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


dag_config = {
    'dag_id': '050_egress_to_hdfs',
    'default_args': DEFAULT_ARGS,
    'description': 'An Airflow DAG to run egress data from rating datamart',
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2024, 3, 19)
}

with DAG(**dag_config) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    egress_to_hdfs = PythonOperator(
        task_id='postgres_to_hdfs',
        python_callable=egress_to_hdfs,
        op_kwargs={
            'table_mappings': {
                'dm_rating.top_bottom_five': 'top_bottom_five.csv',
                'dm_rating.top_five_increased_rating': 'top_five_increased_rating.csv'
            },
        },
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    start >> egress_to_hdfs >> end
