import sys
import importlib.util
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator


GX_PLUGINS_DIR = '/opt/airflow/great_expectations'
OPERATOR_MODULE_PATH = f'{GX_PLUGINS_DIR}/gx_run_expectations_operator.py'
OPERATOR_MODULE_NAME = 'gx_run_expectations_operator'


DEFAULT_ARGS = {
    'owner': 'aleksandr.klein',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


def get_gx_run_expectations_operator():
    """Dynamically import and return the GXRunExpectationsOperator class."""
    sys.path.insert(0, GX_PLUGINS_DIR)

    spec = importlib.util.spec_from_file_location(OPERATOR_MODULE_NAME, OPERATOR_MODULE_PATH)
    custom_operator_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(custom_operator_module)

    return getattr(custom_operator_module, "GXRunExpectationsOperator")


dag_config = {
    'dag_id': '040_data_quality_checking',
    'default_args': DEFAULT_ARGS,
    'description': 'An Airflow DAG to run GX expectations',
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2024, 3, 19)
}

with DAG(**dag_config) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    GXRunExpectationsOperator = get_gx_run_expectations_operator()

    run_expectations = GXRunExpectationsOperator(
        task_id='run_expectations',
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    trigger_egress_to_hdfs = TriggerDagRunOperator(
        task_id="trigger_egress_to_hdfs",
        trigger_dag_id="050_egress_to_hdfs",
        wait_for_completion=False
    )

    start >> run_expectations >> trigger_egress_to_hdfs >> end
