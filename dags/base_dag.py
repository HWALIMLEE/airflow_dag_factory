from utils.factory.dag_factory import DAGFactory
from utils.components.airflow_task import AirflowTask
from utils.components.operators import Operators
from utils.components.operator_args import PythonOperatorArgs

from datetime import datetime, timedelta
from airflow import DAG


class BaseDAG:
    def __init__(self, idx):
        self.idx = idx
        self.tasks = []

    def create_crawler_dag(self, dag_id):
        DEFAULT_ARGS = {
            'owner': 'HWALIMLEE',
            'start_date': datetime(2023, 1, 1),
            'retries': 3,
            'retry_delay': timedelta(minutes=1)
        }

        dag_args = {
            'default_args': DEFAULT_ARGS,
            'schedule_interval': '*/10 * * * *',
            'catchup': False
        }
        dag = DAG(dag_id=dag_id, **dag_args)
        self.tasks.extend([self.get_data(), self.update_data(), self.save_bigquery()])
        return DAGFactory.create_dag(self.tasks, dag)

    def _get_data(self):
        ...

    def _update_data(self):
        ...

    def _save_bigquery(self):
        ...

    def get_data(self):
        return AirflowTask(
            operator=Operators.PYTHON(),
            args=PythonOperatorArgs(
                task_id='get_data',
                python_callable=self._get_data
            )
        )

    def update_data(self):
        return AirflowTask(
            operator=Operators.PYTHON(),
            args=PythonOperatorArgs(
                task_id='update_data',
                python_callable=self._update_data
            )
        )

    def save_bigquery(self):
        return AirflowTask(
            operator=Operators.PYTHON(),
            args=PythonOperatorArgs(
                task_id='save_data',
                python_callable=self._save_bigquery
            )
        )
