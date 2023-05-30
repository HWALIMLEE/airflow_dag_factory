from typing import Union, List

from airflow import DAG
from utils.components.airflow_task import AirflowTask


class TaskFactory:
    @classmethod
    def generate_task(
            cls,
            dag: DAG,
            airflow_tasks: Union[List[AirflowTask], AirflowTask]):

        if type(airflow_tasks) != list:
            task = airflow_tasks.operator(
                dag=dag,
                **airflow_tasks.args.to_args()
            )
            return task
        else:
            tasks = []
            for airflow_task in airflow_tasks:
                task = airflow_task.operator(
                    dag=dag,
                    **airflow_tasks.args.to_args()
                )
                tasks.append(task)
            return tasks
