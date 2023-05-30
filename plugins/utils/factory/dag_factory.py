from typing import List, Union

from utils.factory.task_factory import TaskFactory
from utils.components.airflow_task import AirflowTask

from airflow import DAG


class DAGFactory:

    @classmethod
    def create_dag(cls, tasks: List[Union[List[AirflowTask], AirflowTask]], dag) -> DAG:
        dependency_tasks = []
        for task in tasks:
            t = TaskFactory.generate_task(dag=dag, airflow_tasks=task)
            if dependency_tasks:
                upstream_task = dependency_tasks[-1]
                upstream_task.set_downstream(t)
            dependency_tasks.append(t)
        return dag

