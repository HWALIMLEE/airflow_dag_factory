from typing import List, Union

from utils.factory.task_factory import TaskFactory
from utils.components.airflow_task import AirflowTask

from airflow import DAG


class DAGFactory:

    @classmethod
    def create_dag(cls, tasks: List[Union[List[AirflowTask], AirflowTask]], dag):
        dependency_tasks = []
        for task in tasks:
            task_operators = TaskFactory.generate_task(dag=dag, airflow_tasks=task)
            if dependency_tasks:
                upstream_tasks = dependency_tasks[-1]
                if type(upstream_tasks) != list and type(task_operators) != list:
                    upstream_tasks.set_downstream(task_operators)
                elif type(upstream_tasks) != list and type(task_operators) == list:
                    for (task_operator, downstream_task_next) in task_operators:
                        upstream_tasks.set_downstream(task_operator)
                elif type(upstream_tasks) == list and type(task_operators) != list:
                    for (upstream_task, upstream_task_next) in upstream_tasks:
                        if upstream_task_next:
                            upstream_task.set_downstream(task_operators)
                else:
                    for (upstream_task, upstream_task_next) in upstream_tasks:
                        if upstream_task_next:
                            for (task_operator, downstream_task_next) in task_operators:
                                upstream_task.set_downstream(task_operator)
            dependency_tasks.append(task_operators)
        return dag

