from base_dag import BaseDAG


class TestDAG(BaseDAG):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


airflow_dag = TestDAG(idx=1).create_crawler_dag(dag_id='test_dag')

