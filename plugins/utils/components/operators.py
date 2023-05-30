from airflow.operators.python import PythonOperator


class Operators:
    @classmethod
    def PYTHON(cls):
        return PythonOperator
