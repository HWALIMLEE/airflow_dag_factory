from typing import Callable


class BaseOperatorArgs:
    def to_args(self):
        return self.__dict__


class PythonOperatorArgs(BaseOperatorArgs):
    def __init__(self,
                 python_callable: Callable,
                 task_id=None,
                 op_kwargs=None,
                 op_args=None):
        super(BaseOperatorArgs, self).__init__()
        self.task_id = python_callable.__name__ if not task_id else task_id
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs
        self.op_args = op_args
