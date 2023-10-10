import os
import shutil
from pathlib import Path

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateLocalDirectoryOperator(BaseOperator):
    """
    Creates local folder and returns the path
    """

    @apply_defaults
    def __init__(
        self,
        path: str,
        create_parents: bool = False,
        ok_if_exists: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.file_path = path
        self.parents = create_parents
        self.exist_ok = ok_if_exists

    def execute(self, context):
        path = Path(self.file_path)
        path.mkdir(parents=self.parents, exist_ok=self.exist_ok)

        return str(path)


class DeleteLocalDirectoryOperator(BaseOperator):
    """
    Deletes local folder and all its contents (including subfolders) unless specified
    """

    @apply_defaults
    def __init__(self,
    path = None,
    path_task_id: str = None,
    path_task_key: str = None,
    delete_recursively: bool = True,
    **kwargs):
        super().__init__(**kwargs)
        self.file_path = path
        self.file_path_task_id = path
        self.file_path_task_key = path

        self.recursively = delete_recursively

    def execute(self, context):
        # init task instance from context
        ti = context['ti']

        # get path if provided by another task
        # commented out as it causes a "cannot adapt" error from psycopg, and is currently not needed
        # if self.file_path_task_id and self.file_path_task_key:
        #     self.file_path = ti.xcom_pull(task_ids=self.file_path_task_id)[self.file_path_task_key]

        path = Path(self.file_path) if type(self.file_path) == "str" else self.file_path
            
        if not self.recursively:
            os.rmdir(path)
        else:
            shutil.rmtree(path)

        return {"path": path}
