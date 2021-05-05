from pathlib import Path

import requests
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class DownloadFileOperator(BaseOperator):
    """
    Downloads file from URL and saves to provided directory using provided filename
    """

    @apply_defaults
    def __init__(
        self,
        file_url: str,
        dir_task_id: str,
        filename: str,
        overwrite_if_exists: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_url = file_url
        self.dir_task_id = dir_task_id
        self.filename = filename
        self.overwrite_if_exists = overwrite_if_exists

    def execute(self, context):
        print(context)
        path = Path(context["ti"].xcom_pull(task_id=self.dir_task_id)) / self.filename

        if not self.overwrite_if_exists and path.exists():
            return path

        res = requests.get(self.file_url)
        assert res.status_code == 200, f"Response status: {res.status_code}"

        with open(path, "wb") as f:
            f.write(res.content)

        return path
