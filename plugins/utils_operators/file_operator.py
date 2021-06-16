import hashlib
import logging
import os
from datetime import datetime
from pathlib import Path

import requests
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class DownloadFileOperator(BaseOperator):
    """
    Downloads file from URL and saves to provided directory using provided filename.

    Returns a dictionary containing:
        - path: path to saved file
        - last_modified: timestamp file was last_modified (from the request)
        - checksum: using md5 algorithm
    """

    @apply_defaults
    def __init__(
        self,
        file_url: str,

        dir: str,
        #TODO replace dir_task_id with an actual filepath
        
        filename: str,
        overwrite_if_exists: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_url = file_url
        self.dir = dir
        self.filename = filename
        self.overwrite_if_exists = overwrite_if_exists

    def execute(self):
        # init the filepath to the file we will create
        #TODO 
        # replace assignment of "path" to read from an input var, not xcom
        #path = Path(context["ti"].xcom_pull(task_ids=self.dir_task_id)) / self.filename
        path = Path(self.dir) / self.filename

        # if the file exists already and we don't want to overwrite it
        if not self.overwrite_if_exists and path.exists():

            # make a hash of the file 
            checksum = hashlib.md5()
            f = open(path, "rb")
            content = f.read()
            checksum.update(content)

            s = os.stat(path)

            # return file hash and other metadata
            result = {
                "path": path,
                "last_modified": datetime.fromtimestamp(s.st_mtime).isoformat(),
                "checksum": checksum.hexdigest(),
            }
        # if the file doesn't exist or we're ok with overwriting an existing one
        else:
            # grab data from input url
            res = requests.get(self.file_url)
            assert res.status_code == 200, f"Response status: {res.status_code}"

            # make a hash out of the file
            checksum = hashlib.md5()
            with open(path, "wb") as f:
                f.write(res.content)

            checksum.update(res.content)

            # init last-modified date of file, if available in response
            if "last-modified" in res.headers.keys():
                last_modified = res.headers["last-modified"]
            else:
                last_modified = ""


            # return file hash and other metadata
            result = {
                "path": path,
                "last_modified": last_modified,
                "checksum": checksum.hexdigest(),
            }

        logging.info(f"Returning: {result}")

        return result
