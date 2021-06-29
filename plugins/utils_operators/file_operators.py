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
        filename: str,
        overwrite_if_exists: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_url = file_url
        self.dir = dir
        self.filename = filename
        self.overwrite_if_exists = overwrite_if_exists

        # init the filepath to the file we will create
        self.path = Path(self.dir) / self.filename

    def overwrite_file_check(self):
        # if the file exists already and we don't want to overwrite it, return false
        return not self.overwrite_if_exists and self.path.exists()

    def return_current_file_metadata(self):
        # make a hash of the file 
        checksum = hashlib.md5()
        f = open(self.path, "rb")
        content = f.read()
        checksum.update(content)

        s = os.stat(self.path)

        # return file hash and other metadata
        return {
            "path": self.path,
            "last_modified": datetime.fromtimestamp(s.st_mtime).isoformat(),
            "checksum": checksum.hexdigest(),
        }

    def get_data_from_http_request(self):
        # grab data from input url
        res = requests.get(self.file_url)
        assert res.status_code == 200, f"Response status: {res.status_code}"

        return res

    def write_response_to_file_and_return_hash(self, res):
        # write the data to a file
        with open(self.path, "wb") as f:
            f.write(res.content)

        # make a hash out of the data
        checksum = hashlib.md5()
        checksum.update(res.content)

        return checksum

    def return_new_file_metadata(self, res, checksum):
        # init last-modified date of file, if available in response
        if "last-modified" in res.headers.keys():
            last_modified = res.headers["last-modified"]
        else:
            last_modified = ""


        # return file hash and other metadata
        return {
            "path": self.path,
            "last_modified": last_modified,
            "checksum": checksum.hexdigest(),
        }
            


    def execute(self, context):
        # if the file exists already and we don't want to overwrite it
        if self.overwrite_file_check():
            result = self.return_current_file_metadata()
            
        # if the file doesn't exist or we're ok with overwriting an existing one
        else:
            # get data from http request
            res = self.get_data_from_http_request()

            # write response to file and get its md5 hash
            checksum = self.write_response_to_file_and_return_hash(res)

            # create result
            result = self.return_new_file_metadata(res, checksum)
            

        logging.info(f"Returning: {result}")

        return result
