#!python3
# agol_operator.py - logic for all AGOL airflow operators

import logging
import os
import json
import hashlib
from datetime import datetime
from pathlib import Path

import requests
from airflow.models.baseoperator import BaseOperator
from utils_operators.file_operator import DownloadFileOperator
from airflow.utils.decorators import apply_defaults
from utils import agol_utils


class AGOLDownloadFileOperator(BaseOperator):
    """
    Downloads file from AGOL URL and saves to provided directory using provided filename.
    This will always overwrite an existing file, if it's there.
    This is because we are not interested in versioning this data - only pulling the latest from AGOL.

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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_url = file_url
        self.dir = dir
        self.filename = filename
         
        # init the filepath to the file we will create
        self.path = Path(self.dir) / self.filename

    def parse_data_from_agol(self):
        res = agol_utils.get_data( self.file_url )
        last_modified = requests.get(self.file_url).headers["last-modified"]
        fields = agol_utils.get_fields( self.file_url )
        
        return { "data": res,
                 "last_modified": last_modified,
                 "fields": fields
                }

    def write_to_file(self, data, filepath):
        # write the data to a file
        with open(filepath, "w") as f:
            f.write(data)





    def execute(self, context):
        # Store data in memory
        res = self.parse_data_from_agol()
        last_modified = res["last_modified"]

        # write data to a file, get checksum
        data = json.dumps(res["data"])
        self.write_to_file(data, self.path)

        # write fields to a file
        fields = json.dumps(res["fields"])
        fields_filename = "fields_" + self.filename
        fields_path = Path(self.dir) / fields_filename
        self.write_to_file(fields, fields_path)


        #  Return dict with data's filepath, last modified date, and checksum
        return {
            "path": self.path,
            "fields_path": fields_path,
            "last_modified": last_modified,
            #"checksum": checksum.hexdigest(),
        }



    

