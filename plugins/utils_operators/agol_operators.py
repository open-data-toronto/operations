# agol_operator.py - logic for all AGOL airflow operators

import logging
import os
import json
import hashlib
from datetime import datetime
from pathlib import Path

import requests
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils import agol_utils


class AGOLDownloadFileOperator(BaseOperator):
    """
    Downloads file from AGOL URL and saves to provided directory using provided filename.
    This will always overwrite an existing file, if it's there.
    This is because we are not interested in versioning AGOL data - only pulling the latest from AGOL.

    Returns a dictionary containing:
        - path: path to saved file containing data
        - last_modified: timestamp file was last_modified (from the request)
        - fields_path: path to file containing fields
        - checksum: md5 hash of the download's contents
    """

    @apply_defaults
    def __init__(
        self,
        file_url: str = None,
        file_url_task_id: str = None,
        file_url_task_key: str = None,

        dir: str = None,
        dir_task_id: str = None,
        dir_task_key: str = None, 
               
        filename: str = None,
        filename_task_id: str = None,
        filename_task_key: str = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_url, self.file_url_task_id, self.file_url_task_key = file_url, file_url_task_id, file_url_task_key
        self.dir, self.dir_task_id, self.dir_task_key = dir, dir_task_id, dir_task_key
        self.filename, self.filename_task_id, self.filename_task_key = filename, filename_task_id, filename_task_key
    
    def set_path(self, ti):
        # init the filepath to the file we will create
        # how this is done depends on whether the operator received task ids/keys, or actual values 
        if self.dir_task_id and self.dir_task_key:
            self.dir = ti.xcom_pull(task_ids=self.dir_task_id)[self.dir_task_key]

        if self.filename_task_id and self.filename_task_key:
            self.filename = ti.xcom_pull(task_ids=self.filename_task_id)[self.filename_task_key]

        self.path = Path(self.dir) / self.filename    
        

    def parse_properties_from_features(self, features):
        return [ object["properties"] for object in features ]


    def parse_data_from_agol(self):
        # calls agol utils to get only the features from a simple GET request to AGOL
        res = agol_utils.get_features( self.file_url )
        unparsed_last_modified = requests.get(self.file_url).headers["last-modified"]
        last_modified = datetime.strptime(unparsed_last_modified[:-13], "%a, %d %b %Y")
        fields = agol_utils.get_fields( self.file_url )
        
        logging.info("Received {} AGOL records".format(str(len(res))))

        return { "data": self.parse_properties_from_features(res),
                 "last_modified": last_modified,
                 "fields": fields
                }


    def write_to_file(self, data, filepath):
        # write the data to a file
        with open(filepath, "w") as f:
            f.write(data)

        checksum = hashlib.md5()
        checksum.update(data.encode('utf-8'))

        return checksum


    def execute(self, context):
        # init task instance context
        ti = context['ti']

        # create target filepath
        self.set_path(ti)

        # Store data in memory
        res = self.parse_data_from_agol()
        last_modified = res["last_modified"]

        # write data to a file, get checksum
        data = json.dumps(res["data"])
        checksum = self.write_to_file(data, self.path)

        # write fields to a file
        fields = json.dumps(res["fields"])
        fields_filename = "fields_" + self.filename
        fields_path = Path(self.dir) / fields_filename
        self.write_to_file(fields, fields_path)

        # Return dict with data's filepath, last modified date, and checksum
        return {
            "data_path": self.path,
            "fields_path": fields_path,
            "last_modified": last_modified,
            "checksum": checksum.hexdigest(),
        }