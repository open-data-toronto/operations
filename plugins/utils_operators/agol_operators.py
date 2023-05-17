# agol_operator.py - logic for all AGOL airflow operators

import logging
import os
import csv
import json
import hashlib
import shutil
from datetime import datetime
from pathlib import Path

import requests
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils import misc_utils


class AGOLDownloadFileOperator(BaseOperator):
    """
    Downloads data from AGOL URL 
    If new data != old data on the server, old data is replaced
    Data is saved as CSV provided directory using provided filename
    

    Expects the following inputs:
        - request_url - reference to url where this operator will grab data
        - dir - directory where the response from the above will be saved
        - filename - name of the file to be made
        Each of the three above can be given with an actual value, or with a
        reference to a task_id and task_key that returns the value

    Returns a dictionary containing:
        - data_path - path on server to newest data's file
        - needs_update - whether the existing file was replaced by a new one
    """

    @apply_defaults
    def __init__(
        self,
        request_url: str = None,
        request_url_task_id: str = None,
        request_url_task_key: str = None,

        dir: str = None,
        dir_task_id: str = None,
        dir_task_key: str = None, 
               
        filename: str = None,
        filename_task_id: str = None,
        filename_task_key: str = None,

        delete_col: list = [],
        create_backup: bool = False,

        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.request_url = request_url
        self.request_url_task_id = request_url_task_id
        self.request_url_task_key = request_url_task_key

        self.dir = dir
        self.dir_task_id = dir_task_id
        self.dir_task_key = dir_task_key

        self.filename = filename
        self.filename_task_id = filename_task_id
        self.filename_task_key = filename_task_key

        self.delete_col = delete_col
        self.create_backup = create_backup
        self.backup_path = None
    
    def set_path(self, ti):
        # init the filepath to the file we will create
        if self.dir_task_id and self.dir_task_key:
            self.dir = ti.xcom_pull(task_ids=self.dir_task_id)[self.dir_task_key]

        if self.filename_task_id and self.filename_task_key:
            self.filename = ti.xcom_pull(task_ids=self.filename_task_id)[self.filename_task_key]

        self.path = self.dir + "/" + self.filename  


    def get_fields(self, query_url):
        # list of attributes that are redundant when geometry is present in a response
        delete_fields = [
            "longitude",
            "latitude",
            "shape__length",
            "shape__area",
            "lat",
            "long",
            "lon",
            "x",
            "y",
            "index_"
        ]

        query_string_params = {
            "where": "1=1",
            "outFields": "*",
            "outSR": 4326,
            "f": "json",
            "resultRecordCount": 1
        }
        
        params = "&".join([ f"{k}={v}" for k,v in query_string_params.items() ])
        url = "https://" + "/".join([ url_part for url_part in f"{query_url}/query?{params}".replace("https://", "").split("/") if url_part ])
        res = requests.get(url)
        assert res.status_code == 200, f"Status code response: {res.status_code}"
        
        ckan_fieldnames = []
        for field in res.json()["fields"]:
            if field["name"].lower() not in [*delete_fields, *self.delete_col]:            
                ckan_fieldnames.append(field["name"])

        # check if geometry in response too
        if "geometry" in res.json()["features"][0].keys():
            ckan_fieldnames.append("geometry")

        logging.info("Utils parsed the following fields: {}".format(ckan_fieldnames))
            
        return ckan_fieldnames


    def agol_generator(self, query_url, fieldnames):
        '''yields chunks of AGOL records'''
        logging.info(f"  getting features from AGOL")

        records = 0
        overflow = True
        offset = 0
        query_string_params = {
            "where": "1=1",
            "outSR": 4326,
            "f": "geojson",
            "resultType": "standard",
            "resultOffset": offset,
            "outFields": ",".join([f for f in fieldnames if f!="geometry"])
        }

        while overflow is True:
            # As long as there are more records to get on another request
            
            # make a request url 
            params = "&".join([ f"{k}={v}" for k,v in query_string_params.items() ])
            url = "https://" + "/".join([ url_part for url_part in f"{query_url}/query?{params}".replace("https://", "").split("/") if url_part ])
            logging.info(url)

            # make the request
            res = requests.get(url)
            assert res.status_code == 200, f"Status code response: {res.status_code}"
            
            # parse the response
            geojson = json.loads(res.text)
            # get the properties out of each returned object
            for object in geojson["features"]:
                this_record = object["properties"]
                this_record["geometry"] = json.dumps(object["geometry"])
                yield(this_record)

            # prepare the next request, if needed
            if "exceededTransferLimit" in geojson:
                overflow = geojson["exceededTransferLimit"] is True

            elif "properties" in geojson:
                overflow = "properties" in geojson and "exceededTransferLimit" in geojson["properties"] and geojson["properties"]["exceededTransferLimit"] is True

            else:
                overflow = False

            offset = offset + len(geojson["features"])
            query_string_params["resultOffset"] = offset


    def agol_to_csv(self, query_url, path, fieldnames):
        '''write agol records to CSV on disk'''        
        with open(path, "w") as f:
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerows(self.agol_generator(query_url, fieldnames))
               
            f.close()

        logging.info("Returned AGOL records")
        return


    def execute(self, context):
        # init task instance context
        ti = context['ti']

        # create target filepath
        self.set_path(ti)

        # get fieldnames
        fieldnames = self.get_fields(self.request_url)

        # write new agol data to local temp CSV
        self.agol_to_csv(self.request_url, self.path + "-temp", fieldnames)

        # get new and old data's hashes
        new_hash = misc_utils.file_to_md5(self.path + "-temp")
        if not os.path.isfile(self.path):
            old_hash = "no_old_file"
        else:
            old_hash = misc_utils.file_to_md5(self.path)

        # if new and old files dont match, replace old with new
        if new_hash != old_hash:
            logging.info("New data does not match old data's hash")
            
            # make a backup
            if self.create_backup and os.path.exists(self.path):
                # write backup and remember backup location
                # we'll want to use or delete it later
                self.backup_path = self.path + "-backup"
                shutil.copy(self.path, self.backup_path)

            shutil.move(self.path + "-temp", self.path)
            needs_update = True
        # otherwise, get rid of the new "-temp" file
        else:
            logging.info("New data matches old data's hash")
            os.remove(self.path + "-temp")
            needs_update = False

        logging.info(self.path)
        
        return {
                "data_path": self.path,
                "needs_update": needs_update,
                "backup_path": self.backup_path
                }
