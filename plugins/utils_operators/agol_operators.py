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

# init some vars to be used throughout this file
# maps agol data type to ckan data type
AGOL_CKAN_TYPE_MAP =  {
    "sqlTypeFloat": "float",
    "sqlTypeNVarchar": "text",
    "sqlTypeInteger": "int",
    "sqlTypeOther": "text",
    "sqlTypeTimestamp2": "timestamp",
    "sqlTypeDouble": "float",
    "esriFieldTypeString": "text",
    "esriFieldTypeDate": "timestamp",
    "esriFieldTypeInteger": "int",
    "esriFieldTypeOID": "int",
    "esriFieldTypeDouble": "float"
}

# list of attributes that are redundant when geometry is present in a response
DELETE_FIELDS = [
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

    def esri_timestamp_to_datetime(self, ts):
        #convert esri timestamp (which is unix) to ISO format datetime string
        return  datetime.fromtimestamp(ts/1000).isoformat()  
        

    def parse_properties_from_features(self, features, fields):
        # returns properties (and geometry, if present) from the agol response
        if "geometry" not in features[0].keys():
            return [ object["properties"] for object in features ]

        else:
            rows = []
            for feature in features:
                # create a row object for each object in the json response, and flag its timestamp fields
                row = {**feature["properties"], "geometry": json.dumps(feature["geometry"])}
                datetime_fields = [ field["id"] for field in fields if field["type"] == "timestamp" ]
                
                # for every row
                for k,v in {**row}.items():
                    # delete field if duplicate geo field (x, y, lat, long, etc.)
                    if k.lower() in DELETE_FIELDS:
                        del row[k]
                    
                    # delete "<null>" string field
                    if isinstance(v, str) and v.lower() == "<null>":
                        del row[k]
                        
                # convert esri time data types to JSON standard (for ckan)
                for dtf in datetime_fields:
                    row[dtf] = self.esri_timestamp_to_datetime(row[dtf])
                
                rows.append(row)

            return rows
        

    def parse_data_from_agol(self):
        # calls agol utils to get only the features from a simple GET request to AGOL
        res = agol_utils.get_features( self.file_url )
        unparsed_last_modified = requests.get(self.file_url).headers["last-modified"]
        last_modified = datetime.strptime(unparsed_last_modified[:-13], "%a, %d %b %Y")
        fields = agol_utils.get_fields( self.file_url )
        
        logging.info("Received {} AGOL records".format(str(len(res))))
        logging.info("Parsed the following fields: {}".format(fields))

        return { "data": self.parse_properties_from_features(res, fields),
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