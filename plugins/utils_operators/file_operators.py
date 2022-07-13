import hashlib
import logging
import os
import io
import zipfile
import json
from datetime import datetime
from pathlib import Path

import requests

from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class DownloadFileOperator(BaseOperator):
    """
    Downloads file from URL and saves to provided directory using provided filename.
    Takes strings or reference to task ids + keys returning strings as inputs

    Returns a dictionary containing:
        - path: path to saved file
        - last_modified: timestamp file was last_modified (from the request)
        - checksum: using md5 algorithm
    """

    @apply_defaults
    def __init__(
        self,
        file_url: str = None,
        file_url_task_id: str = None,
        file_url_task_key: str= None,

        dir: str = None,
        dir_task_id: str = None,
        dir_task_key: str= None,
        
        filename: str = None,
        filename_task_id: str = None, 
        filename_task_key: str= None,

        overwrite_if_exists: bool = True,

        custom_headers: dict = {},
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        # Assigning these vars in single lines to emphasize that they are alternatives to one another
        self.file_url, self.file_url_task_id, self.file_url_task_key = file_url, file_url_task_id, file_url_task_key
        self.dir, self.dir_task_id, self.dir_task_key = dir, dir_task_id, dir_task_key
        self.filename, self.filename_task_id, self.filename_task_key = filename, filename_task_id, filename_task_key
                    
        self.overwrite_if_exists = overwrite_if_exists

        # pull headers from airflow variables, as needed
        self.custom_headers = {k:Variable.get(v) for k,v in custom_headers.items()}

    def set_path(self, ti):
        # init the filepath to the file we will create
        # how this is done depends on whether the operator received task ids/keys, or actual values 
        if self.dir_task_id and self.dir_task_key:
            self.dir = ti.xcom_pull(task_ids=self.dir_task_id)[self.dir_task_key]

        if self.filename_task_id and self.filename_task_key:
            self.filename = ti.xcom_pull(task_ids=self.filename_task_id)[self.filename_task_key]

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

    def get_data_from_http_request(self, ti):
        # get url if its from a separate task
        if self.file_url_task_id and self.file_url_task_key:
            self.file_url = ti.xcom_pull(task_ids=self.file_url_task_id)[self.file_url_task_key]

        # grab data from input url
        res = requests.get(self.file_url, headers = self.custom_headers)
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
            "data_path": self.path,
            "last_modified": last_modified,
            "checksum": checksum.hexdigest(),
        }
            


    def execute(self, context):
        # set task instance from context
        ti = context['ti']

        # create filepath for file well create
        self.set_path(ti)

        # if the file exists already and we don't want to overwrite it
        if self.overwrite_file_check():
            result = self.return_current_file_metadata()
            
        # if the file doesn't exist or we're ok with overwriting an existing one
        else:
            # get data from http request
            res = self.get_data_from_http_request(ti)

            # write response to file and get its md5 hash
            checksum = self.write_response_to_file_and_return_hash(res)

            # create result
            result = self.return_new_file_metadata(res, checksum)
            

        logging.info(f"Returning: {result}")

        return result


class DownloadZipOperator(BaseOperator):
    """
    Input zip url and output directory, and this operator will grab a zip file from an online location
    and unzip all of its contents into the output directory
    """

    @apply_defaults
    def __init__(
        self,
        file_url: str = None,
        file_url_task_id: str = None,
        file_url_task_key: str= None,

        dir: str = None,
        dir_task_id: str = None,
        dir_task_key: str= None,
        
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_url, self.file_url_task_id, self.file_url_task_key = file_url, file_url_task_id, file_url_task_key
        self.dir, self.dir_task_id, self.dir_task_key = dir, dir_task_id, dir_task_key

    def execute(self, context):
        # set task instance from context
        ti = context['ti']

        # create filepath for file well create
        # how this is done depends on whether the operator received task ids/keys, or actual values 
        if self.dir_task_id and self.dir_task_key:
            self.dir = ti.xcom_pull(task_ids=self.dir_task_id)[self.dir_task_key]

        if self.file_url_task_id and self.file_url_task_key:
            self.file_url = ti.xcom_pull(task_ids=self.file_url_task_id)[self.file_url_task_key]

        # extract files into self.path
        r = requests.get(self.file_url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(self.dir)

        # build output dict of filenames
        filenames = []
        print("Printing files in folder")
        for files in os.walk(self.dir):
            print(files)
            for file in files:
                print(file)
                filenames.append(file)
        
        data_path = {filename: self.dir + "/" + filename for filename in filenames}
        print(filenames)
        print(data_path)
        return {
            "path": data_path,
            "data_path": data_path
        }


class DownloadGeoJsonOperator(BaseOperator):
    """
    This is a stripped down copy of the AgolDownloadFileOperator
    Downloads and parses GeoJson file from URL and saves to provided directory using provided filename as a flat json file.
    Takes strings or reference to task ids + keys returning strings as inputs.

    Returns a dictionary containing:
        - path: path to saved file
        - last_modified: timestamp file was last_modified (from the request)
        - checksum: using md5 algorithm
    """

    @apply_defaults
    def __init__(
        self,
        file_url: str = None,
        file_url_task_id: str = None,
        file_url_task_key: str= None,

        dir: str = None,
        dir_task_id: str = None,
        dir_task_key: str= None,
        
        filename: str = None,
        filename_task_id: str = None, 
        filename_task_key: str= None,

        overwrite_if_exists: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        # Assigning these vars in single lines to emphasize that they are alternatives to one another
        self.file_url, self.file_url_task_id, self.file_url_task_key = file_url, file_url_task_id, file_url_task_key
        self.dir, self.dir_task_id, self.dir_task_key = dir, dir_task_id, dir_task_key
        self.filename, self.filename_task_id, self.filename_task_key = filename, filename_task_id, filename_task_key

        # list of fields we dont want to accept
        # list of attributes that are redundant when geometry is present in a response
        self.DELETE_FIELDS = [
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

    def set_path(self, ti):
        # init the filepath to the file we will create
        # how this is done depends on whether the operator received task ids/keys, or actual values 
        if self.dir_task_id and self.dir_task_key:
            self.dir = ti.xcom_pull(task_ids=self.dir_task_id)[self.dir_task_key]

        if self.filename_task_id and self.filename_task_key:
            self.filename = ti.xcom_pull(task_ids=self.filename_task_id)[self.filename_task_key]

        return Path(self.dir) / self.filename

    def get_features(self):
        # flattens file by removing all subobjects from each of the input file's dicts
        output = []
        res = json.loads( requests.get( self.file_url ).text )

        # for each feature, combine it with its geometry into one object and append it to the output
        for feature in res["features"]:
        # create a row object for each object in the json response, and flag its timestamp fields
            row = {**feature["properties"], "geometry": json.dumps(feature["geometry"])}
            
            # for every row
            for k,v in {**row}.items():
                # delete field if duplicate geo field (x, y, lat, long, etc.)
                if k.lower() in self.DELETE_FIELDS:
                    del row[k]

            output.append( row )
        
        return output

    def write_to_file(self, data, filepath):
        # write the data to a file
        with open(filepath, "w") as f:
            f.write( json.dumps(data) )
        

    def execute(self, context):
        # init task instance context
        ti = context['ti']

        # create target filepath
        path = self.set_path(ti)

        # store parsed data in memory
        data = self.get_features()

        # write data into a file
        self.write_to_file(data, path)

        return {
            "data_path": path,
        }

    