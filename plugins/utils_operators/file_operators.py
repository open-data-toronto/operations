import hashlib
import logging
import os
import shutil
import io
import csv
import zipfile
import json
from datetime import datetime
from pathlib import Path
from utils import misc_utils

import requests

from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class DownloadFileOperator(BaseOperator):
    """
    Downloads file from URL and saves to input directory using input filename.
    Takes strings or reference to task ids + keys returning strings as inputs.

    Returns a dictionary containing:
        - path: path to saved file
        - needs_update: using md5 algorithm
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

        create_backup: bool = False,
        custom_headers: dict = {},

        stream: bool = True,


        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        # Assigning these vars in single lines to emphasize that they are alternatives to one another
        self.file_url = file_url
        self.file_url_task_id = file_url_task_id
        self.file_url_task_key = file_url_task_key
        
        self.dir = dir
        self.dir_task_id = dir_task_id
        self.dir_task_key = dir_task_key
        self.filename = filename
        self.filename_task_id = filename_task_id
        self.filename_task_key = filename_task_key

        # pull headers from airflow variables, as needed
        self.custom_headers = {k:Variable.get(v) for k,v in custom_headers.items()}
        self.create_backup = create_backup

        self.stream = stream

    def set_path(self, ti):
        # init the filepath to the file we will create
        if self.dir_task_id and self.dir_task_key:
            self.dir = ti.xcom_pull(task_ids=self.dir_task_id)[self.dir_task_key]

        if self.filename_task_id and self.filename_task_key:
            self.filename = ti.xcom_pull(task_ids=self.filename_task_id)[self.filename_task_key]

        self.path = self.dir + "/" + self.filename

        # get url if its from a separate task, if needed
        if self.file_url_task_id and self.file_url_task_key:
            self.file_url = ti.xcom_pull(task_ids=self.file_url_task_id)[self.file_url_task_key]


    def stream_response_to_file(self, path):
        # grab data from input url
        res = requests.get(self.file_url, headers=self.custom_headers, stream=True)
        assert res.status_code == 200, f"Response status: {res.status_code}"

        with open(path, "wb") as f:
            for chunk in res.iter_content(chunk_size=8192): 
                f.write(chunk)



    def get_data_from_http_request(self, path):        
        # grab data from input url
        res = requests.get(self.file_url, headers = self.custom_headers)
        assert res.status_code == 200, f"Response status: {res.status_code}"

        with open(self.path, "wb") as f:
            f.write(res.content)


    def execute(self, context):
        # set task instance from context
        ti = context['ti']

        # create filepath for file well create and init backup path
        self.set_path(ti)
        backup_path = None

        # init old and new file hashes for comparison
        if self.stream:
            print("streaming hash")
            new_hash = misc_utils.stream_download_to_md5(self.file_url)
        elif not self.stream:
            print("bulk downloading into hash")
            new_hash = misc_utils.download_to_md5(self.file_url)

        print("New hash is " + str(new_hash))
        if not os.path.isfile(self.path):
            print("No old file!")
            old_hash = "no_old_file"
        else:
            old_hash = misc_utils.file_to_md5(self.path)
            print("old hash is " + str(old_hash))
        #old_hash = misc_utils.file_to_md5(self.path)
        
        # if new and old files dont match, replace old with new
        if new_hash != old_hash:
            # make a backup if the old file, if it exists
            if self.create_backup and os.path.exists(self.path):
                # write backup and remember backup location
                # we'll want to use or delete it later
                backup_path = self.path + "-backup"
                shutil.copy(self.path, backup_path)
                logging.info("Created a backup at " + backup_path)

            if self.stream:
                print("streaming into file")
                self.stream_response_to_file(self.path)
            else:
                print("downloading into file")
                self.get_data_from_http_request(self.path)
            needs_update = True

        # otherwise, do nothing
        else:
            needs_update = False
        
        return {
                "data_path": self.path,
                "needs_update": needs_update,
                "backup_path": backup_path
                }


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

        create_backup: bool = False,
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

        # create target filepath and init backup path
        path = self.set_path(ti)
        backup_path = None

        if self.create_backup and os.path.exists(path):
            # write backup and remember backup location
            # we'll want to use or delete it later
            backup_path = path + "-backup"
            shutil.copy(path, backup_path)

        # store parsed data in memory
        data = self.get_features()

        # write data into a file
        self.write_to_file(data, path)

        return {
            "data_path": path,
            "backup_path": backup_path
        }


class CleanBackupFilesOperator(BaseOperator):
    """
    If success == True, deletes -backup file
    If success == False, moves -backup file over recently downloaded file
    """

    @apply_defaults
    def __init__(self,
    data_path = None,
    data_path_task_id: str = None,
    data_path_task_key: str = None,
    backup_path = None,
    backup_path_task_id: str = None,
    backup_path_task_key: str = None,
    success = None,
    success_task_id: str = None,
    success_task_key: str = None,
    **kwargs):
        super().__init__(**kwargs)
        self.data_path = data_path
        self.data_path_task_id = data_path_task_id
        self.data_path_task_key = data_path_task_key

        self.backup_path = backup_path
        self.backup_path_task_id = backup_path_task_id
        self.backup_path_task_key = backup_path_task_key

        self.success = success
        self.success_task_id = success_task_id
        self.success_task_key = success_task_key


    def execute(self, context):
        # init task instance from context
        ti = context['ti']

        # get data path and backup path if provided by another task
        if self.data_path_task_id and self.data_path_task_key:
            self.data_path = ti.xcom_pull(task_ids=self.data_path_task_id)[self.data_path_task_key]

        if self.backup_path_task_id and self.backup_path_task_key:
            self.backup_path = ti.xcom_pull(task_ids=self.backup_path_task_id)[self.backup_path_task_key]

        # get success status if provided by another task
        if self.success_task_id and self.success_task_key:
            success_task = ti.xcom_pull(task_ids=self.success_task_id)
            # if we can get the success task, we'll get its success status
            if success_task:
                self.success = success_task[self.success_task_key]
            # if we cant get the success task, we assume it failed
            else:
                self.success = False

        #path = Path(self.file_path) if type(self.file_path) == "str" else self.file_path
            
        # if success is true, remove -backup file
        if self.success == True and self.backup_path != None:
            os.remove(self.backup_path)

        # if success is false, move -backup over most recently downloaded file
        elif self.success == False and self.backup_path != None:
            shutil.move(self.backup_path, self.data_path)


class ValidateFileSchemaOperator(BaseOperator):
    """
    Compares input file's colnames to input list of 
    column names

    Fails if the column names dont match between the
    two inputs
    """

    @apply_defaults
    def __init__(
        self,
        filepath: str = None,
        filepath_task_id: str = None,
        filepath_task_key: str= None,
        correct_columns: list = [],
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.filepath = filepath
        self.filepath_task_id = filepath_task_id
        self.filepath_task_key = filepath_task_key
        self.correct_columns = correct_columns

    def execute(self, context):

        # get task instance context 
        ti = context['ti']

        # read input file colnames
        if self.filepath_task_id and self.filepath_task_key:
            self.filepath = ti.xcom_pull(task_ids=self.filepath_task_id)[self.filepath_task_key]

        with open(self.filepath, "r") as f:
            reader = csv.reader(f)
            file_columns = next(reader, None)

            for correct_col in self.correct_columns:
                for correct_col in self.correct_columns:
                # we do not need to check geometry columns
                # sometimes, we create the geometry columns from the input
                # so, while its present in the config file, it may not
                # be present in the input file
                    if correct_col != "geometry":
                        assert correct_col.strip() in [f.strip() for f in file_columns], correct_col + " is in the config, but not in the data"
