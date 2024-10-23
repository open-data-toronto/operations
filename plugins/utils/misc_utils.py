'''utils.py - misc useful functions'''

import logging
import requests
import hashlib
import csv
import ckanapi
import json
from datetime import datetime

def stream_download_to_md5(url):
    '''streams input url into returned md5 hash string'''
    r = requests.get(url, stream=True)

    sig = hashlib.md5()

    # loop over lines in url response
    for line in r.iter_content():
        sig.update(line)

    return sig.hexdigest()


def download_to_md5(url):
    '''downloads input url into returned md5 hash string'''
    r = requests.get(url)

    sig = hashlib.md5(r.text.encode())    

    return sig.hexdigest()    


def file_to_md5(filepath):
    '''streams input string into returned md5 hash string'''
    sig = hashlib.md5()
    with open(filepath, "rb") as f:
        # loop over bytes of file until there are no bytes left
        # reads 8192 bytes at a time - md5 has 128-byte digest blocks
        # and 8192 is 128*64
        # stackoverflow.com/questions/1131220/get-the-md5-hash-of-big-files-in-python
        while True:
            buf = f.read(8192)
            if not buf:
                break
            sig.update(buf)

    return sig.hexdigest()


def csv_to_generator(filepath, fieldnames, encoding="latin1"):
    '''input csv filepath and fieldnames, output stream of rows as json'''
    with open(filepath, mode="r", encoding=encoding) as f:
        reader = csv.DictReader(f, fieldnames=fieldnames)
        next(reader)
        for row in reader:
            # convert '' to None - CKAN prefers None
            for k,v in row.items():
                if v == '':
                    row[k] = None
            yield(row)


def clean_string(input):
    '''converts data to fit in CKAN datastore text column'''
    if input is None:
        return ""
    if not isinstance(input, str):
        return str(input)
    else:
        return str(input).strip()

def clean_int(input):
    '''converts data to fit in CKAN datastore int column'''
    if input:
        return int(input)
    else:
        return None

def clean_float(input):
    '''converts data to fit in CKAN datastore float column'''
    if input:
        return float(input)
    else:
        return None

def clean_date_format(input, input_format=None):
    '''converts data to fit in CKAN datastore date or timestamp column'''
    # loops through the list of formats and tries to return an input
    # string into a datetime of one of those formats

    if input in [None, '']:
        return

    format_dict = {
        "%Y-%m-%dT%H:%M:%S.%f": "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f": "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S": "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S": "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d": "%Y-%m-%d",
        "%d-%b-%Y": "%Y-%m-%d",
        "%d-%b-%y": "%Y-%m-%d",
        "%b-%d-%Y": "%Y-%m-%d",
        "%m-%d-%y": "%Y-%m-%d",
        "%m-%d-%Y": "%Y-%m-%d",
        "%d-%m-%y": "%Y-%m-%d",
        "%d-%m-%Y": "%Y-%m-%d",
        "%Y%m%d%H%M%S": "%Y-%m-%dT%H:%M:%S",
        "%m-%d-%Y %I:%M:%S %p": "%Y-%m-%dT%H:%M:%S",
        "%d%m%Y": "%Y-%m-%d",
        "%d%b%Y": "%Y-%m-%d",
        "%Y-%b-%d": "%Y-%m-%d",
        "%b %d, %Y": "%Y-%m-%d",
        "%Y-%m-%d %H:%M %p": "%Y-%m-%dT%H:%M:%S",
        '%m-%d-%Y %I:%M:%S %p': "%Y-%m-%dT%H:%M:%S",
    }

    if input_format:
        if input_format == "epoch":
            # assumes input is in milliseconds

            datetime_object = datetime.fromtimestamp(int(input)/1e3)
            try:
                return datetime_object.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                return datetime_object.strftime("%Y-%m-%d")

        else:
            assert isinstance(
                input, str
            ), "Utils.clean_date_format() accepts strings - it got {}".format(
                type(input)
          
            )
            input = input.replace("/", "-")
            input_format = input_format.replace("/", "-")
            datetime_object = datetime.strptime(input, input_format)
            output = datetime_object.strftime(format_dict[input_format])

        return output

    else:
        for format in format_dict.keys():
            try:
                input = input.replace("/", "-")
                datetime_object = datetime.strptime(input, format)
                output = datetime_object.strftime(format_dict[format])
                return output
            except ValueError:
                pass


def validate_url(source_url):
    try:
        req = requests.get(source_url)
        assert str(req.status_code).startswith("2"), \
            "Input URL {} returned {}".format(source_url, req.status_code)
        return source_url

    except ConnectionError as e:
        logging.error("Error when connecting to {}".format(source_url))
        raise e
    
    except Exception as e:
        raise e


def parse_geometry_from_row(source_row):
    '''input row as dict, output same row with geometry object
    made of that rows geometric contents'''

    latitude_attributes = ["lat", "latitude", "y", "y coordinate", "point_y"]
    longitude_attributes = ["long", "longitude", "x", "x coordinate", "point_x"]
    
    for attr in source_row:
        if attr.lower() in latitude_attributes:
            latitude_attribute = attr

        if attr.lower() in longitude_attributes:
            longitude_attribute = attr

    # manage empty coordinates
    if source_row[longitude_attribute] and source_row[latitude_attribute]:
        coordinates = [
                float(source_row[longitude_attribute]),
                float(source_row[latitude_attribute]),
            ]
    else:
        coordinates = [None, None]

    source_row["geometry"] = json.dumps({
        "type": "Point",
        "coordinates": coordinates,
    })

    return source_row


import os
import shutil
from pathlib import Path

from airflow.models import Variable


def create_dir_with_dag_id(dag_id: str, dir_path: str) -> str:
    """
    Create a directory with dag name

    Parameters:
    - dag_id : str
        the id of the dag(pipeline)
    - dir_path : str
        the path of directory

    Returns:
        full directory path string: str

    """

    dir_with_dag_name = Path(dir_path) / dag_id
    dir_with_dag_name.mkdir(parents=False, exist_ok=True)

    return str(dir_with_dag_name)


def delete_tmp_dir(
    dag_id: str, dir_path: str = "/data/tmp", delete_recursively: bool = True
) -> None:
    """
    Delete tmp directory with dag id

    Parameters:
    - dag_id : str
        the id of the dag(pipeline)
    - dir_path: str, Optional
        the path of directory
    - delete_recursively : bool, Optional
        flag if delete all files and subdirectories

    Returns:
        None

    """

    dag_tmp_dir = Path(dir_path) / dag_id

    # list all files under current directory before delete directory.
    logging.info(f"Deleted file: {os.listdir(dag_tmp_dir)}")

    if not delete_recursively:
        os.rmdir(dag_tmp_dir)
    else:
        shutil.rmtree(dag_tmp_dir)


def delete_file(dag_tmp_dir: str, file_name: str) -> None:
    """
    Delete file under current directory
    Parameters:
    - dag_tmp_dir : str
        the directory path of the file to be deleted
    - file_name : str
        the name of the file to be deleted

    Returns:
        None

    """

    file_path = Path(dag_tmp_dir) / file_name
    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"{file_name} deleted.")
    else:
        logging.info("The file does not exist.")


def file_equal(existing_file_path: str, new_file_path: str):
    """
    Compare if new file and existing file are same
    Parameters:
    - existing_file_path : str
        the path of current file
    - new_file_path : str
        the path of upcoming new file

    Returns:
        Bool

    """
    curr_hash = file_to_md5(existing_file_path)
    new_hash = file_to_md5(new_file_path)

    return curr_hash == new_hash

def connect_to_ckan():
    '''Returns RemoteCKAN object for appropriate CKAN instance'''
    
    # init hardcoded vars
    ACTIVE_ENV = Variable.get("active_env")
    CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
    CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]#
    CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]#

    return ckanapi.RemoteCKAN(apikey=CKAN_APIKEY, address=CKAN)

def parse_source_and_target_names(attributes):
    '''
    Parse source and target names from a list of dicts from a YAML config
    resource attributes section, return a list of working attributes.

    Parameters:
    - attributes : List
        the attributes section of yaml config (data fields)
    
    Returns:
        working_attributes : List
    '''
    working_attributes = []
    for attr in attributes:
        if "id" in attr.keys():
            working_attributes.append(attr)
        elif "source_name" in attr.keys():
            attr["id"] = attr["target_name"]
            working_attributes.append(attr)
    
    return working_attributes