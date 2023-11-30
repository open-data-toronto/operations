'''utils.py - misc useful functions'''

import requests
import hashlib
import codecs
import csv
import ckanapi
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


def csv_to_generator(filepath, fieldnames):
    '''input csv filepath and fieldnames, output stream of rows as json'''
    with codecs.open(filepath, "rbU", "latin1") as f:
        reader = csv.DictReader(f, fieldnames=fieldnames)
        next(reader)
        for row in reader:
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

    if input is None:
        return

    if len(input) == 0:
        return

    assert isinstance(
        input, str
    ), "Utils.clean_date_format() accepts strings - it got {}".format(
        type(input)
    )

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

    latitude_attributes = ["lat", "latitude", "y", "y coordinate"]
    longitude_attributes = ["long", "longitude", "x", "x coordinate"]
    
    for attr in source_row:
        if attr.lower() in latitude_attributes:
            latitude_attribute = attr

        if attr.lower() in longitude_attributes:
            longitude_attribute = attr

    source_row["geometry"] = json.dumps({
        "type": "Point",
        "coordinates": [
            float(source_row[longitude_attribute]),
            float(source_row[latitude_attribute]),
        ],
    })

    return source_row