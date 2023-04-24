# agol_operator.py - logic for all AGOL airflow operators

import logging
import os
import csv
import json
import hashlib
from datetime import datetime
from pathlib import Path

import requests
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

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
    "esriFieldTypeDouble": "float",
    "esriFieldTypeSmallInteger": "int",
    "esriFieldTypeSingle": "text",
    "esriFieldTypeGlobalID": "int",
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
    Downloads data from AGOL URL and saves to provided directory using provided filename.
    This will always overwrite an existing file, if it's there.
    This is because we are not interested in versioning AGOL data - only pulling the latest from AGOL.

    Expects the following inputs:
        - request_url - reference to the url where this operator will grab the data
        - dir - directory where the response from the above will be saved
        - filename - name of the file to be made
        each of the three above can be given with an actual value, or with a reference to a task_id and task_key that returns the value

    Returns a dictionary containing:
        - path: path to saved file containing data
        - last_modified: timestamp file was last_modified (from the request)
        - fields_path: path to file containing fields
        - checksum: md5 hash of the download's contents
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
        if ts:
            return  datetime.fromtimestamp(ts/1000).isoformat()  
        else:
            return None

    def get_fields(self, query_url):

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
        
        ckan_fields = []
        for field in res.json()["fields"]:
            if field["name"].lower() not in [*DELETE_FIELDS, *self.delete_col]:
                ckan_fields.append({
                    "id": field["name"],
                    "type": AGOL_CKAN_TYPE_MAP[field["type"]],
                })

        logging.info("Utils parsed the following fields: {}".format(ckan_fields))
            
        return ckan_fields

    def agol_response_to_csv(self, query_url):
        logging.info(f"  getting features from AGOL")

        records = 0
        overflow = True
        offset = 0
        query_string_params = {
            "where": "1=1",
            "outFields": "*",
            "outSR": 4326,
            "f": "geojson",
            "resultType": "standard",
            "resultOffset": offset,
        }
        with open(self.path, "w") as f:
            writer = csv.DictWriter(f, self.fields)
            writer.writeheader()
            while overflow is True:
                # As long as there are more records to get on another request
                data=[]
                
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
                    this_record["geometry"] = object["geometry"]
                    data.append(object)

                records += len(data)

                print(data)

                # now we write this chunk to a csv
                writer.writerows(rows_generator)


                
                # prepare the next request, if needed
                if "exceededTransferLimit" in geojson:
                    overflow = geojson["exceededTransferLimit"] is True

                elif "properties" in geojson:
                    overflow = "properties" in geojson and "exceededTransferLimit" in geojson["properties"] and geojson["properties"]["exceededTransferLimit"] is True

                else:
                    overflow = False

                offset = offset + len(geojson["features"])
                query_string_params["resultOffset"] = offset

            f.close()

        logging.info("Returned {} AGOL records".format(str(data)))
        return

    

    def execute(self, context):
        # init task instance context
        ti = context['ti']

        # create target filepath
        self.set_path(ti)

        # get fields [{"id":field_name, "type": ckan_data_type}...]
        self.fields = self.get_fields(self.request_url)

        # write agol data to local CSV
        self.agol_response_to_csv(self.request_url)


        '''
        # Store data in memory
        print("Getting data from agol")
        res = self.parse_data_from_agol()
        last_modified = res["last_modified"]

        # write data to a file, get checksum
        #data = json.dumps(res["data"])
        print("Putting AGOL Data into a file")
        checksum = self.write_to_file(res["data"], self.path)
        print("AGOL Data saved to a file: " + str(self.path))

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
        '''



    
        
    '''
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
                    if k.lower() in [*DELETE_FIELDS, *self.delete_col]:
                        del row[k]
                    
                    # delete "<null>" string field
                    if isinstance(v, str) and v.lower() == "<null>":
                        del row[k]
                        
                # convert esri time data types to JSON standard (for ckan)
                for dtf in datetime_fields:
                    row[dtf] = self.esri_timestamp_to_datetime(row[dtf])
                
                rows.append(row)

            return rows
        
    def get_features(self, query_url):
        logging.info(f"  getting features from AGOL")

        features = []
        overflow = True
        offset = 0
        query_string_params = {
            "where": "1=1",
            "outFields": "*",
            "outSR": 4326,
            "f": "geojson",
            "resultType": "standard",
            "resultOffset": offset,
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
            #data = [ object["properties"] for object in geojson["features"] ]
            #features.extend( geojson["features"] )



            
            # prepare the next request, if needed
            if "exceededTransferLimit" in geojson:
                overflow = geojson["exceededTransferLimit"] is True

            elif "properties" in geojson:
                overflow = "properties" in geojson and "exceededTransferLimit" in geojson["properties"] and geojson["properties"]["exceededTransferLimit"] is True

            
            else:
                overflow = False

            offset = offset + len(geojson["features"])
            query_string_params["resultOffset"] = offset

        logging.info("Returned {} AGOL records".format(str(len(features)) ))
        return features

    def get_fields(self, query_url):

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
        
        ckan_fields = []
        for field in res.json()["fields"]:
            if field["name"].lower() not in [*DELETE_FIELDS, *self.delete_col]:
                ckan_fields.append({
                    "id": field["name"],
                    "type": AGOL_CKAN_TYPE_MAP[field["type"]],
                })

        logging.info("Utils parsed the following fields: {}".format(ckan_fields))
            
        return ckan_fields

    def parse_data_from_agol(self):
        # calls agol utils to get only the features from a simple GET request to AGOL
        res = self.get_features( self.request_url )
        try:
            unparsed_last_modified = requests.get(self.request_url).headers["last-modified"]
            last_modified = datetime.strptime(unparsed_last_modified[:-13], "%a, %d %b %Y")
        except:
            last_modified = datetime.now() #.strftime("%Y-%m-%dT%H:%M:%S.%f")

        fields = self.get_fields( self.request_url )
        
        logging.info("Received {} AGOL records".format(str(len(res))))
        logging.info("Parsed the following fields: {}".format(fields))

        return { "data": self.parse_properties_from_features(res, fields),
                 "last_modified": last_modified,
                 "fields": fields
                }


    def write_to_file(self, data, filepath):
        # write the data to a file dict by dict to avoid memory errors
        with open(filepath, "w") as f:
            f.write("[")

            for item in data[:-1]:
                json.dump(item, f)
                f.write(",")
            
            json.dump(data[-1], f)
            f.write("]")

        checksum = hashlib.md5()
        #checksum.update(data.encode('utf-8'))

        return checksum


    def execute(self, context):
        # init task instance context
        ti = context['ti']

        # create target filepath
        self.set_path(ti)

        # Store data in memory
        print("Getting data from agol")
        res = self.parse_data_from_agol()
        last_modified = res["last_modified"]

        # write data to a file, get checksum
        #data = json.dumps(res["data"])
        print("Putting AGOL Data into a file")
        checksum = self.write_to_file(res["data"], self.path)
        print("AGOL Data saved to a file: " + str(self.path))

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
    '''