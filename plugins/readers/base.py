'''Base class(es) for extracting data from sources'''

import logging
import requests
import csv
import json
import sys

from io import StringIO
from utils import misc_utils
from abc import ABC, abstractmethod



class Reader(ABC):
    '''Base class for airflow extracting data from a source'''

    def __init__(
            self, 
            source_url: str = None,
            schema: list = None,
            out_dir: str = "",
            filename: str = None
        ):
        self.source_url = misc_utils.validate_url(source_url)
        self.schema = schema

        self.cleaners = {
            "text": str,
            "int": misc_utils.clean_int,
            "float": misc_utils.clean_float,
            "timestamp": misc_utils.clean_date_format,
            "date": misc_utils.clean_date_format,
            "json": json.loads
        } 

        # where the data will be saved
        self.path = out_dir + "/" + filename
        # names intended to be written out to saved file
        self.fieldnames = ["_id"]
        self.fieldnames += [attr["id"] for attr in self.schema]
        

    @abstractmethod
    def read(self):
        raise NotImplementedError("Reader's 'read' method must be overridden")


    def write_to_csv(self):
        '''Input stream or list of dicts with the same schema.
        Writes to local csv'''
        csv.field_size_limit(sys.maxsize)

        with open(self.path, "w") as f:
            writer = csv.DictWriter(f, self.fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(self.read())
            f.close()


    def clean_line(self, line):
        '''Input one line of data as a list of dicts.
        Forces values to CKAN-friendly data types.
        Raises error if data format issue is found'''
        output = {}
        for attr in self.schema:
        
            value = line[attr["id"]]
            cleaner = self.cleaners[attr["type"]]

            output[attr["id"]] = cleaner(value)
        
        return output


class CSVReader(Reader):
    '''Reads a CSV from a URL and writes it locally'''

    def __init__(
            self,
            encoding: str = "latin1",
            **kwargs
        ):
        super().__init__(**kwargs)
        self.encoding = encoding

    def read(self):
        '''Return generator yielding csv rows as dicts'''
        self.latitude_attributes = ["lat", "latitude", "y", "y coordinate"]
        self.longitude_attributes = ["long", "longitude", "x", "x coordinate"]
        i = 0 # we'll use this to count rows and add a fake row _id

        # get source file stream
        with requests.get(self.source_url, stream=True) as r:
            # decode bytes to string
            iterator = (line.decode(self.encoding) for line in r.iter_lines())

            buff = StringIO(next(iterator))
            header_reader = csv.reader(buff)
            for line in header_reader:
                source_headers = line
            reader = csv.DictReader(iterator, fieldnames = source_headers)
            
            for source_row in reader:
                i += 1
                out = {"_id": i}

                # if geometric data, parse into geometry object
                if "geometry" in self.fieldnames and "geometry" not in source_headers:
                    for attr in source_row:
                        if attr.lower() in self.latitude_attributes:
                            latitude_attribute = attr

                        if attr.lower() in self.longitude_attributes:
                            longitude_attribute = attr

                    source_row["geometry"] = json.dumps({
                        "type": "Point",
                        "coordinates": [
                            float(source_row[longitude_attribute]),
                            float(source_row[latitude_attribute]),
                        ],
                    })
                # add source data to out row
                for attr in self.schema:
                    out[attr["id"]] = source_row[attr["id"]] 

                yield out



class AGOLReader(Reader):
    '''Reads a AGOL from a URL and writes it locally'''

    def generate_url(
        self,
        params: dict = {
            "where": "1=1",
            "outFields": "*",
            "outSR": 4326,
            "f": "geojson",
        }
    ):
        '''return a valid agol url based on inputs'''

        param_string = "&".join([f"{k}={v}" for k,v in params.items()])
        url = self.source_url + "/query?" + param_string

        return url

    def read(self):
        '''Return generator yielding AGOL rows as dicts'''
        self.generate_url()

        overflow = True
        offset = 0
        params = {
            "where": "1=1",
            "outSR": 4326,
            "f": "geojson",
            "resultType": "standard",
            "resultOffset": offset,
            "outFields": "*",
        }

        while overflow is True:            
            
            # make a request url and request
            url = self.generate_url(params)
            res = requests.get(url)
            assert res.status_code == 200, f"HTTP status: {res.status_code}"

            # parse the response
            geojson = json.loads(res.text)
            # get and yield the properties out of each returned object
            for object in geojson["features"]:
                this_record = object["properties"]
                if object.get("geometry", None):
                    this_record["geometry"] = json.dumps(object["geometry"])
                print(this_record)
                yield(this_record)

            # prepare the next request, if needed
            if "exceededTransferLimit" in geojson:
                overflow = geojson["exceededTransferLimit"] is True

            elif "properties" in geojson:
                overflow = (
                            "properties" in geojson 
                            and "exceededTransferLimit" in geojson["properties"] 
                            and geojson["properties"]["exceededTransferLimit"] is True
                        )

            else:
                overflow = False

            offset = offset + len(geojson["features"])
            params["resultOffset"] = offset

if __name__ == "__main__":
    #d = Reader("https://httpstat.us/Random/200,201,500-504")
    #print(d.source_url)

    

    import os
    import yaml
    this_dir = os.path.dirname(os.path.realpath(__file__))
    test_source_url = "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services/COTGEO_EMS/FeatureServer/0"
    with open(this_dir + "/test_agol_schema.yaml", "r") as f:
            config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["ambulance-station-locations"]["resources"]["ambulance-station-locations"]["attributes"]
#
    #ckan_url = "https://ckanadmin0.intra.prod-toronto.ca/datastore/dump/22c57a77-de52-4206-b6a7-276fb1f7ae17?bom=True"
#

    AGOLReader(
        test_source_url,
        test_schema,
        "/data/tmp",
        "ssha-temp-test.csv"
    ).write_to_csv()

    #c = CSVReader(
    #    test_source_url,
    #    test_schema,
    #    "/data/tmp",
    #    "ssha-temp-test.csv"
    #    )
#
    #c.write_to_csv()
    #
    #print(misc_utils.file_to_md5("/data/tmp/ssha-temp-test.csv"))
    #print(misc_utils.stream_download_to_md5(ckan_url))
