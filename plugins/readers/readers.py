'''Base class(es) for extracting data from sources'''

import logging
import requests
import csv
import json
import sys
from inspect import getmembers, isfunction
import importlib
import types

from utils import misc_utils
from abc import ABC, abstractmethod
from io import StringIO


class Reader(ABC):
    '''Base class for airflow downloading data from an external source

    This class always expects an input YAML schema and always writes to a
    local CSV. It will validate the schema of the source data based on the
    YAML schema provided.
    
    source_url - url of the data to download
    schema - path to yaml containing the download's data dictionary
    out_dir - path to where the file will be written as a csv
    filename - name of output csv file
    
    '''

    def __init__(
            self, 
            source_url: str = None,
            schema: list = None, 
            out_dir: str = "",
            filename: str = None,
            **kwargs,
        ):

        if source_url:
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
        self.fieldnames = [attr["id"] if "id" in attr.keys() else attr["target_name"] for attr in self.schema]

        logging.info(f"Reader prepping to save the following fields: {self.fieldnames}")
        

    @abstractmethod
    def read(self):
        '''Placeholder method to be used by child classes'''
        raise NotImplementedError("Reader's 'read' method must be overridden")


    def write_to_csv(self):
        '''Input stream or list of dicts with the same schema.
        Writes to local csv'''
        csv.field_size_limit(sys.maxsize)

        with open(self.path, "w") as f:
            writer = csv.DictWriter(f, self.fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(self.clean_lines())
            f.close()


    def clean_lines(self):
        '''Input one line of data as a list of dicts.
        Forces values to CKAN-friendly data types.
        Raises error if data format issue is found'''
        output = {}
        for line in self.read():
            for attr in self.schema:

                # consider remapped column names when parsing data
                if "id" not in attr.keys():
                    attr["id"] = attr["target_name"]
            
                cleaner = self.cleaners[attr["type"]]
                value = line[attr["id"]]
                if attr["type"] in ["date", "timestamp"]:
                    assert "format" in attr.keys(), f"{attr['id']} doesn't have a {attr['type']} format"
                    output[attr["id"]] = cleaner(value, attr["format"])
                else:                    
                    output[attr["id"]] = cleaner(value)
            
            yield output


class CSVReader(Reader):
    '''Reads a CSV from a URL and writes it locally
    
    encoding - encoding of the source file
    '''

    def __init__(
            self,
            encoding: str = "latin1",
            **kwargs
        ):        

        super().__init__(**kwargs)
        self.encoding = encoding

    def read(self):
        '''Return generator yielding csv rows as dicts'''
        
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
                out = {}

                # if geometric data, parse into geometry object
                if "geometry" in self.fieldnames \
                    and "geometry" not in source_headers:
                    source_row = misc_utils.parse_geometry_from_row(source_row)
                
                # add source data to out row
                for attr in self.schema:
                    # remap column names if in config file
                    if "source_name" in attr.keys() and "target_name" in attr.keys():                        
                        out[attr["target_name"]] = source_row[attr["source_name"]]
                    elif "id" in attr.keys():
                        out[attr["id"]] = source_row[attr["id"]]

                yield out


class AGOLReader(Reader):
    '''Reads a AGOL from a URL and writes it locally
    
    params - optional input to control query sent to AGOL'''

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

        logging.info(f"Requesting CSV from {self.source_url}...")
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
    

class CustomReader(Reader):
    '''Reads data using external custom python function. 
    
    Expects function to return a generator of dicts
    
    full_module_name - name of module where logic lives
    func_name - name of function to use
    input_args - optional dict of arguments to feed the function
    
    '''

    def __init__(
            self,
            #full_module_name: str,
            #func_name: str,
            #resource_config: dict = {},
            custom_reader: str,
            custom_headers: dict = {},
            **kwargs
        ):

        super().__init__(**kwargs)
        self.full_module_name = "custom_readers"
        self.func_name = custom_reader
        #self.custom_headers = custom_headers


    def read(self):
        '''Return input function'''
        module = importlib.import_module(self.full_module_name)
        if self.func_name not in dir(module):
            raise ValueError("Function name must be valid.")
        else:
            for attribute_name in dir(module):
                attribute = getattr(module, attribute_name)
                if isfunction(attribute) and attribute_name == self.func_name:
                    logging.info(f"Returning logic from {module} - {self.func_name}")                 
                    func = attribute()
                    assert isinstance(func, types.GeneratorType), "Custom func must return generator!"
                    return func


class ExcelReader(Reader):
    '''Reads from remote excel file, writes to local CSV
    Data is read all at once into memory - it may struggle with big datasets
    
    sheet - the sheet in the excel to write to a CSV
    '''
    def __init__(
            self, 
            sheet,           
            **kwargs
        ):
        import openpyxl
        from io import BytesIO
        import gc

        super().__init__(**kwargs)
        self.sheet = sheet

        file = requests.get(self.source_url).content
        wb = openpyxl.load_workbook(filename = BytesIO(file))
        self.worksheet = wb[self.sheet]
        
        del file
        gc.collect()
        

    def read(self):

        source_headers = [col.value for col in self.worksheet[1]]

        for row in self.worksheet.iter_rows(min_row=2):
            output_row = {
                source_headers[i]: str(row[i].value).strip() if row[i].value else None
                for i in range(len(row))
            }

            # if geometric data, parse into geometry object
            if "geometry" in self.fieldnames and "geometry" not in source_headers:
                output_row = misc_utils.parse_geometry_from_row(output_row)

            yield output_row

    
class JSONReader(Reader):
    '''Reads from a remote, flat JSON or GEOJSON and writes to local CSV.
    Data is read all at once into memory; it will struggle with large datasets
    
    jsonpath - if flat contents are not top level json, jsonpath evaluates
              where to start parsing said flat contents
              
              This currently only accepts "dot" notation of jsonpath

              ex: 
                $.1.target_attribute is good
                $[1][target_attribute] is not good

    is_geojson - if true, parses file like geojson. Looks for geometry 
                attribute and adds it as its own column in output CSV
    '''

    def __init__(
            self,
            jsonpath: str = None,
            is_geojson: bool = False,
            **kwargs,
        ):
        super().__init__(**kwargs)

        # If both of these are provided, we only parse the geojson
        # We assume the geojson is not nested deeper than standard geojson
        if jsonpath and is_geojson:
            logging.warning("Input jsonpath ignored; geojson input data expected")

        if jsonpath:
            assert jsonpath.startswith("$"), "JSONPath must start with $"    
        self.jsonpath = jsonpath

        self.is_geojson = is_geojson


    def parse_jsonpath(self, input):
        if self.jsonpath:
            # Currently, this only parses "dot" notation of jsonpath
            path_steps = self.jsonpath.split(".")

            # loop over each step of jsonpath and return target json
            for step in path_steps:
                if step == "$":
                    continue
                if step.isdigit():
                    step = int(step)
                input = input[step]

        for row in input:
            yield row

    def parse_geojson(self, input):
        output = []
         # for each feature, combine it with its geometry into one object and append it to the output
        for feature in input["features"]:
        # create a row object for each object in the json response, and flag its timestamp fields
            row = {**feature["properties"], "geometry": json.dumps(feature["geometry"])}

            yield row


    def read(self):
        res = json.loads(requests.get(self.source_url).text)

        if self.is_geojson:
            return self.parse_geojson(input=res)

        elif not self.is_geojson:
            return self.parse_jsonpath(input=res)


def select_reader(package_name, resource_name, resource_config):
    '''input CKAN resource config, returns correctReader instance'''

    out_dir_basepath = "/data/tmp/"


    readers = {
        "csv": CSVReader,
        "json": JSONReader,
        "geojson": JSONReader,
        "agol": AGOLReader,
        "xls": ExcelReader,
        "xlsx": ExcelReader,
    }

    # make sure config has minimum keys we'll need
    assert isinstance(resource_config, dict), f"Input must be dict! Got {type(resource_config)}"
    
    assert "url" in resource_config.keys(), "Resource config missing URL!"
    assert "format" in resource_config.keys(), "Resource config missing format!"

    # parse attributes from config for Readers
    resource_config["source_url"] = resource_config["url"]
    resource_config["schema"] = resource_config["attributes"]
    resource_config["out_dir"] = out_dir_basepath + package_name
    resource_config["filename"] = resource_name + ".csv"

    # add attribute to differentiate json and geojson for jsonReader
    if resource_config["format"] == "geojson":
        resource_config["is_geojson"] = True

    # TODO: How do we mark custom readers in YAMLs?
    # TODO: How do we mark custom reader inputs in YAMLs?
    if "custom_reader" in resource_config.keys():
        return CustomReader(**resource_config)

    # if file is AGOL, return AGOLReader
    elif resource_config.get("agol", None):
        return AGOLReader(**resource_config) 

    # return reader
    else:
        reader = readers[resource_config["format"]](**resource_config)
        return reader


if __name__ == "__main__":
    print("Testing")
    config = {
                "custom_reader": "bodysafe",
                "format": "geojson",
                "custom_headers": {
                    "SRV-KEY": "bodysafe_secure_toronto_opendata_SRV_KEY",
                    "USER-KEY": "secure_toronto_opendata_USER_KEY"
                },
                "url": "https://secure.toronto.ca/opendata/bs_od/full_list/v1?format=json",
                "attributes": [
                    {
                    "id": "estId",
                    "type": "int",
                    "info": {
                        "notes": "Establishment ID"
                    }
                    },
                    {
                    "id": "estName",
                    "type": "text",
                    "info": {
                        "notes": "Establishment Name"
                    }
                    },
                    {
                    "id": "addrFull",
                    "type": "text",
                    "info": {
                        "notes": "Address of Establishment"
                    }
                    },
                    {
                    "id": "srvType",
                    "type": "text",
                    "info": {
                        "notes": "Type of services offered by Establishment"
                    }
                    },
                    {
                    "id": "insStatus",
                    "type": "text",
                    "info": {
                        "notes": "Status of the inspection (ex: Pass)"
                    }
                    },
                    {
                    "id": "insDate",
                    "type": "date",
                    "info": {
                        "notes": "Date of inspection"
                    }
                    },
                    {
                    "id": "observation",
                    "type": "text",
                    "info": {
                        "notes": "Notes on the observations made during inspection"
                    }
                    },
                    {
                    "id": "infCategory",
                    "type": "text",
                    "info": {
                        "notes": "Category of infraction"
                    }
                    },
                    {
                    "id": "defDesc",
                    "type": "text",
                    "info": {
                        "notes": "Description of infraction"
                    }
                    },
                    {
                    "id": "infType",
                    "type": "text",
                    "info": {
                        "notes": "Type of infraction"
                    }
                    },
                    {
                    "id": "actionDesc",
                    "type": "text",
                    "info": {
                        "notes": "Description of action taken"
                    }
                    },
                    {
                    "id": "OutcomeDate",
                    "type": "date",
                    "info": {
                        "notes": "Date of outcome"
                    }
                    },
                    {
                    "id": "OutcomeDesc",
                    "type": "text",
                    "info": {
                        "notes": "Description of outcome"
                    }
                    },
                    {
                    "id": "fineAmount",
                    "type": "int",
                    "info": {
                        "notes": "Amount of fine given"
                    }
                    },
                    {
                    "id": "geometry",
                    "type": "text",
                    "info": {
                        "notes": ""
                    }
                    }
                ]
                }
    for i in select_reader("bodysafe", "bodysafe", config).read():
        print(i)