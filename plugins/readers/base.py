'''Base class(es) for extracting data from sources'''

import logging
import requests
import csv
import json
import sys

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
            # TODO: are these mandatory or optional?
            source_url: str = None,
            schema: list = None, 
            out_dir: str = "",
            filename: str = None
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
        self.fieldnames = [attr["id"] for attr in self.schema]

        logging.info(f"Reader expecting the following fields: {self.fieldnames}")
        

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
    

class CustomReader(Reader):
    '''Reads data using external custom python function. 
    
    Expects function to return a generator of dicts
    
    full_module_name - name of module where logic lives
    func_name - name of function to use
    input_args - optional dict of arguments to feed the function
    
    '''

    def __init__(
            self,
            full_module_name: str,
            func_name: str,
            input_args: dict = {},
            **kwargs
        ):
        from inspect import isfunction
        import importlib
        import types

        super().__init__(**kwargs)
        self.full_module_name = full_module_name
        self.func_name = func_name
        self.input_args = input_args


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
                    func = attribute(**self.input_args)
                    assert isinstance(func, types.GeneratorType), "Custom func must return generator!"
                    return func


class ExcelReader(Reader):
    '''Reads from remote excel file, writes to local CSV
    
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
                source_headers[i]: str(row[i].value).strip()
                for i in range(len(row))
            }

            # if geometric data, parse into geometry object
            if "geometry" in self.fieldnames and "geometry" not in source_headers:
                output_row = utils.parse_geometry_from_row(output_row)

            yield output_row
