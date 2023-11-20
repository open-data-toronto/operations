'''Base class(es) for extracting data from sources'''

import logging
import requests
import csv
import json
import sys

from io import StringIO
from utils import misc_utils


class Reader():
    '''Base class for airflow extracting data from a source'''

    def __init__(
        self, 
        source_url: str = None,
        schema: list = [],
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
        self.fieldnames = [attr["id"] for attr in self.schema]

    def write_to_csv(self, read_method):
        '''Input stream or list of dicts with the same schema.
        Writes to local csv'''
        csv.field_size_limit(sys.maxsize)

        with open(self.path, "w") as f:
            writer = csv.DictWriter(f, self.fieldnames)
            writer.writeheader()
            writer.writerows(read_method)
            f.close()


    def clean_line(self, line):
        '''Input list of dicts, intended schema.
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

    def stream_from_csv(self):
        '''Return generator yielding csv rows as dicts'''
        self.encoding = "latin-1"
        self.latitude_attributes = ["lat", "latitude", "y", "y coordinate"]
        self.longitude_attributes = ["long", "longitude", "x", "x coordinate"]

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

                # if geometric data, parse into geometry object
                if "geometry" in self.fieldnames:
                    for attr in source_row:
                        if attr.lower() in self.latitude_attributes:
                            latitude_attribute = attr

                        if attr.lower() in self.longitude_attributes:
                            longitude_attribute = attr

                    source_row["geometry"] = {
                        "type": "Point",
                        "coordinates": [
                            float(source_row[longitude_attribute]),
                            float(source_row[latitude_attribute]),
                        ],
                    }

                out = {attr["id"]:dict(source_row)[attr["id"]] for attr in self.schema}

                yield out









if __name__ == "__main__":
    #d = Reader("https://httpstat.us/Random/200,201,500-504")
    #print(d.source_url)

    c = CSVReader(
        "https://opendata.toronto.ca/DummyDatasets/COT_affordable_rental_housing_mod.csv",
        [
            {
                "id": "Date",
                "type": "date",
                "info": {
                    "notes": "Operational date for the data, uses the date before midnight to refer to the evening of service. A closeout completed at 4:00 am on the morning of August 1, 2022 would be recorded as operational date July 31, 2022."
                }
            },
            {
                "id": "Unmatched callers",
                "type": "int",
                "info": {
                    "notes": "Total individuals who were not able to be offered a space over the past 24 hours, at time of closeout at 4am."
                }
            },
            {
                "id": "Single call",
                "type": "int",
                "info": {
                    "notes": "Number of unmatched callers who had called Central Intake a single time on this date."
                }
            },
            {
                "id": "Repeat caller",
                "type": "int",
                "info": {
                    "notes": "Number of unmatched callers who had called Central Intake two or more times on this date."
                }
            }
        ],
        "/data/tmp",
        "ssha-temp-test.csv"
        )

    c.write_to_csv(c.stream_from_csv())
    #i = 0
    #for x in c.stream_from_csv():
    #    print("-----------------------")
    #    print(x)
    #    print(c.clean_line(x))
    #    i += 1
    #print(i)
