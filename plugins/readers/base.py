'''Base class(es) for extracting data from sources'''

import logging
import requests
import csv
import json

from io import StringIO
from utils import misc_utils


class Reader():
    '''Base class for airflow extracting data from a source'''

    def __init__(
        self, 
        source_url: str = None,
        schema: list = []
        ):
        '''Ensure url is valid, returns 2**'''
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

    def write_to_csv():
        '''Input stream or list of dicts with the same schema.
        Writes to local csv'''
        print("Write!")

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

        with requests.get(self.source_url, stream=True) as r:
            iterator = r.iter_lines()
            
            # parse out the headers
            header_string = next(iterator).decode(self.encoding)
            buff = StringIO(header_string)
            header_reader = csv.reader(buff)
            for line in header_reader:
                headers = line

            # connect headers and values in a dict
            for row in iterator:
                if len(row) == 0:
                    continue
                values = row.decode(self.encoding)
                buff = StringIO(values)
                data_reader = csv.reader(buff)
                for line in data_reader:
                    data = line
                out = {headers[i]:data[i] for i in range(len(data))}
                yield out


if __name__ == "__main__":
    #d = Reader("https://httpstat.us/Random/200,201,500-504")
    #print(d.source_url)

    c = CSVReader(
        "https://opendata.toronto.ca/shelter.support.housing.administration/central-intake-calls/central intake - service queue.csv",
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
        ]
        )
    i = 0
    for x in c.stream_from_csv():
        print("-----------------------")
        print(x)
        print(c.clean_line(x))
        i += 1


    print(i)
