#!python3
# http_to_file_operator.py - customer airflow operator classes for loading data via http

import logging
import requests

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class HTTPGETToFileOperator(BaseOperator):
    """
    Make HTTP call to input url (with input params, header etc)

    Writes raw, inprocessed output to file
    """

    @apply_defaults
    def __init__(self, url: str, target_file: str, *args, **kwargs):
        self.url = url
        self.target_file = target_file
        super(HTTPGETToFileOperator, self).__init__(*args, **kwargs)

    def return_parsed_response(self, type):
        # http get resource, parse based on type input (JSON, CSV, XML, etc)
        pass

    def write_to_file(self):
        # write data in memory to file
        pass

    def execute(self):
        output = requests.get(self.url).text
        return output


