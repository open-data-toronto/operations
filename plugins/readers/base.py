'''Base class(es) for extracting data from sources'''

import logging
import requests
from utils import misc_utils

class Downloader():
    '''Base class for airflow extracting data from a source'''

    def __init__(self, source_url: str = None):
        '''Ensure url is valid, returns 2**'''
        self.source_url = misc_utils.validate_url(source_url)
        

    def write_to_csv():
        '''Input stream or list of dicts with the same schema.
        Writes to local csv'''
        pass

    def validate_line():
        '''Input list of dicts, intended schema.
        Raises error if data format issue is found'''
        pass

        


if __name__ == "__main__":
    Downloader("https://httpstat.us/Random/200,201,500-504")
