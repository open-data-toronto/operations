'''test cases for reader classes'''

import pytest
from base import Downloader

def test_downloader():

    test_url = "https://httpstat.us/Random/200,201"
    downloader = Downloader(test_url)

    assert downloader.source_url
