'''test cases for reader classes'''

import pytest
from readers.base import Reader

def test_reader():

    test_url = "https://httpstat.us/Random/200,201"
    reader = Reader(test_url)

    assert reader.source_url == test_url
