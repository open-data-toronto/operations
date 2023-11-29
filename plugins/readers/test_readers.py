'''test cases for reader classes'''

import pytest
import csv
import os
import yaml
from readers.base import Reader, CSVReader, AGOLReader

this_dir = os.path.dirname(os.path.realpath(__file__))

@pytest.fixture
def test_csv_vars_latin1():
    test_source_url = "https://opendata.toronto.ca/DummyDatasets/COT_affordable_rental_housing_mod.csv"
    with open(this_dir + "/test_csv_schema.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["upcoming-and-recently-completed-affordable-housing-units"]["resources"]["Affordable Rental Housing Pipeline"]["attributes"]
    test_filename = "test_csv_output.csv"

    return {
        "test_source_url": test_source_url,
        "test_schema": test_schema,
        "this_dir": this_dir,
        "test_filename": test_filename
    }

@pytest.fixture
def test_csv_reader(test_csv_vars_latin1):
    
    test_csv_reader = CSVReader(
        source_url = test_csv_vars_latin1["test_source_url"],
        schema = test_csv_vars_latin1["test_schema"],
        out_dir = test_csv_vars_latin1["this_dir"],
        filename = test_csv_vars_latin1["test_filename"],
    )

    return test_csv_reader

@pytest.fixture
def test_agol_vars():
    test_source_url = "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services/COTGEO_EMS/FeatureServer/0"
    with open(this_dir + "/test_agol_schema.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["ambulance-station-locations"]["resources"]["ambulance-station-locations"]["attributes"]
    test_filename = "test_agol_output.csv"

    return {
        "test_source_url": test_source_url,
        "test_schema": test_schema,
        "this_dir": this_dir,
        "test_filename": test_filename
    }

@pytest.fixture
def test_agol_reader(test_agol_vars):
    
    test_agol_reader = AGOLReader(
        source_url = test_agol_vars["test_source_url"],
        schema = test_agol_vars["test_schema"],
        out_dir = test_agol_vars["this_dir"],
        filename = test_agol_vars["test_filename"],
    )

    return test_agol_reader

def test_csv_reader_vars(test_csv_reader, test_csv_vars_latin1):
    '''test cases for CSVReader class variables'''
    assert test_csv_reader.path == test_csv_vars_latin1["this_dir"] + "/" + test_csv_vars_latin1["test_filename"]

def test_csv_reader_output(test_csv_reader):
    '''test cases for CSVReader write method'''

    test_csv_reader.write_to_csv()
    with open(test_csv_reader.path, "r") as f:
        assert f

def test_agol_reader_output(test_agol_reader):
    '''test cases for agolReader write method'''

    test_agol_reader.write_to_csv()
    with open(test_agol_reader.path, "r") as f:
        assert f