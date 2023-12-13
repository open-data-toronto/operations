'''test cases for reader classes'''

import pytest
import csv
import os
import yaml
from readers.base import Reader, CSVReader, AGOLReader, CustomReader, ExcelReader, JSONReader

this_dir = os.path.dirname(os.path.realpath(__file__))

@pytest.fixture
def test_csv_reader():
    '''Inits a CSV reader for testing'''
    test_source_url = "https://opendata.toronto.ca/DummyDatasets/COT_affordable_rental_housing_mod.csv"
    with open(this_dir + "/test_csv_schema.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["upcoming-and-recently-completed-affordable-housing-units"]["resources"]["Affordable Rental Housing Pipeline"]["attributes"]
    test_filename = "test_csv_output.csv"

    return CSVReader(
        source_url = test_source_url,
        schema = test_schema,
        out_dir = this_dir,
        filename = test_filename
    )


@pytest.fixture
def test_agol_reader():
    '''Inits an AGOL reader for testing'''
    test_source_url = "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services/COTGEO_EMS/FeatureServer/0"
    with open(this_dir + "/test_agol_schema.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["ambulance-station-locations"]["resources"]["ambulance-station-locations"]["attributes"]
    test_filename = "test_agol_output.csv"

    return AGOLReader(
        source_url = test_source_url,
        schema = test_schema,
        out_dir = this_dir,
        filename = test_filename
    )


@pytest.fixture
def test_excel_reader():
    '''Inits an excel reader for testing'''
    test_source_url = "https://opendata.toronto.ca/toronto.public.health/deaths-of-people-experiencing-homelessness/Homeless deaths_demographics.xlsx"
    with open(this_dir + "/test_excel_schema.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["deaths-of-people-experiencing-homelessness"]["resources"]["Homeless deaths by demographics"]["attributes"]
    test_sheet = sheet = config["deaths-of-people-experiencing-homelessness"]["resources"]["Homeless deaths by demographics"]["sheet"]
    test_filename = "test_excel_output.csv"

    return ExcelReader(
        source_url = test_source_url,
        schema = test_schema,
        out_dir = this_dir,
        filename = test_filename,
        sheet = sheet
    )


@pytest.fixture
def test_csv_reader_special_chars():
    '''Inits csv reader with a source with special chars for testing'''
    test_source_url = "https://opendata.toronto.ca/DummyDatasets/VW_OPEN_VOTE_2018_2022.csv"
    with open(this_dir + "/test_csv_schema_special_chars.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["members-of-toronto-city-council-voting-record"]["resources"]["member-voting-record-2018-2022"]["attributes"]
    test_filename = "test_csv_output_special_chars.csv"

    return CSVReader(
        source_url = test_source_url,
        schema = test_schema,
        out_dir = this_dir,
        filename = test_filename,
    )


@pytest.fixture
def test_geojson_reader():
    '''Inits csv reader with a source with special chars for testing'''
    test_source_url = "https://opendata.toronto.ca/transportation.services/traffic-calming-database/Traffic Calming Database.geojson"
    with open("/data/operations/plugins/readers/test_geojson_schema.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["traffic-calming-database"]["resources"]["Traffic Calming Database"]["attributes"]
    test_filename = "test_json_output.csv"

    return JSONReader(
        source_url = test_source_url,
        schema = test_schema,
        out_dir = this_dir,
        filename = test_filename,
        is_geojson = True,
    )


@pytest.fixture
def test_json_reader():
    '''Inits csv reader with a source with special chars for testing'''
    test_source_url = "https://opendata.toronto.ca/childrens.services/child-family-programs/earlyOnLocations_prod.json"
    with open("/data/operations/plugins/readers/test_json_schema.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["earlyon-child-and-family-centres"]["resources"]["EarlyON Child and Family Centres Locations - geometry"]["attributes"]
    test_filename = "test_geojson_output.csv"

    return JSONReader(
        source_url = test_source_url,
        schema = test_schema,
        out_dir = this_dir,
        filename = test_filename,
    )


@pytest.fixture
def test_json_reader_jsonpath():
    '''Inits csv reader with a source with special chars for testing'''
    test_source_url = "https://opendata.toronto.ca/DummyDatasets/tpl-events-feed_mod3.json"
    with open("/data/operations/plugins/readers/test_json_schema_jsonpath.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)
    test_schema = config["earlyon-child-and-family-centres"]["resources"]["EarlyON Child and Family Centres Locations - geometry"]["attributes"]
    test_filename = "test_geojson_output_jsonpath.csv"
    test_jsonpath = "$.data.1"

    return JSONReader(
        source_url = test_source_url,
        schema = test_schema,
        out_dir = this_dir,
        filename = test_filename,
        jsonpath = test_jsonpath
    )


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


def test_excel_reader_output(test_excel_reader):
    '''test cases for excelReader write method'''

    test_excel_reader.write_to_csv()
    with open(test_excel_reader.path, "r") as f:
        assert f


def test_csv_reader_special_chars_output(test_csv_reader_special_chars):
    '''test cases for CSVReader write method'''

    test_csv_reader_special_chars.write_to_csv()
    with open(test_csv_reader_special_chars.path, "r") as f:
        assert f


def test_json_reader_output(test_json_reader):
    '''test cases for JSONReader write method with json input'''

    test_json_reader.write_to_csv()
    with open(test_json_reader.path, "r") as f:
        assert f


def test_geojson_reader_output(test_geojson_reader):
    '''test cases for JSONReader write method with geojson input'''

    test_geojson_reader.write_to_csv()
    with open(test_geojson_reader.path, "r") as f:
        assert f


def test_json_reader_output_jsonpath(test_json_reader_jsonpath):
    '''test cases for JSONReader write method with json input that needs parsing'''

    test_json_reader_jsonpath.write_to_csv()
    with open(test_json_reader_jsonpath.path, "r") as f:
        assert f