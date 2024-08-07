import pytest
import requests
import ckanapi
import random

from utils import misc_utils
from ckan_operators.resource_operator import GetOrCreateResource
from ckan_operators.package_operator import GetOrCreatePackage
from ckan_operators.datastore_operator import stream_to_datastore


CKAN = misc_utils.connect_to_ckan()

random_name = "test-name" + str(random.randint(1000,9999))

package_metadata = {
    "title": random_name,
    "date_published": "2023-01-13 00:00:00",
    "refresh_rate": "Monthly",
    "dataset_category": "Document",
    "owner_division": "Information & Technology",
    "owner_section": "test_section",
    "owner_unit": "test_unit",
    "owner_email": "opendata@toronto.ca",
    "civic_issues": "Test_issue",
    "topics": "City government",
    "tags": [
        {"name": "analytics", "vocabulary_id": None},
        {"name": "statistics", "vocabulary_id": None},
        {"name": "open data", "vocabulary_id": None},
        {"name": "usage", "vocabulary_id": None},
    ],
    "information_url": "Test_info_url",
    "excerpt": """This dataset contains web analytics (statistics)
                capturing visitors' usage of datasets published on
                the City of Toronto Open Data Portal.
                """,
    "limitations": "No limits!",
    "notes": """\r\n\r\nThis dataset contains web analytics (statistics)
                capturing visitors' usage of datasets published on
                the City of Toronto 
                [Open Data Portal](https://open.toronto.ca).\r\n""",
}

resource_metadata = {
    "format": "CSV",
    "is_preview": True,
    "url_type": "datastore",
    "extract_job": "airflow_test_case",
    "url": "placeholder"
}

datastore_attributes = [
  {
    "id": "text_col",
    "type": "text",
  },
  {
    "id": "int_col",
    "type": "int",
  },
  {
    "id": "float_col",
    "type": "float",
  },
  {
    "id": "date_col",
    "type": "date",
    "format": "%m/%d/%Y",
  },
]

@pytest.fixture
def dummy_package():
    return CKAN.action.package_create(
        name = random_name,
        owner_org = "city-of-toronto",       
        id = random_name, 
        license_url="https://open.toronto.ca/open-data-license/",
        **package_metadata,
    )

@pytest.fixture
def dummy_resource():
    return CKAN.action.resource_create(
        name=random_name,
        package_id=random_name,
        **resource_metadata,
    )

@pytest.fixture
def cleanup():
    yield
    CKAN.action.dataset_purge(id = random_name)

def test_get(dummy_package, dummy_resource, cleanup):
    package = GetOrCreatePackage(package_name = random_name, package_metadata = package_metadata).get_or_create_package()
    resource = GetOrCreateResource(package_name = random_name, resource_name = random_name, resource_attributes = resource_metadata).get_or_create_resource()

    for key in package_metadata.keys():
        assert package[key] == dummy_package[key]

    for key in resource_metadata.keys():
        assert resource[key] == dummy_resource[key]



def test_create(cleanup):
    package = GetOrCreatePackage(package_name = random_name, package_metadata = package_metadata).get_or_create_package()
    resource = GetOrCreateResource(package_name = random_name, resource_name = random_name, resource_attributes = resource_metadata).get_or_create_resource()

    for key in package_metadata.keys():
        if key not in ["tags"]: # CKAN handles these - we wont check them
            assert package[key] == package_metadata[key]

    for key in resource_metadata.keys():
        if key not in ["url"]: # CKAN handles these - we wont check them
            assert resource[key] == resource_metadata[key]

def test_stream_to_datastore(dummy_package, dummy_resource, cleanup):

    resource_id = dummy_resource["id"]
    package_id = dummy_package["name"]
    file_path = "/data/operations/plugins/ckan_operators/test_csv.csv"

    result = stream_to_datastore(
        resource_id = resource_id,
        file_path = file_path,
        attributes = datastore_attributes,
    )

    assert result["success"]