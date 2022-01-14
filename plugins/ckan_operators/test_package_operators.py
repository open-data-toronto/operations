# test_package_operators - test suite for http to package operators

import pytest
import requests
import os
import ckanapi
import json

from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from airflow.models import Variable

from ckan_operators.package_operator import GetOrCreatePackageOperator

# init CKAN vars
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

# init current dir
dir = os.path.dirname(os.path.realpath(__file__))

# init test package data
package_name="not-a-real-package"
with open( dir + "/test_package_operators_metadata.json" ) as f:
    package_metadata = json.load(f)

# init the dag for the tests
dag = DAG(dag_id='anydag', start_date=datetime.now())

# init the Operator and Task Instance
task = GetOrCreatePackageOperator(
    task_id="get_or_create_package",
    package_name_or_id=package_name,
    address=CKAN,
    apikey=CKAN_APIKEY,
    dag=dag,
    package_metadata=package_metadata
)

ti = TaskInstance(task=task, execution_date=datetime.now())

def test_create():
    # run test once to create the package
    # checks all keys are present in operator output
    output = task.execute(ti.get_template_context())
    keys = output.keys()
    assert len(keys), "There are no keys in the returned package!"
    assert output["name"] == package_name, "Created package name is not what was expected"

def test_get():
    # run test again to get the package
    output = task.execute(ti.get_template_context())
    keys = output.keys()
    assert len(keys), "There are no keys in the returned package!"
    assert output["name"] == package_name, "Found package name is not what was expected"

def test_cleanup():
    # delete the package
    ckanapi.RemoteCKAN(apikey=CKAN_APIKEY, address=CKAN).action.dataset_purge(id=package_name)