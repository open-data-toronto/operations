# test_package_operators - test suite for http to package operators

import pytest
import requests
import os
import ckanapi

from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from airflow.models import Variable

from ckan_operators.package_operator import GetOrCreatePackageOperator


# init the directory where the data will be written to
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

package_name="not-a-real-package"

# init the dag for the tests
dag = DAG(dag_id='anydag', start_date=datetime.now())

# init the Operator and Task Instance
task = GetOrCreatePackageOperator(
    task_id="get_or_create_package",
    package_name_or_id=package_name,
    address=CKAN,
    apikey=CKAN_APIKEY,
    dag=dag
)

ti = TaskInstance(task=task, execution_date=datetime.now())

def test_create():
    # run test once to create the package
    # checks all keys are present in operator output
    output = task.execute(ti.get_template_context())
    keys = output.keys()
    assert len(keys), "There are no keys in the returned package!"
    assert "package" in keys

def test_get():
    # run test again to get the package
    output = task.execute(ti.get_template_context())
    keys = output.keys()
    assert len(keys), "There are no keys in the returned package!"
    assert "package" in keys

def test_cleanup():
    # delete the package
    ckanapi.RemoteCKAN(apikey=CKAN_APIKEY, address=CKAN).action.package_delete(id=package_name)