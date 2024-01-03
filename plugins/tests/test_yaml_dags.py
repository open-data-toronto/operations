''' Makes sure YAML DAGs load
Taken from Airflow Best Practices docs:
https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#unit-tests

'''

import pytest
import os

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag()


def test_dag_loaded(dagbag):
    '''Loop through each YAML and make sure it makes a non-empty DAG'''
    files = os.listdir("/data/operations/dags/datasets/files_to_datastore")
    for file in files:
        if file.endswith(".yaml"):
            this_file = file.replace(".yaml", "")
            dag = dagbag.get_dag(dag_id=this_file)
            assert dagbag.import_errors == {}
            assert dag is not None
            assert len(dag.tasks) > 0