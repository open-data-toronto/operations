""" Makes sure YAML DAGs load
Taken from Airflow Best Practices docs:
https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#unit-tests

"""

import pytest
import os
import yaml
from typing import Dict, List

from airflow.models import DagBag

CONFIG_FOLDER = "/data/operations/dags/datasets/files_to_datastore"


def test_dag_loaded(dagbag):
    """Loop through each YAML and make sure it makes a valid DAG"""
    yamls = os.listdir(CONFIG_FOLDER)
    for yaml in yamls:
        if yaml.endswith(".yaml"):
            dag_id = yaml.replace(".yaml", "")
            dag = dagbag.get_dag(dag_id=dag_id)
            assert dagbag.import_errors == {}
            assert dag is not None
            # At least 18 tasks in a valid dag from DAG_generatory.py
            assert len(dag.tasks) >= 18


def test_dag_properties(dagbag):
    yamls = os.listdir(CONFIG_FOLDER)
    for yaml in yamls:
        if yaml.endswith(".yaml"):
            dag_id = yaml.replace(".yaml", "")
            dag = dagbag.get_dag(dag_id=dag_id)

            assert dag.has_task("get_or_create_package")
            assert dag.has_task("done_inserting_into_datastore")

            assert dag.validate_schedule_and_params() is None


def test_yaml_parameters():
    for file in sorted(os.listdir(CONFIG_FOLDER)):
        if file.endswith(".yaml"):
            with open(CONFIG_FOLDER + "/" + file, "r") as f:
                yaml_obj = yaml.load(f, yaml.SafeLoader)
                package_name = list(yaml_obj.keys())[0]
                config = yaml_obj[package_name]

                # dag info
                schedule = config["schedule"]
                dag_owner_name = config["dag_owner_name"]
                dag_owner_email = config["dag_owner_email"]

                # mandatory package attributes
                title = config["title"]
                date_published = config["date_published"]
                dataset_category = config["dataset_category"]
                refresh_rate = config["refresh_rate"]
                owner_division = config["owner_division"]
                owner_email = config["owner_email"]
                notes = config["notes"]
                excerpt = config["excerpt"]
                resources = config["resources"]

                # optional package attributes
                civic_issues = config.get("civic_issues", None)
                topics = config.get("topics", None)
                tags = config.get("tags", None)
                information_url = config.get("information_url", None)
                limitations = config.get("limitations", None)

                assert schedule is not None and type(schedule) is str
                assert dag_owner_name is not None and type(dag_owner_name) is str
                assert dag_owner_email is not None and type(dag_owner_email) is str
                assert title is not None and type(title) is str
                assert date_published is not None
                assert dataset_category in ["Map", "Table"]
                assert refresh_rate in [
                    "Daily",
                    "Weekly",
                    "Monthly",
                    "Quarterly",
                    "Semi-annually",
                    "Annually",
                    "As available",
                    "Will not be Refreshed",
                    "Real-time",
                ]
                assert owner_division is not None and type(owner_division) is str
                assert owner_email is not None and type(owner_email) is str
                assert notes is not None and type(notes) is str
                assert excerpt is not None and type(excerpt) is str
                assert resources is not None and isinstance(resources, Dict)

                assert civic_issues is None or isinstance(civic_issues, List)
                assert topics is None or isinstance(topics, List)
                assert tags is None or isinstance(tags, List)
                assert information_url is None or type(information_url) is str
                assert limitations is None or type(limitations) is str
