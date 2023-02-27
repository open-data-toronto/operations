"""
    This script can be used to grab ckan package metadata from existing packages;
    Remember assign following parameters to generate yamls.

        - dag_owner_name,
        - dag_owner_email,
        - package_names, e.g. {"package_names": ["traffic-cameras"]}

    Please assign package_names using Configuration JSON before triggering DAG.
"""
import requests
import logging
import yaml
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Variable

from utils import airflow_utils
from utils_operators.slack_operators import (
    GenericSlackOperator,
    task_failure_slack_alert,
    task_success_slack_alert
)
from ckan_operators.package_operator import GetCkanPackageListOperator
from airflow.operators.python import PythonOperator

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

DIR_PATH = Path(os.path.dirname(os.path.realpath(__file__))).parent
YAML_DIR_PATH = DIR_PATH / "datasets" / "files_to_datastore"

YAML_METADATA = {  # DAG info
                    "schedule": "@once",
                    "dag_owner_name": "",  # dag owner name
                    "dag_owner_email": "",  # dag owner email
                }

PACKAGE_METADATA = [ # mandatory package attributes
                    "title",
                    "date_published",
                    "refresh_rate",
                    "dataset_category",
                    # optional package attributes
                    "owner_division",
                    "owner_section",
                    "owner_unit",
                    "owner_email",
                    "civic_issues",
                    "topics",
                    "tags",
                    "information_url",
                    "excerpt",
                    "limitations",
                    "notes",
                ]

# init DAG
default_args = airflow_utils.get_default_args(
    {
        "owner": "Yanan",
        "depends_on_past": False,
        "email": "yanan.zhang@toronto.ca",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": 3,
        "on_failure_callback": task_failure_slack_alert,
        "start_date": datetime(2023, 2, 17, 0, 0, 0),
        "tags": ["sustainment"],
    }
)

with DAG(
    "metadata_generator",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:
    
    def get_input_package_list(**kwargs):
        # get package names from airflow dag configuration
        if "package_names" in kwargs["dag_run"].conf.keys():
            package_names = kwargs["dag_run"].conf["package_names"]

            assert isinstance(package_names, list), (
                "Input 'package_names' object needs to be a list of "
                + "package names existing on portal."
            )

        return {"package_names": package_names}
    
    def validate_input_packages(**kwargs):
        # make sure input package name exists
        ti = kwargs.pop("ti")
        input_package_list = ti.xcom_pull(task_ids="get_input_packages")["package_names"]
        package_pool = ti.xcom_pull(task_ids="ckan_package_list")

        for package in input_package_list:
            if package not in package_pool:
                message = f"Package {package} is not valid. Please double check."
                raise Exception(message)

    # write to yaml file from dictionary
    def write_to_yaml(filename, metadata):
        """Receives a json input and writes it to a YAML file"""

        logging.info(f"Generating yaml file: {filename}")
        try: 
            with open(YAML_DIR_PATH / filename, "w") as file:
                yaml.dump(metadata, file, sort_keys=False)
        except PermissionError:
            message = "Note: yaml file already exist, please double check!"
            raise Exception(message)
        return {"filename": filename}

    # grab metadata content
    def metadata_generator(ckan_url, yaml_metadata, package_metadata):
        response = requests.get(ckan_url).json()
        ckan_metadata_content = response["result"]

        ckan_metadata_fields = ckan_metadata_content.keys()

        for field in package_metadata:
            if field in ckan_metadata_fields:
                if field == "tags":
                    tags_content = ckan_metadata_content["tags"]
                    tags_yaml = []
                    for i in range(len(tags_content)):
                        tag = {}
                        tag["name"] = tags_content[i]["display_name"]
                        tag["vocabulary_id"] = None
                        tags_yaml.append(tag)
                    yaml_metadata[field] = tags_yaml
                else:
                    yaml_metadata[field] = ckan_metadata_content[field]
            else:
                yaml_metadata[field] = None
        metadata = {ckan_metadata_content["name"]: yaml_metadata}
        return metadata

    def generate_yamls_for_all(**kwargs):
        input_package_list = kwargs.pop("ti").xcom_pull(task_ids="get_input_packages")[
            "package_names"
        ]

        logging.info(f"Input Package Names: {input_package_list}")
        output_list = {}

        for package_name in input_package_list:
            ckan_url = CKAN + "api/3/action/package_show?id=" + package_name
            # generate metadata for each package
            metadata = metadata_generator(ckan_url, YAML_METADATA, PACKAGE_METADATA)
            
            filename = package_name + ".yaml"
            write_to_yaml(filename, metadata)
            output_list[package_name] = ":done_green:" + filename

        return {"generated-yaml-list": output_list}

    get_input_packages = PythonOperator(
        task_id="get_input_packages",
        python_callable=get_input_package_list,
        provide_context=True,
    )

    ckan_package_list = GetCkanPackageListOperator(
        task_id="ckan_package_list",
        address=CKAN,
        apikey=CKAN_APIKEY,
    )

    validate_input_packages = PythonOperator(
        task_id="validate_input_packages",
        python_callable=validate_input_packages,
        provide_context=True,
    )

    generate_yamls_for_all = PythonOperator(
        task_id="generate_yamls_for_all",
        python_callable=generate_yamls_for_all,
        provide_context=True,
    )

    slack_notificaiton = GenericSlackOperator(
        task_id="slack_notificaiton",
        message_header=(" Task Succeeded, Succefully Generated :yaml: !"),
        message_content_task_id="generate_yamls_for_all",
        message_content_task_key="generated-yaml-list",
        message_body="",
    )

    (
        [get_input_packages, ckan_package_list] >>
        validate_input_packages >>
        generate_yamls_for_all >>
        slack_notificaiton
    )
