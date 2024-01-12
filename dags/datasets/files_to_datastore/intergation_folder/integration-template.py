"""
This is a dag generator template created by yanan.zhang@toronto.ca for codebase refactor purpose
"""


import os
import yaml
import logging
from datetime import datetime

from airflow.decorators import dag, task

from ckan_operators.package_operator import GetOrCreatePackage
from ckan_operators.resource_operator import GetOrCreateResource, EditResourceMetadata
from ckan_operators.datastore_operator import stream_to_datastore
from readers.base import CSVReader
from utils import misc_utils
from utils_operators.slack_operators import task_failure_slack_alert


default_args = {
    "owner": "Yanan",
    "email": ["yanan.zhang@toronto.ca"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": task_failure_slack_alert,
    "retry_delay": 5,
    "pool": "ckan_pool",
    "retries": 1,
}

metadata_pool = [
    "title",
    "date_published",
    "refresh_rate",
    "owner_division",
    "dataset_category",
    "owner_unit",
    "owner_section",
    "owner_division",
    "owner_email",
    "civic_issues",
    "topics",
    "tags",
    "information_url",
    "excerpt",
    "limitations",
    "notes",
]


def get_config_from_yaml(package_name):
    CONFIG_FOLDER = os.path.dirname(os.path.realpath(__file__))
    for config_file in os.listdir(CONFIG_FOLDER):
        if config_file.endswith("units-temp.yaml"):
            # read config file
            with open(CONFIG_FOLDER + "/" + config_file, "r") as f:
                config = yaml.load(f, yaml.SafeLoader)
                package = config[package_name]
                # init package metadata attributes, where available
                package_metadata = {}
                for metadata_attribute in metadata_pool:
                    if metadata_attribute in package.keys():
                        package_metadata[metadata_attribute] = package[
                            metadata_attribute
                        ]
                    else:
                        package_metadata[metadata_attribute] = None
                resources = package["resources"]
                logging.info(package_metadata)
                logging.info(resources)
                return {
                    "resources": resources,
                    "package_metadata": package_metadata,
                }


@dag(
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2024, 1, 2),
    catchup=False,
)
def integration_template():
    package_name = "active-affordable-and-social-housing-units-temp"

    ################################################
    # --------------Get config params----------------
    config = get_config_from_yaml(package_name)
    package_metadata = config["package_metadata"]
    resources = config["resources"]
    resource_list = resources.keys()

    #################################################
    # --------------Package level tasks--------------
    @task
    def create_tmp_dir(dag_id, dir_path):
        tmp_dir = misc_utils.create_dir_with_dag_id(dag_id, dir_path)
        return tmp_dir

    @task
    def get_or_create_package(package_name, package_metadata):
        package = GetOrCreatePackage(
            package_name=package_name, package_metadata=package_metadata
        )
        return package.get_or_create_package()

    @task
    def delete_tmp_dir(dag_id):
        misc_utils.delete_tmp_dir(dag_id)

    # ----------------------init tasks
    create_tmp_dir = create_tmp_dir(dag_id=package_name, dir_path="/data/tmp")
    get_or_create_package = get_or_create_package(package_name, package_metadata)
    delete_tmp_dir = delete_tmp_dir(dag_id=package_name)

    ############################################################
    # ---------------Resource level task lists------------------
    task_lists = {}

    for resource_name in resource_list:
        # clean the resource name so the DAG can label its tasks with it
        resource_label = resource_name.replace(" ", "")
        resource = resources[resource_name]
        resource_url = resource["url"]
        attributes = resource["attributes"]

        # download source data
        @task(task_id="download_data_" + resource_label)
        def read_from_csv(source_url, schema, out_dir, filename):
            csv_reader = CSVReader(
                source_url=source_url, schema=schema, out_dir=out_dir, filename=filename
            )
            csv_reader.write_to_csv()

            if os.listdir(out_dir):
                logging.info(f"Reader output file {filename} Successfully.")
            else:
                raise Exception("Reader failed!")

            return out_dir + "/" + filename

        # get or create resource
        @task(task_id="get_or_create_resource_" + resource_label, multiple_outputs=True)
        def get_or_create_resource(package_name, resource_name):
            resource = GetOrCreateResource(
                package_name=package_name,
                resource_name=resource_name,
                resource_attributes=dict(
                    format="csv",
                    is_preview=True,
                    url_type="datastore",
                    extract_job=f"Airflow: {package_name}",
                    url="placeholder",
                ),
            )

            return resource.get_or_create_resource()

        # stream to ckan datastore
        @task(task_id="insert_records_" + resource_label, trigger_rule="all_success")
        def insert_records_to_datastore(
            resource_id,
            file_path,
            attributes,
        ):
            return stream_to_datastore(
                resource_id=resource_id, file_path=file_path, attributes=attributes
            )

        # -----------------Init tasks
        task_lists["download_data_" + resource_label] = read_from_csv(
            source_url=resource_url,
            schema=attributes,
            out_dir=create_tmp_dir,
            filename=resource_name + ".csv",
        )

        task_lists["get_or_create_resource_" + resource_label] = get_or_create_resource(
            package_name=package_name, resource_name=resource_name
        )

        task_lists["insert_records_" + resource_label] = insert_records_to_datastore(
            resource_id=task_lists["get_or_create_resource_" + resource_label]["id"],
            file_path=task_lists["download_data_" + resource_label],
            attributes=attributes,
        )

        # ----Task Flow----
        (
            get_or_create_package
            >> create_tmp_dir
            >> task_lists["download_data_" + resource_label]
            >> task_lists["get_or_create_resource_" + resource_label]
            >> task_lists["insert_records_" + resource_label]
            >> delete_tmp_dir
        )


integration_template()
