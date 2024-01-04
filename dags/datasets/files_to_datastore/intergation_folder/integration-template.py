"""
This is a dag generator template created by yanan.zhang@toronto.ca for codebase refactor purpose
"""


import os
import yaml
import logging
import pandas as pd
from datetime import datetime

from airflow.decorators import dag, task, task_group

from utils_operators.slack_operators import task_failure_slack_alert
from ckan_operators.resource_operator import GetOrCreateResource, EditResourceMetadata
from ckan_operators.datastore_operator import stream_to_datastore
from readers.base import CSVReader
from utils import misc_utils


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


@dag(
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2023, 11, 21),
    catchup=False,
)
def integration_template():
    package_id = "active-affordable-and-social-housing-units-temp"
    resource_name = "Social and Affordable Housing"

    @task
    def create_tmp_dir(dag_id, dir_variable_name):
        tmp_dir = misc_utils.create_dir_with_dag_id(dag_id, dir_variable_name)
        logging.info(tmp_dir)
        return tmp_dir
    
    @task
    def read_from_csv(source_url, schema, out_dir, filename):
        csv_reader = CSVReader(source_url=source_url, schema=schema, out_dir=out_dir, filename=filename)
        csv_reader.write_to_csv()
        
        if os.listdir(out_dir):
            logging.info(f"Reader output file {filename} Successfully.")
        else:
            raise Exception("Reader failed!")

    
    @task
    def get_or_create_package(package_name, package_metadata):
        package = GetOrCreatePackage(package_name, package_metadata)
        return package.get_or_create_package()
    

    @task
    def get_or_create_resource(package_id, resource_name):
        resource = GetOrCreateResource(
            package_id=package_id,
            resource_name=resource_name,
            resource_attributes=dict(
                format="csv",
                is_preview=True,
                url_type="datastore",
                extract_job=f"Airflow: {package_id}",
                url="placeholder",
            ),
        )

        return resource.get_or_create_resource()
    
    @task
    def delete_tmp_dir(dag_id):
        misc_utils.delete_tmp_dir(dag_id)
    
    @task(multiple_outputs=True)
    def get_attr_from_config():
        CONFIG_FOLDER = os.path.dirname(os.path.realpath(__file__))
        for config_file in os.listdir(CONFIG_FOLDER):
            if config_file.endswith("units-temp.yaml"):
                # read config file
                with open(CONFIG_FOLDER + "/" + config_file, "r") as f:
                    config = yaml.load(f, yaml.SafeLoader)
                    dataset = config[package_id]
                    attributes = dataset["resources"]["Social and Affordable Housing"]
                    logging.info(attributes)
                    return attributes

    # Main Flow
    tmp_dir = create_tmp_dir(dag_id=package_id, dir_variable_name="tmp_dir")

    
    attributes = get_attr_from_config()
    
    get_data = read_from_csv(source_url="https://opendata.toronto.ca/housing.secretariat/Social and Affordable Housing.csv", schema=attributes["attributes"], out_dir=tmp_dir, filename="temp_data.csv")
    
    # package = get_or_create_package(package_name, package_metadata)

    # resource = get_or_create_resource(package_id, resource_name)

    # insert_records = insert_records_to_datastore(
    #     resource_id=resource["id"],
    #     file_path=tmp_dir,
    #     file_name="data.csv",
    #     attributes=attributes,
    # )
    
    
    [tmp_dir, attributes] >> get_data >> delete_tmp_dir(package_id)


integration_template()
