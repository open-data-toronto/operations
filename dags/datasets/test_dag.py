'''
This is a test pipeline created by yanan.zhang@toronto.ca for codebase refactor testing purpose
'''


import os
import yaml
import logging
import pandas as pd
from datetime import datetime
from typing import Dict

from airflow.decorators import dag, task, task_group

from utils_operators.slack_operators import task_failure_slack_alert
from ckan_operators.resource_operator import GetOrCreateResource, EditResourceMetadata
from ckan_operators.insert_into_datastore import stream_to_datastore
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
def test_publishing_pipeline():
    package_id = "template-test-pipeline"
    resource_name = "Daily Shelter Test"
    
    @task
    def get_attributes_from_config() -> Dict:
        CONFIG_FOLDER = os.path.dirname(os.path.realpath(__file__))
        for config_file in os.listdir(CONFIG_FOLDER):
            if config_file.endswith("config.yaml"):
                # read config file
                with open(CONFIG_FOLDER + "/" + config_file, "r") as f:
                    config = yaml.load(f, yaml.SafeLoader)
                    logging.info(config[package_id])
                    return config[package_id]

    @task
    def get_or_create_resource(package_id: str, resource_name: str) -> Dict:
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
    def insert_records_to_datastore(
        resource_id: str,
        file_path: str,
        file_name: str,
        config: Dict,
    ):
        data_path = file_path + "/" + file_name
        return stream_to_datastore(resource_id= resource_id, file_path=data_path, config=config)
        

    @task
    def edit_resource_metadata(
        resource_id: str,
        new_last_modified: datetime = None,
        new_resource_name: str = None,
    ) -> None:
        edited_resource = EditResourceMetadata(resource_id=resource_id)

        edited_resource.edit_resource_metadata(
            new_resource_name=new_resource_name, new_last_modified=new_last_modified
        )

    @task
    def create_tmp_dir(dag_id, dir_variable_name):
        return misc_utils.create_dir_with_dag_id(dag_id, dir_variable_name)

    @task
    def write_to_csv(file_path):
        #data = {"employee1": [39, 23], "employee2": [52, 30], "employee3": [45, 67]}
        current_folder = os.path.dirname(os.path.realpath(__file__))
        logging.info(current_folder)
        
        path = current_folder + "/data.csv"
        df = pd.read_csv(path)
        file = file_path + "/data.csv"
        df.to_csv(file, index=False)
        logging.info(df.head())

        return file

    @task
    def delete_file(dag_tmp_dir, file_name):
        misc_utils.delete_file(dag_tmp_dir, file_name)

    @task
    def delete_tmp_dir(dag_id):
        misc_utils.delete_tmp_dir(dag_id)

    # Main Flow
    # get or create resource
    tmp_dir = create_tmp_dir(dag_id=package_id, dir_variable_name="tmp_dir")
    resource = get_or_create_resource(package_id, resource_name)

    # # edit resource
    # @task_group(group_id="edit_resource_task_group")
    # def tg1():
    #     edit_resource_metadata(
    #         resource_id=resource["id"], new_resource_name="edited new resource name"
    #     )
    #     edit_resource_metadata(
    #         resource_id=resource["id"], new_last_modified="2023-08-10 14:33:07"
    #     )
    
    config = get_attributes_from_config()
    config >> resource >> insert_records_to_datastore(resource_id=resource["id"], file_path=tmp_dir, file_name="data.csv", config=config)
    
    tmp_dir >> write_to_csv(tmp_dir) >> delete_file(tmp_dir, file_name="data.csv") >> delete_tmp_dir(dag_id=package_id)



test_publishing_pipeline()