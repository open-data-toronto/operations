# files_to_datastore.py - via airflow, moves files from an accessible URL into the CKAN datastore

from datetime import datetime
import yaml
import logging
from pathlib import Path


from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from utils import airflow_utils

from utils_operators.file_operators import DownloadFileOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator
from utils_operators.directory_operator import CreateLocalDirectoryOperator, DeleteLocalDirectoryOperator



# init hardcoded vars for these dags
CONFIG_FILEPATH = "/data/operations/dags/sustainment/files_to_datastore/test_yaml_operators.yaml"

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

TMP_DIR = Path(Variable.get("tmp_dir"))

# custom function to create multiple custom dags - sort of like a template for a DAG
def create_dag(dag_id,
                dataset,
                schedule,
                default_args):

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule
    ) as dag:

        # define the operators that each DAG needs regardless of how it is configured
        
        # create tmp dir
        tmp_dir = CreateLocalDirectoryOperator(
            task_id = "tmp_dir", 
            path = TMP_DIR / dag_id
        )   
        
        dummy1 = DummyOperator(
                task_id='dummy1')
        
        dummy2 = DummyOperator(
                task_id='dummy2')

        dummy3 = DummyOperator(
                task_id='dummy3')
                

        # From a List
        resource_names = dataset.keys()
        tasks_list = {}
        for index, value in enumerate(resource_names):
            
            # download file
            tasks_list["download_" + value] = DownloadFileOperator(
                task_id="download_" + value,
                file_url=dataset[value]["url"],
                dir=TMP_DIR,
                filename=dataset[value]["url"].split("/")[-1]
            )
            # 
            tasks_list["dummy"] = DummyOperator(task_id='Component'+str(value))

             
            dummy1 >> tasks_list["download_" + value] >> tasks_list["dummy"] >> dummy2 
            dummy2 >> dummy3
    
    return dag

# read config file
with open(CONFIG_FILEPATH, "r") as f:
    config = yaml.load(f, yaml.SafeLoader)

# Generate DAG (s) using the function above as a template parameterized by the config
for package_name in config.keys():
    dag_id = package_name
    schedule = "@once"
    default_args = airflow_utils.get_default_args(
        {
            "owner": "Mackenzie",
            "depends_on_past": False,
            "email": ["mackenzie.nichols4@toronto.ca"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "on_failure_callback": task_failure_slack_alert,
            "retries": 0,
            "start_date": datetime(2021, 10, 30, 0, 0, 0)
        }
    )

    globals()[dag_id] = create_dag(dag_id,
                                    config[package_name],
                                    schedule,
                                    default_args)