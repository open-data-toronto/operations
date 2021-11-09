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
from ckan_operators.package_operator import GetOrCreatePackageOperator
from ckan_operators.resource_operator import GetOrCreateResourceOperator
from ckan_operators.datastore_operator import BackupDatastoreResourceOperator, DeleteDatastoreResourceOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

# init hardcoded vars for these dags
CONFIG_FILEPATH = "/data/operations/dags/sustainment/files_to_datastore/test_yaml_operators.yaml"

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

TMP_DIR = Path(Variable.get("tmp_dir"))

# custom function to create multiple custom dags - sort of like a template for a DAG
def create_dag(dag_id,
                package_name,
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

        # get or create package
        get_or_create_package = GetOrCreatePackageOperator(
            task_id = "get_or_create_package",
            address = CKAN,
            apikey = CKAN_APIKEY,
            package_name_or_id = package_name
        )

        
        
        dummy2 = DummyOperator(
                task_id='dummy2')

        dummy3 = DummyOperator(
                task_id='dummy3')
                

        # From a List
        resource_names = dataset.keys()
        tasks_list = {}
        for resource_name in resource_names:
            
            # download file
            tasks_list["download_" + resource_name] = DownloadFileOperator(
                task_id="download_" + resource_name,
                file_url=dataset[resource_name]["url"],
                dir=TMP_DIR,
                filename=dataset[resource_name]["url"].split("/")[-1]
            )

            # get or create a resource a file
            # TODO: this will change with Gary's addition to the resource operator
            # it will include an output saying whether the resource is new or not
            tasks_list["get_or_create_resource_" + resource_name] = GetOrCreateResourceOperator(
                task_id="get_or_create_resource_" + resource_name,
                address=CKAN,
                apikey=CKAN_APIKEY,
                package_name_or_id=package_name,
                resource_name=resource_name,
                resource_attributes=dict(
                    format=dataset[resource_name]["format"],
                    is_preview=True,
                    url_type="datastore",
                    extract_job=f"Airflow: files_to_datastore.py",
                    package_id=package_name
                ),
            )

            # branch depending whether or not resource is new
            def is_resource_new(get_or_create_resource_task_id, **kwargs):
                resource = kwargs["ti"].xcom_pull(task_ids=get_or_create_resource_task_id)

                if resource["is_new"]:
                    return "new_" + resource_name

                return "existing_" + resource_name

            tasks_list["new_or_existing_" + resource_name] = BranchPythonOperator(
                task_id="new_or_existing_" + resource_name, 
                python_callable=is_resource_new,
                op_kwargs={"get_or_create_resource_task_id": "get_or_create_resource_" + resource_name}
            )

            tasks_list["new_" + resource_name] = DummyOperator(task_id="new_" + resource_name)
            tasks_list["existing_" + resource_name] = DummyOperator(task_id="existing_" + resource_name)

            tasks_list["backup_resource_" + resource_name] = BackupDatastoreResourceOperator(
                task_id = "backup_resource_" + resource_name,
                address=CKAN,
                apikey=CKAN_APIKEY,
                resource_task_id="get_or_create_resource_" + resource_name,
                dir_task_id="tmp_dir"

            )

            tasks_list["delete_resource_" + resource_name] = DeleteDatastoreResourceOperator(
                task_id="delete_resource_" + resource_name,
                address = CKAN,
                apikey = CKAN_APIKEY,
                resource_id_task_id = "get_or_create_resource_" + resource_name,
                resource_id_task_key = "id"
            )

            # TODO:
            # Here is where we convert the file into a datastore resource
            # the logic behind this will vary on the filetype
            # 
            
            tasks_list["dummy"] = DummyOperator(task_id='Component'+str(resource_name))

            # init a temp directory and get/create the package for the target data
            tmp_dir >> get_or_create_package
            
            # grab the target data, put it in the temp dir, and get or create resource(s) for the data
            get_or_create_package >> tasks_list["download_" + resource_name] >> tasks_list["get_or_create_resource_" + resource_name] >> tasks_list["new_or_existing_" + resource_name] >> [tasks_list["new_" + resource_name], tasks_list["existing_" + resource_name]]
            
            # for each resource, if the resource existed before this run, back it up then delete it
            tasks_list["existing_" + resource_name] >> tasks_list["backup_resource_" + resource_name] >> tasks_list["delete_resource_" + resource_name] >> dummy2 
            
            # if it didnt exist before this run, then dont backup or delete anything
            tasks_list["new_" + resource_name] >> dummy2

            # parse the target data into each resource as a datastore resource
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
                                    package_name,
                                    config[package_name],
                                    schedule,
                                    default_args)