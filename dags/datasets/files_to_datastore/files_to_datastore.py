# files_to_datastore.py - via airflow, moves files from an accessible URL into the CKAN datastore

from datetime import datetime
import yaml
import logging
import os
from pathlib import Path


from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from utils import airflow_utils

from utils_operators.file_operators import DownloadFileOperator, DownloadZipOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator
from utils_operators.directory_operator import CreateLocalDirectoryOperator, DeleteLocalDirectoryOperator
from ckan_operators.package_operator import GetOrCreatePackageOperator
from ckan_operators.resource_operator import GetOrCreateResourceOperator
from ckan_operators.datastore_operator import BackupDatastoreResourceOperator, DeleteDatastoreResourceOperator, InsertDatastoreFromYAMLConfigOperator, RestoreDatastoreResourceBackupOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from utils_operators.agol_operators import AGOLDownloadFileOperator


# init hardcoded vars for these dags
CONFIG_FOLDER = os.path.dirname(os.path.realpath(__file__))

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = "https://ckanadmin1.intra.dev-toronto.ca/"#CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = "7b37ca63-ee3a-4553-8822-57d0f7e14e33"#CKAN_CREDS[ACTIVE_ENV]["apikey"]

TMP_DIR = Variable.get("tmp_dir")

# branch logic - depends whether or not input resource is new
def is_resource_new(get_or_create_resource_task_id, resource_name, **kwargs):
    resource = kwargs["ti"].xcom_pull(task_ids=get_or_create_resource_task_id)

    if resource["is_new"]:
        return "new_" + resource_name

    return "existing_" + resource_name

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

        # init list of resource names
        resource_names = dataset["resources"].keys()

        # init package metadata attributes, where available
        package_metadata = {}
        for metadata_attribute in [
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
            ]:
            if metadata_attribute in dataset.keys():
                package_metadata[metadata_attribute] = dataset[metadata_attribute]
            else:
                package_metadata[metadata_attribute] = None


        # define the operators that each DAG needs regardless of how it is configured
        
        # create tmp dir
        tmp_dir = CreateLocalDirectoryOperator(
            task_id = "tmp_dir", 
            path = TMP_DIR + "/" + dag_id
        )   

        # get or create package
        get_or_create_package = GetOrCreatePackageOperator(
            task_id = "get_or_create_package",
            address = CKAN,
            apikey = CKAN_APIKEY,
            package_name_or_id = package_name,
            package_metadata = package_metadata,
            #notes = package_metadata["notes"],
            #limitations = package_metadata["limitations"],
            #refresh_rate = package_metadata["refresh_rate"],
            #dataset_category = package_metadata["dataset_category"],
            #owner_division = package_metadata["owner_division"],
        )
        

        success_message_slack = GenericSlackOperator(
            task_id = "success_message_slack",
            message_header = "Files to Datastore " + package_name,
            message_content = "\n\t\t   ".join( [name + " | `" + dataset["resources"][name]["format"] +"`" for name in resource_names] ),
            message_body = ""
        )
                

        # From a List
        
        tasks_list = {}
        for resource_label in resource_names:
            # clean the resource label so the DAG can label its tasks with it
            resource_name = resource_label.replace(" ", "")
            resource = dataset["resources"][resource_label]

            
            # download file
            # ZIP files:
            if "zip" in resource.keys():
                if resource["zip"]:
                    tasks_list["download_" + resource_name] = DownloadZipOperator(
                        task_id="download_" + resource_name,
                        file_url=resource["url"],
                        dir=TMP_DIR,
                    )

            # CSV, XLSX files:
            elif resource["format"] in ["csv", "xlsx"]:
                tasks_list["download_" + resource_name] = DownloadFileOperator(
                    task_id="download_" + resource_name,
                    file_url=resource["url"],
                    dir=TMP_DIR,
                    filename=resource["url"].split("/")[-1]
                )

            # AGOL files:
            elif "agol" in resource.keys():
                if resource["agol"] and resource["format"] in ["geojson", "json"]:
                    # remove geometry attribute if file is not geojson
                    delete_col = ["geometry"] if resource["format"] == "json" else []
                    tasks_list["download_" + resource_name] = AGOLDownloadFileOperator(
                        task_id="download_" + resource_name,
                        request_url=resource["url"],
                        dir=TMP_DIR,
                        filename=resource_name + "." + resource["format"],
                        delete_col=delete_col
                    )

            # get or create a resource a file
            tasks_list["get_or_create_resource_" + resource_name] = GetOrCreateResourceOperator(
                task_id="get_or_create_resource_" + resource_name,
                address=CKAN,
                apikey=CKAN_APIKEY,
                package_name_or_id=package_name,
                resource_name=resource_label,
                resource_attributes=dict(
                    format=resource["format"],
                    is_preview=True,
                    url_type="datastore",
                    extract_job=f"Airflow: files_to_datastore.py",
                    package_id=package_name
                ),
            )

            
            # determine whether the resource is new or not
            tasks_list["new_or_existing_" + resource_name] = BranchPythonOperator(
                task_id="new_or_existing_" + resource_name, 
                python_callable=is_resource_new,
                op_kwargs={"get_or_create_resource_task_id": "get_or_create_resource_" + resource_name, "resource_name": resource_name }
            )

            tasks_list["new_" + resource_name] = DummyOperator(task_id="new_" + resource_name)
            tasks_list["existing_" + resource_name] = DummyOperator(task_id="existing_" + resource_name)

            # backup an existing resource
            tasks_list["backup_resource_" + resource_name] = BackupDatastoreResourceOperator(
                task_id = "backup_resource_" + resource_name,
                address=CKAN,
                apikey=CKAN_APIKEY,
                resource_task_id="get_or_create_resource_" + resource_name,
                dir_task_id="tmp_dir"
            )

            tasks_list["restore_backup_" + resource_name] = RestoreDatastoreResourceBackupOperator(
                task_id="restore_backup_" + resource_name,
                address=CKAN,
                apikey=CKAN_APIKEY,
                backup_task_id="backup_resource_" + resource_name,
                trigger_rule="one_failed",
            )

            # delete existing resource records
            tasks_list["delete_resource_" + resource_name] = DeleteDatastoreResourceOperator(
                task_id="delete_resource_" + resource_name,
                address = CKAN,
                apikey = CKAN_APIKEY,
                resource_id_task_id = "get_or_create_resource_" + resource_name,
                resource_id_task_key = "id"
            )

            # intelligently insert new records into an emptied resource based on input yaml config
            tasks_list["insert_records_" + resource_name] = InsertDatastoreFromYAMLConfigOperator(
                task_id="insert_records_" + resource_name,
                address = CKAN,
                apikey = CKAN_APIKEY,
                resource_id_task_id = "get_or_create_resource_" + resource_name,
                resource_id_task_key = "id",
                data_path_task_id = "download_" + resource_name,
                data_path_task_key = "data_path",
                config = resource,
                #fields = dataset["resources"][resource_name]["attributes"],
                #format = dataset["resources"][resource_name]["format"],
                trigger_rule = "one_success",
            )
            


            # init a temp directory and get/create the package for the target data
            tmp_dir >> get_or_create_package
            
            # grab the target data, put it in the temp dir, and get or create resource(s) for the data
            get_or_create_package >> tasks_list["download_" + resource_name] >> tasks_list["get_or_create_resource_" + resource_name] >> tasks_list["new_or_existing_" + resource_name] >> [tasks_list["new_" + resource_name], tasks_list["existing_" + resource_name]]
            
            # for each resource, if the resource existed before this run, back it up then delete it
            tasks_list["existing_" + resource_name] >> tasks_list["backup_resource_" + resource_name] >> tasks_list["delete_resource_" + resource_name] >> tasks_list["insert_records_" + resource_name] >> success_message_slack 
            
            # if it didnt exist before this run, then dont backup or delete anything
            tasks_list["new_" + resource_name] >> tasks_list["insert_records_" + resource_name] >> success_message_slack

            # if something happens while a resource is being deleted or added
            [ tasks_list["delete_resource_" + resource_name], tasks_list["insert_records_" + resource_name] ] >> tasks_list["restore_backup_" + resource_name]
    
    return dag

# Generate DAGs using the function above as a template parameterized by the configs - one DAG per YAML file
for config_file in os.listdir(CONFIG_FOLDER):
    if config_file.endswith(".yaml"):

        # read config file
        with open(CONFIG_FOLDER + "/" + config_file, "r") as f:
            config = yaml.load(f, yaml.SafeLoader)
            package_name = list(config.keys())[0]


        dag_id = config_file.split(".yaml")[0]
        schedule = config[package_name]["schedule"]
        dag_owner_name = config[package_name]["dag_owner_name"]
        dag_owner_email = config[package_name]["dag_owner_email"]

        default_args = airflow_utils.get_default_args(
            {
                "owner": dag_owner_name,
                "depends_on_past": False,
                "email": [dag_owner_email],
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