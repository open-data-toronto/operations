# files_to_datastore.py - via airflow, moves files from an accessible URL into the CKAN datastore

from datetime import datetime, timedelta
import yaml
import logging
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from utils import airflow_utils

from utils_operators.file_operators import DownloadFileOperator, DownloadGeoJsonOperator, CleanBackupFilesOperator, ValidateFileSchemaOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator
from utils_operators.directory_operator import CreateLocalDirectoryOperator, DeleteLocalDirectoryOperator
from ckan_operators.package_operator import GetOrCreatePackageOperator
from ckan_operators.resource_operator import GetOrCreateResourceOperator, EditResourceMetadataOperator
from ckan_operators.datastore_operator import DeleteDatastoreResourceOperator, InsertDatastoreFromYAMLConfigOperator, RestoreDatastoreResourceBackupOperator, DeltaCheckOperator, CSVStreamToDatastoreYAMLOperator, DatastoreCacheOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from utils_operators.agol_operators import AGOLDownloadFileOperator


# branch logic - depends whether or not input resource is new
def is_resource_new(get_or_create_resource_task_id, resource_name, **kwargs):
    resource = kwargs["ti"].xcom_pull(task_ids=get_or_create_resource_task_id)

    if resource["is_new"]:
        return "new_" + resource_name

    return "existing_" + resource_name

# branch logic - depends on whether the existing resource needs updating
def does_resource_need_update(delta_check_task_id, resource_name, **kwargs):
    resource = kwargs["ti"].xcom_pull(task_ids=delta_check_task_id)
    if "force" in kwargs['dag_run'].conf.keys():
        if kwargs['dag_run'].conf["force"] == True:
            return "update_resource_" + resource_name
        elif kwargs['dag_run'].conf["force"] == False:
            return "dont_update_resource_" + resource_name

    if resource["needs_update"]:
        return "update_resource_" + resource_name

    return "dont_update_resource_" + resource_name

# create slack message logic
def build_message_fcn(config, **kwargs):
    # init counter used to prevent empty messages
    counter = 0
    package_name = list(config.keys())[0]
    message = "*Package*: " + package_name + "\n\t\t   *Resources*:\n"
    for resource_name in config[package_name]["resources"]:        
        try:
            resource_format = config[package_name]["resources"][resource_name]["format"]
            resource_records = kwargs["ti"].xcom_pull(task_ids="insert_records_" + resource_name.replace(" ", ""))["record_count"]
            
            message += "\n\t\t   " + "*{}* `{}`: {} records".format(resource_name, resource_format, resource_records)
            counter += 1
        except Exception as e:
            logging.error(e)
            continue

    if counter > 0:
        return {"message": message}
    else:
        return None


# custom function to create multiple custom dags - sort of like a template for a DAG
def create_dag(dag_id,
                package_name,
                dataset,
                schedule,
                default_args):

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup = False # By default Airflow tries to complete all "missed" DAGs since start_date, should be turned off
    ) as dag:

        # init tmp_dir where airflow stashes downloads
        # these downloads are use to check deltas and so data
        # going into CKAN is stored on disk, not all in memory
        TMP_DIR = "/data/tmp/" + dag_id

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

        # write some DAG-level documentation to be visible on the Airflow UI
        dag.doc_md = """
        ### Summary
        This DAG loads the following resource(s) into CKAN:\n 

        {resources}

        ### Contact
        - Dataset-related inquiries should go to {dataset_owner_contact}
        - ETL-related inquiries should go to {dag_owner_contact}

        ### Runtime UI Inputs
        - "force": if set to true (no quotes), will force an update of the data regardless of delta-detection result
        """.format(
            resources = "\n".join(["- `" + resource_name + "` from [" + dataset["resources"][resource_name]["url"] + "](" + dataset["resources"][resource_name]["url"] + ")" for resource_name in resource_names]),
            dataset_owner_contact = "[{name}](mailto:{email})".format(name=package_metadata["owner_division"], email=package_metadata["owner_email"]),
            dag_owner_contact = "[{name}](mailto:{email})".format(name=default_args["owner"], email=default_args["email"]),
        )

        # define the operators that each DAG always needs, regardless of input configuration
        
        # create tmp dir
        tmp_dir = CreateLocalDirectoryOperator(
            task_id = "tmp_dir", 
            path = TMP_DIR
        )   

        # get or create package
        get_or_create_package = GetOrCreatePackageOperator(
            task_id = "get_or_create_package",            
            package_name_or_id = package_name,
            package_metadata = package_metadata,
            pool = "ckan_pool",
        )
        # build message to send to slack
        build_message = PythonOperator(
            task_id="build_message", 
            python_callable=build_message_fcn,
            op_kwargs={"config": config }
        )

        # success message to slack
        success_message_slack = GenericSlackOperator(
            task_id = "success_message_slack",
            message_header = "Files to Datastore",
            message_content_task_id = "build_message",
            message_content_task_key = "message",
            message_body = ""
        )

        done_inserting_into_datastore = DummyOperator(task_id = "done_inserting_into_datastore", trigger_rule="none_failed")
                
        # From a List
        tasks_list = {}
        for resource_label in resource_names:
            # clean the resource label so the DAG can label its tasks with it
            resource_name = resource_label.replace(" ", "")
            resource = dataset["resources"][resource_label]

            
            # download file
            
            # CSV, XLSX files:
            if resource["format"].lower() in ["csv", "xlsx"]:
                stream = True
                if resource["url"].startswith("https://secure.toronto.ca/opendata"):
                    stream = False
                tasks_list["download_" + resource_name] = DownloadFileOperator(
                    task_id="download_" + resource_name,
                    file_url=resource["url"],
                    dir=TMP_DIR,
                    filename=resource["url"].split("/")[-1],
                    custom_headers=resource.get("custom_headers", {}),
                    create_backup=True,
                    stream=stream,
                )

            # AGOL files:
            elif "agol" in resource.keys():
                if resource["agol"] and resource["format"].lower() in ["geojson", "json"]:
                    # remove geometry attribute if file is not geojson
                    delete_col = ["geometry"] if resource["format"] == "json" else []
                    tasks_list["download_" + resource_name] = AGOLDownloadFileOperator(
                        task_id="download_" + resource_name,
                        request_url=resource["url"],
                        dir=TMP_DIR,
                        filename=resource_name + "." + resource["format"],
                        delete_col=delete_col,
                        create_backup=True,
                    )

            # Non AGOL flat JSON files:
            elif not resource.get("agol", False):
                if resource["format"] == "json" or (resource["format"] == "geojson" and resource.get("nested", False)):
                    tasks_list["download_" + resource_name] = DownloadFileOperator(
                        task_id="download_" + resource_name,
                        file_url=resource["url"],
                        dir=TMP_DIR,
                        filename=resource["url"].split("/")[-1],
                        custom_headers=resource.get("custom_headers", {}),
                        create_backup=True,
                    )
            
                # Non AGOL flat GEOJSON files:
                elif resource["format"] == "geojson":
                    tasks_list["download_" + resource_name] = DownloadGeoJsonOperator(
                        task_id="download_" + resource_name,
                        file_url=resource["url"],
                        dir=TMP_DIR,
                        filename=resource["url"].split("/")[-1],
                        create_backup=True,
                    )

            # get or create a resource a file
            tasks_list["get_or_create_resource_" + resource_name] = GetOrCreateResourceOperator(
                task_id="get_or_create_resource_" + resource_name,                
                package_name_or_id=package_name,
                resource_name=resource_label,
                resource_attributes=dict(
                    format=resource["format"],
                    is_preview=True,
                    url_type="datastore",
                    extract_job=f"Airflow - files_to_datastore.py - " + package_name,
                    package_id=package_name,
                    url=resource["url"],
                ),
            )

            
            # determine whether the resource is new or not
            tasks_list["new_or_existing_" + resource_name] = BranchPythonOperator(
                task_id="new_or_existing_" + resource_name, 
                python_callable=is_resource_new,
                op_kwargs={"get_or_create_resource_task_id": "get_or_create_resource_" + resource_name, "resource_name": resource_name }
            )

            # determine if the resource needs to be updated
            # intelligently delta check based on input yaml config
            if (resource["format"].lower() == "csv" or resource.get("agol", None)) and not resource["url"].startswith("https://secure.toronto.ca/opendata"):
                check = "download_"
            else:
                check = "validate_schema_"
            tasks_list["does_" + resource_name + "_need_update"] = BranchPythonOperator(
                task_id="does_" + resource_name + "_need_update", 
                python_callable=does_resource_need_update,
                op_kwargs={"delta_check_task_id": check + resource_name, "resource_name": resource_name }
            )

            tasks_list["new_" + resource_name] = DummyOperator(task_id="new_" + resource_name)
            tasks_list["existing_" + resource_name] = DummyOperator(task_id="existing_" + resource_name)

            tasks_list["update_resource_" + resource_name] = DummyOperator(task_id="update_resource_" + resource_name)
            tasks_list["dont_update_resource_" + resource_name] = DummyOperator(task_id="dont_update_resource_" + resource_name)            

            tasks_list["prepare_update_" + resource_name] = DummyOperator(task_id="prepare_update_" + resource_name, trigger_rule="one_success")

            # intelligently delta check based on input yaml config
            if (resource["format"].lower() == "csv" or resource.get("agol", None)) and not resource["url"].startswith("https://secure.toronto.ca/opendata"):
                # ensure downloaded file has expected colnames
                tasks_list["validate_schema_" + resource_name] = ValidateFileSchemaOperator(
                    task_id = "validate_schema_" + resource_name,
                    filepath_task_id = "download_" + resource_name,
                    filepath_task_key = "data_path",
                    correct_columns = [attr.get("id", attr.get("source_name", None)) for attr in resource["attributes"]],
                )
            else:
                tasks_list["validate_schema_" + resource_name] = DeltaCheckOperator(
                    task_id="validate_schema_" + resource_name,
                    resource_id_task_id = "get_or_create_resource_" + resource_name,
                    resource_id_task_key = "id",
                    data_path_task_id = "download_" + resource_name,
                    data_path_task_key = "data_path",
                    config = resource,
                    #trigger_rule = "one_success",
                    resource_name = resource_name
                )

            # delete existing resource records
            tasks_list["delete_resource_" + resource_name] = DeleteDatastoreResourceOperator(
                task_id="delete_resource_" + resource_name,
                resource_id_task_id = "get_or_create_resource_" + resource_name,
                resource_id_task_key = "id",
                pool = "ckan_pool",
            )
            
            # delete existing resource records before restoring backup
            tasks_list["delete_resource_" + resource_name + "_before_backup"] = DeleteDatastoreResourceOperator(
                task_id="delete_resource_" + resource_name + "_before_backup",
                resource_id_task_id = "get_or_create_resource_" + resource_name,
                resource_id_task_key = "id",
                trigger_rule = "one_failed",
                pool = "ckan_pool",
            )

            # intelligently insert new records into an emptied resource based on input yaml config
            if resource["format"].lower() == "csv" or resource.get("agol", None):
                tasks_list["insert_records_" + resource_name] = CSVStreamToDatastoreYAMLOperator(
                    task_id="insert_records_" + resource_name,
                    resource_id_task_id = "get_or_create_resource_" + resource_name,
                    resource_id_task_key = "id",
                    data_path_task_id = "download_" + resource_name,
                    data_path_task_key = "data_path",
                    config = resource,
                    trigger_rule = "all_success",
                    retries = 0,
                    pool = "ckan_pool",
                    do_not_cache = True,
                )
                tasks_list["restore_backup_" + resource_name] = CSVStreamToDatastoreYAMLOperator(
                    task_id="restore_backup_" + resource_name,
                    resource_id_task_id = "get_or_create_resource_" + resource_name,
                    resource_id_task_key = "id",
                    data_path_task_id = "download_" + resource_name,
                    data_path_task_key = "backup_path",
                    config = resource,
                    trigger_rule = "all_success",
                    retries = 0,
                    pool = "ckan_pool",
                )

            else:
                tasks_list["insert_records_" + resource_name] = InsertDatastoreFromYAMLConfigOperator(
                    task_id="insert_records_" + resource_name,
                    resource_id_task_id = "get_or_create_resource_" + resource_name,
                    resource_id_task_key = "id",
                    data_path_task_id = "download_" + resource_name,
                    data_path_task_key = "data_path",
                    config = resource,
                    trigger_rule = "all_success",
                    retries = 0,
                    pool = "ckan_pool",
                    do_not_cache = True,
                )

                tasks_list["restore_backup_" + resource_name] = InsertDatastoreFromYAMLConfigOperator(
                    task_id="restore_backup_" + resource_name,
                    resource_id_task_id = "get_or_create_resource_" + resource_name,
                    resource_id_task_key = "id",
                    data_path_task_id = "download_" + resource_name,
                    data_path_task_key = "backup_path",
                    config = resource,
                    trigger_rule = "all_success",
                    retries = 0,
                    pool = "ckan_pool",
                )

            tasks_list["revert_last_refresh_date_" + resource_name] = EditResourceMetadataOperator(
                task_id = "revert_last_refresh_date_" + resource_name,
                resource_id_task_id = "get_or_create_resource_" + resource_name,
                resource_id_task_key = "id",
                last_modified_task_id = "get_or_create_resource_" + resource_name,
                last_modified_task_key = "last_modified",
            )

            tasks_list["datastore_cache_" + resource_name] = DatastoreCacheOperator(
                task_id="datastore_cache_" + resource_name,
                resource_id_task_id = "get_or_create_resource_" + resource_name,
                resource_id_task_key = "id",
                pool = "ckan_datastore_cache_pool",
                execution_timeout=timedelta(hours=2),
            )

            tasks_list["clean_backups_" + resource_name] = CleanBackupFilesOperator(
                task_id = "clean_backups_" + resource_name,
                data_path_task_id = "download_" + resource_name,
                data_path_task_key = "data_path",
                backup_path_task_id = "download_" + resource_name,
                backup_path_task_key = "backup_path",
                success_task_id = "insert_records_" + resource_name,
                success_task_key = "success",
                trigger_rule = "one_success",
            )
            
            # init a temp directory and get/create the package for the target data
            tmp_dir >> get_or_create_package
            
            # grab the target data, put it in the temp dir, and get or create resource(s) for the data
            get_or_create_package >> tasks_list["download_" + resource_name] >> tasks_list["get_or_create_resource_" + resource_name] >> tasks_list["new_or_existing_" + resource_name]
            
            # for each resource, find out if said resource was just created or existed already
            tasks_list["new_or_existing_" + resource_name] >> [tasks_list["new_" + resource_name], tasks_list["existing_" + resource_name]]
            
            # for each resource, if the resource changed then update it ... otherwise dont touch it
            tasks_list["existing_" + resource_name] >> tasks_list["validate_schema_" + resource_name] >> tasks_list["does_" + resource_name + "_need_update"] >> [tasks_list["update_resource_" + resource_name], tasks_list["dont_update_resource_" + resource_name]]  
            
            # if the resource didnt change, dont touch it
            tasks_list["dont_update_resource_" + resource_name] >> done_inserting_into_datastore

            # if its new, add data into the datastore
            tasks_list["new_" + resource_name] >> tasks_list["prepare_update_" + resource_name]

            # if it didnt exist before this run, then dont backup or delete anything
            tasks_list["update_resource_" + resource_name] >> tasks_list["delete_resource_" + resource_name] >> tasks_list["prepare_update_" + resource_name]

            # write into the datastore
            tasks_list["prepare_update_" + resource_name] >> tasks_list["insert_records_" + resource_name] >> tasks_list["datastore_cache_" + resource_name] >> done_inserting_into_datastore

        
        # Delete our temporary dir and report success to slack
        done_inserting_into_datastore >> build_message >> success_message_slack #>> dag_complete

        for resource_label in resource_names:
            # clean the resource label so the DAG can label its tasks with it
            resource_name = resource_label.replace(" ", "")
            resource = dataset["resources"][resource_label]
            # if something happens while a resource is being deleted or added
            [tasks_list["insert_records_" + resource_name], tasks_list["datastore_cache_" + resource_name]] >> tasks_list["delete_resource_" + resource_name + "_before_backup"] >> tasks_list["restore_backup_" + resource_name] >> tasks_list["revert_last_refresh_date_" + resource_name]

            [done_inserting_into_datastore, tasks_list["revert_last_refresh_date_" + resource_name]] >> tasks_list["clean_backups_" + resource_name]
    
    return dag

# Generate DAGs using the function above as a template parameterized by the configs - one DAG per YAML file
CONFIG_FOLDER = os.path.dirname(os.path.realpath(__file__))
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

        pool = "default_pool"

        default_args = airflow_utils.get_default_args(
            {
                "owner": dag_owner_name,
                "depends_on_past": False,
                "email": [dag_owner_email],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": 3,
                "on_failure_callback": task_failure_slack_alert,
                "start_date": datetime(2023, 5, 5, 0, 0, 0),
                "config_folder": CONFIG_FOLDER,
                "pool": pool,
                "tags": ["dataset", "yaml"], 
                "weight_rule": "upstream"
            }
        )

        globals()[dag_id] = create_dag(dag_id,
                                        package_name,
                                        config[package_name],
                                        schedule,
                                        default_args)