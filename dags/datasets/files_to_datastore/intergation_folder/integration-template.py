"""
This is the 2nd generation of the Airflow DAG generator module.

It serves as the backbone of the Open Data pipeline.
It is continuously digested by Apache Airflow Scheduler.
DAGs are generated based on the YAML files available in the Current Working Directory (CWD).
The module takes advantages of Airflow TaskFlow API for better performance and sustainability. 
(Compatibility Warning: TaskFlow API is supported by Airflow v2.0 and above)

The module is architected as follows:
- dag_factory() function: contains a for-loop to ingest all the YAMLs from its CWD and runs create_dag() for each of them.
    - create_dag() function: it contains the DAG constuctor function which is integration_template(). 
    The essense of this function is to dynamically change the DAG-level attributes based on the digested YAML.
        - integration_template() function: this is main funciton making a DAG grounded on TaskFlow paradigm.
        To see the architecture (psuedo code) of the integration_template() function, 
        it is recommended to check out its flowchart created in Miro board:
        https://miro.com/app/board/uXjVNvIJyzE=/?share_link_id=481351942209
        (Please contact Reza.Ghasemzadeh@toronto.ca if you need access to view the board)

At the end, the module runs dag_factory() function.

Module authors: Yanan.Zhang@toronto.ca and Reza.Ghasemzadeh@toronto.ca
Developed in Winter 2024.
Compatible with Python 3.7, Apache Airflow 2.6.3, CKAN 2.9
"""

# Python Built-In Libraries
import os
import yaml
import logging
import pendulum
import shutil
import time

# Third-Party Libraries
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

# Custom Libraries
from ckan_operators.package_operator import GetOrCreatePackage
from ckan_operators.resource_operator import GetOrCreateResource, EditResourceMetadata
from ckan_operators.datastore_operator import (
    DeleteDatastoreResource,
    stream_to_datastore,
)
from readers.base import select_reader
from utils import misc_utils
from utils_operators.slack_operators import task_failure_slack_alert, SlackWriter


def create_dag(package_name, config, schedule, default_args):
    """
    it contains the DAG constuctor function which is integration_template(). 

    The essense of this function is to dynamically change the DAG-level attributes based on the fed arguments.

    Args:
        - package_name (str): Name of the package/dataset
        - config (nested dict): all the informations contained in the YAML file.
        - schedule (str): the DAG run schedule
        - default_args (dict): contains the DAG default arguments

    Return: runs integration_template()
    """
    @dag(
        dag_id=package_name,
        schedule=schedule,
        start_date=pendulum.now(),
        catchup=False,
        default_args=default_args,
    )
    def integration_template():
        ################################################
        # --------------Get config params----------------
        package = config[package_name]
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
        # package metadata attributes, where available
        package_metadata = {}
        for metadata_attribute in metadata_pool:
            if metadata_attribute in package.keys():
                package_metadata[metadata_attribute] = package[metadata_attribute]
            else:
                package_metadata[metadata_attribute] = None

        dir_path = Variable.get("tmp_dir")
        dag_tmp_dir = dir_path + "/" + package_name
        ###################################################################
        # ---------------------- Package level tasks ----------------------
        @task
        def create_tmp_dir(dag_id, dir_path):
            """
            Create a directory with dag name

            Args:
            - dag_id : str
                the id of the dag(pipeline)
            - dir_path : str
                the path of directory

            Returns: full directory path string
            Return Type: str
            """
            tmp_dir = misc_utils.create_dir_with_dag_id(dag_id, dir_path)
            return tmp_dir

        @task
        def get_or_create_package(package_name, package_metadata):
            """
            Get the existing CKAN package object or create one.

            Args:
            - package_name: str
                the package name/id to be retrieved/created.
            - package_metadata: dict 
                the package metadata in form of a dict. 
                Required when the function wants to create the package.

            Return: CKAN Package Object Metadata
            Return Type: Dict
            """
            package = GetOrCreatePackage(
                package_name=package_name, 
                package_metadata=package_metadata,
            )
            return package.get_or_create_package()

        @task
        def message_factory(package_name, package, **context):
            """
            Generate a formatted message for each YAML file.

            The message contains the new/updated resource names and thier record count. 
            It will look like this:
                Package: package_name
                Resources:
                    - recource_1 [format] : ### records
                    - recource_2 [format] : ### records
            Usually, the output would be fed into the `SlackTownCrier` task to get announced on Slack.

            Args:
            - package_name: str
            - package: str
                YAML file contents.

            Returns:
                The final message (str)
                None: If no resources are found in the YAML file or if there is no new/updated resource
            """
            resources = package.get("resources", {})

            if not resources:   
                logging.info("No resources found in the YAML file.")
                return None

            message_lines = [f"*Package*: {package_name}", "\t\t\t*Resources*:"]
            
            for resource_name, resource_content in resources.items():
                try:
                    resource_format = resource_content.get("format")
                    record_count = context["ti"].xcom_pull(task_ids="insert_records_" + resource_name.replace(" ", ""))["record_count"]
                    message_line = f"\t\t\t\t- {resource_name} `{resource_format}`: {record_count} records"
                    message_lines.append(message_line)
                except Exception as e:
                    logging.error(e)
                    continue
            
            # The following `if` statement makes sure that the blanc/meaningless messages won't be created.
            final_message = "\n".join(message_lines) # if len(message_lines) > 2 else None
            
            return final_message

        @task
        def slack_writer(dag_id, message_header, message_content, message_body):
            """
            Send the message to the appropriate slack channel
            
            Args:
            - dag_id: The DAG ID; It appears after the message_header
            - message_header: The title of the message, to appear in bold font.
            - message_content: The content of the message; Usually supplied by the `message_factory` task.
            - message_body: Sort of a footer. Displayed at the end of the message.

            Return: Pass the inputs to the `SlackTownCrier` plugin to announce on slack.
            """
            return SlackWriter(dag_id, message_header, message_content, message_body).announce()
        
        # ---------------------- init tasks ----------------------
        create_tmp_dir = create_tmp_dir(package_name, dir_path)
        get_or_create_package = get_or_create_package(package_name, package_metadata)

        # This dummy task would be use at the end of the pipeline!
        done_inserting_into_datastore = EmptyOperator(
            task_id="done_inserting_into_datastore", 
            trigger_rule = "none_failed",
        )

        ############################################################
        # ---------------Resource level task lists------------------
        task_list = {}
        resources = package["resources"]
        resource_list = resources.keys()

        for resource_name in resource_list:
            # clean the resource name so the DAG can label its tasks with it
            resource_label = resource_name.replace(" ", "")

            # get resource config
            resource_config = resources[resource_name]
            attributes = resource_config["attributes"]

            #define the file name and its directory and backup
            resource_filename = resource_name + ".csv"
            resource_filepath = dag_tmp_dir + "/" + resource_filename
            backup_resource_filename = "backup_" + resource_filename
            backup_resource_filepath = dag_tmp_dir + "/" + backup_resource_filename

            # download source data
            @task(task_id="download_data_" + resource_label)
            def read_from_readers(package_name, resource_name, resource_config):
                """
                Download all resources provided in the YAML file.

                Regardless of whether there is any new resource or updaten, 
                all the resources - including those with no update - will be
                downloade, converted in csv format and finally saved into the corresponding temporary directory.

                Args:
                - package_name: str
                - resource_name: str
                - resource_config: dict
                    it is a nested dict which contains the resource format, URL, attributes, etc.

                Returns: True
                Return Type: Bool
                """

                # find and instantiate the approoriate reader for the resource based on its file format.
                reader = select_reader(
                    package_name=package_name,
                    resource_name=resource_name,
                    resource_config=resource_config,
                )

                # convert the downloaded resource file into csv format
                reader.write_to_csv()

                if os.listdir(dag_tmp_dir):
                    logging.info(
                        f"Reader output file {resource_filename} Successfully."
                    )
                    logging.info(f"File list: {os.listdir(dag_tmp_dir)}")

                else:
                    raise Exception("Reader failed!")

                return True

            @task(
                task_id="get_or_create_resource_" + resource_label,
                multiple_outputs=True,
            )
            def get_or_create_resource(package_name, resource_name):
                """
                Get the existing CKAN resource object or create one.

                Args:
                - package_name: str
                    the package name/id of the resource to be retrieved/created.
                - resource_name: str 
                    the resource name/id to be retrieved/created.

                Return: CKAN resource object metadata
                Return Type: Dict
                """
                ckan_resource = GetOrCreateResource(
                    package_name=package_name,
                    resource_name=resource_name,
                    resource_attributes=dict(
                        format=resource_config["format"],
                        is_preview=True,
                        url_type="datastore",
                        extract_job=f"Airflow: Integration Pipeline {package_name}",
                        url=resource_config["url"],
                    ),
                )

                return ckan_resource.get_or_create_resource()

            # branching for new and existing resource
            @task.branch(task_id="new_or_existing_" + resource_label)
            def new_or_existing(resource_label, **context):
                """
                Detemines whether the resource is brand new or not 
                based the "is_new" key. 
                Accordingly, the fuction directs to the correct branch.

                Args:
                - resource_label: str

                Returns: ID of the downstream task
                Return type: str
                """
                # Get the output of the get_or_create_resource task
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                # Check whether the resource is new
                if resource["is_new"]:
                    # Heads up: brand_new is a dummy/empty task!
                    return "brand_new_" + resource_label

                return "does_" + resource_label + "_need_update"

            @task.branch(task_id="does_" + resource_label + "_need_update")
            def does_resource_need_update(
                resource_label, resource_filepath, backup_resource_filepath
            ):
                """
                Determine if the resource needs to be updated.

                When the resource already exist,
                compare the new file with the existing file, 
                checking if any new change is introduced.
                Accordingly, the fuction directs to the correct branch.

                Args:
                - resource_label: str
                - resource_filepath: str
                    Absoulte path of the new file.
                - backup_resource_filepath: str
                    Absoulte path of the existing file.

                Returns: ID of the downstream task
                Return type: str
                """
                # Compare the two files by their hash values! A boolean is returned.
                equal = misc_utils.file_equal(
                    resource_filepath, 
                    backup_resource_filepath,
                )
                # Determine the downsteam task to move on.
                if equal:
                    # Heads up: dont_update_resource is a dummy/empty task!
                    return "dont_update_resource_" + resource_label
                else:
                    return "delete_resource_" + resource_label

            @task(task_id="delete_resource_" + resource_label)
            def delete_resource(resource_label, **context):
                """
                Delete Datastore resource from CKAN database.

                The function removes the existing resource file (not object!) from CKAN DB
                to make room empty for the new file to be inserted.
                
                Args:
                - resource_label: str

                Return: None
                """
                # Get the output of the get_or_create_resource task
                # which is the CKAN resource object.
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                # Delete a given datastore rescouce records by calling ckan.action.datastore_delete.
                # The DeleteDatastoreResource class is capable of keeping 
                # the resource schema by setting keep_schema = True
                delete = DeleteDatastoreResource(resource_id=resource["id"])
                return delete.delete_datastore_resource()

            @task(
                task_id="insert_records_" + resource_label , 
                trigger_rule="none_failed_min_one_success",
            )
            def insert_records_to_datastore(
                file_path, attributes, resource_label, **context
            ):
                """
                Insert the new/updated resource file as Datastore file into CKAN DB.

                Args:
                - file_path : str
                    the absolute path of data file
                - attributes : Dict
                    the attributes section of yaml config (data fields)
                - resource_label : str

                Return: None
                """
                # Get the output of the get_or_create_resource task
                # which is the CKAN resource object.
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                # Stream records from csv; insert records into ckan datastore in batch
                # based on yaml config attributes.
                return stream_to_datastore(
                    resource_id=resource["id"],
                    file_path=file_path,
                    attributes=attributes,
                    do_not_cache=True,
                )

            # trigger datastore_cache
            @task(
                task_id="datastore_cache_" + resource_label,
                pool="ckan_datastore_cache_pool",
            )
            def datastore_cache(resource_label, **context):
                """
                Make different format versions of the resource.

                It rund the iotrans plugin which create duplicates of the datastore resource
                in different formats and inserts them in the same packeage but in the filestore format.

                Args:
                - resource_label: str

                Return: None
                """
                logging.info(f"Staring caching {resource_label}")
                # Get the output of the get_or_create_resource task
                # which is the CKAN resource object.
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                # Connect to CKAN
                ckan = misc_utils.connect_to_ckan()
                return ckan.action.datastore_cache(resource_id=resource["id"])

            @task(
                task_id="clean_backups_" + resource_label, 
                trigger_rule = "one_success",
            )
            def clean_backups(resource_label, resource_filepath, backup_resource_filepath, **kwargs):
                """
                Clean up unneccessary resource files from the directory.

                The functions considers three scenarios:
                - The new/updated resource is inserted to CKAN successfully: 
                    Overwrite the existing backup file with the new file.
                - The new/updated resource is NOT inserted to CKAN successfully.
                    Do not replace the new file with the old file. Discard the new file!
                - There were no update to the existing resource.
                    Do not replace the new file with the old file. Discard the new file!
                Finally it logs the survivig file name along with its most recent modification date.

                Args:
                - resource_label: str
                - resource_filepath: str
                    Absolute path of the new resource file
                - backup_resource_filepath: str
                    Absolute path of the existing resource file

                Return: True
                Return Type: Bool
                """
                # When the resource is updated successfully
                succeeded_to_update = kwargs['ti'].xcom_pull(task_ids="datastore_cache_" + resource_label)
                if succeeded_to_update:
                    shutil.move(resource_filepath, backup_resource_filepath)
                    logging.info(">>>>>>> BACKUP RESOURCE OVERWRITTEN BY THE NEW FILE <<<<<<<")

                # Stop overwrting the original file in case of failure
                elif kwargs['ti'].xcom_pull(task_ids="restore_backup_records_" + resource_label):
                    os.remove(resource_filepath)
                    logging.info(">>>>>>> DISCARDED THE NEW CORRUPTED RESOURCE <<<<<<<")

                # Stop overwrting the original file in case of no update
                else:
                    os.remove(resource_filepath)
                    logging.info(">>>>>>> NO NEW RECORD - NEW RESOURCE DISCARDED! <<<<<<<")

                # Log the final file name along with its last modification date.
                mod_time = os.path.getmtime(backup_resource_filepath)
                mod_time = time.ctime(mod_time)
                logging.info(f">>>>>>> BACKUP FILE: '{backup_resource_filepath}', Last Modification Date and Time: {mod_time} <<<<<<<")

                return True

            ############################################################
            #------------------ Failure Protocol ------------------
            # Delete the incomplete new resource from CKAN
            @task(
                task_id="delete_failed_resource_" + resource_label, 
                trigger_rule="one_failed",
            )
            def delete_failed_resource(resource_label, **context):
                """
                Delete failed Datastore resource from CKAN database.

                The function removes the new but corrupted resource file (not object!) from CKAN DB
                to make room empty for the backup file to be inserted.
                
                Args:
                - resource_label: str

                Return: None
                """
                # Get the output of the get_or_create_resource task
                # which is the CKAN resource object.
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                # Delete a given datastore rescouce records by calling ckan.action.datastore_delete.
                # The DeleteDatastoreResource class is capable of keeping 
                # the resource schema by setting keep_schema = True
                delete = DeleteDatastoreResource(resource_id=resource["id"])
                return delete.delete_datastore_resource()

            @task(task_id="restore_backup_records_" + resource_label)
            def restore_backup_records_(
                file_path, attributes, resource_label, **context
            ):
                """
                Insert the backup resource file as Datastore file into CKAN DB.

                Args:
                - file_path : str
                    the absolute path of data file
                - attributes : Dict
                    the attributes section of yaml config (data fields)
                - resource_label : str

                Return: None
                """
                # Get the output of the get_or_create_resource task
                # which is the CKAN resource object.
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                # Stream records from csv; insert records into ckan datastore in batch
                # based on yaml config attributes.
                return stream_to_datastore(
                    resource_id=resource["id"],
                    file_path=file_path,
                    attributes=attributes,
                    do_not_cache=True,
                )
            ############################################################
            # --------------- Init Resource Level Tasks ---------------
            # All tasks are intialized and stored as key:value pairs in task_list dict
            # Thi is done to simplify tasks' dependency definition.
            task_list["download_data_" + resource_label] = read_from_readers(
                package_name=package_name,
                resource_name=resource_name,
                resource_config=resource_config,
            )

            task_list[
                "get_or_create_resource_" + resource_label
            ] = get_or_create_resource(
                package_name=package_name, 
                resource_name=resource_name,
            )

            task_list["new_or_existing_" + resource_label] = new_or_existing(
                resource_label,
            )

            task_list["brand_new_" + resource_label] = EmptyOperator(
                task_id="brand_new_" + resource_label,
            )

            task_list[
                "does_" + resource_label + "_need_update"
            ] = does_resource_need_update(
                resource_label=resource_label,
                resource_filepath=resource_filepath,
                backup_resource_filepath=backup_resource_filepath,
            )

            task_list["dont_update_resource_" + resource_label] = EmptyOperator(
                task_id="dont_update_resource_" + resource_label,
            )

            task_list["delete_resource_" + resource_label] = delete_resource(
                resource_label=resource_label,
            )

            task_list["insert_records_" + resource_label] = insert_records_to_datastore(
                file_path=resource_filepath,
                attributes=attributes,
                resource_label=resource_label,
            )

            task_list["datastore_cache_" + resource_label] = datastore_cache(
                resource_label=resource_label,
            )

            task_list["delete_failed_resource_" + resource_label] = delete_failed_resource(
                resource_label=resource_label,
            )
            
            task_list["restore_backup_records_" + resource_label] = restore_backup_records_(
                file_path=backup_resource_filepath,
                attributes=attributes,
                resource_label=resource_label,
            )

            task_list["clean_backups_" + resource_label] = clean_backups(
                resource_label=resource_label,
                resource_filepath=resource_filepath,
                backup_resource_filepath=backup_resource_filepath,
            )

            ############################################################
            # ---------- Explicitly Define Tasks Dependencies ----------
            (
                create_tmp_dir
                >> get_or_create_package
                >> task_list["download_data_" + resource_label]
                >> task_list["get_or_create_resource_" + resource_label]
                >> task_list["new_or_existing_" + resource_label]
            )

            (
                task_list["new_or_existing_" + resource_label]
                >> Label("New")
                >> task_list["brand_new_" + resource_label]
            )

            (
                task_list["new_or_existing_" + resource_label]
                >> Label("Existing")
                >> task_list["does_" + resource_label + "_need_update"]
            )

            (
                task_list["brand_new_" + resource_label]
                >> task_list["insert_records_" + resource_label]
            )

            (   
                task_list["does_" + resource_label + "_need_update"]
                >> Label("No Update")
                >> task_list["dont_update_resource_" + resource_label]
            )

            (   
                task_list["does_" + resource_label + "_need_update"]
                >> Label("Update")
                >> task_list["delete_resource_" + resource_label]
            )

            (
                task_list["dont_update_resource_" + resource_label]
                >> task_list["clean_backups_" + resource_label]
            )
            (
                task_list["delete_resource_" + resource_label]
                >> task_list["insert_records_" + resource_label]
            )
            (
                task_list["insert_records_" + resource_label]
                >> Label("Success")
                >> task_list["datastore_cache_" + resource_label]
                >> task_list["clean_backups_" + resource_label]
                >> done_inserting_into_datastore
            )
            
            # Fail Tolerance Branch
            (
                task_list["insert_records_" + resource_label]
                >> Label("Fail")
                >> task_list["delete_failed_resource_" + resource_label]
                >> task_list["restore_backup_records_" + resource_label]
                >> task_list["clean_backups_" + resource_label]
            )

        # Init slack-related task.
        message_task = message_factory(package_name, package)
        slack_writer_task = slack_writer(
            dag_id = package_name,
            message_header = "DAG Template v2",
            message_content = message_task,
            message_body = "",
        )
        # Set up the task dependencies
        done_inserting_into_datastore >> message_task >> slack_writer_task

    return integration_template()


def dag_factory():
    """
    Read DAG configurations from the YAMLs in the directory.

    For each YAML, it loads the YAML files into the memory and sets the DAG/Package level information from it. 
    DAG default args are also set here either manualy or based on the YAML file content.
    Finally, it runs the create_dag() functoin (defined above) by feeding the extracted configs into its args.

    Args: None

    Returns: None
    """
    CONFIG_FOLDER = os.path.dirname(os.path.realpath(__file__))
    for config_file in os.listdir(CONFIG_FOLDER):
        if config_file.endswith(".yaml"):
            # read config file
            with open(CONFIG_FOLDER + "/" + config_file, "r") as f:
                config = yaml.load(f, yaml.SafeLoader)

            # dag level info
            package_name = list(config.keys())[0]
            schedule = config[package_name]["schedule"]
            dag_owner_name = config[package_name]["dag_owner_name"]
            dag_owner_email = config[package_name]["dag_owner_email"]

            default_args = {
                "owner": dag_owner_name,
                "email": [dag_owner_email],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": 3,
                "on_failure_callback": task_failure_slack_alert,
                "tags": ["dataset", "yaml"],
            }

            # create dag
            create_dag(package_name, config, schedule, default_args)


dag_factory()
