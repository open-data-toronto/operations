"""
This is the 2nd generation of the Airflow DAG generator module.

It serves as the backbone of the Open Data pipeline.
DAGs are generated based on the YAML files available in the Current Working Directory (CWD).
The module takes advantages of Airflow TaskFlow API for better performance and sustainability. 
(Compatibility Warning: TaskFlow API is supported by Airflow v2.0 and above)
The module is architected as follows:
- dag_factory() function: contains a for-loop to ingest all the YAMLs from its CWD and runs create_dag() for each of them.
    - create_dag() function: it contains the DAG constuctor function which is integration_template(). 
    The essense of this function is to dynamically change the DAG-level attributes based on the digested YAML.
        - integration_template() function: this is main funciton making a DAG grounded on TaskFlow paradigm.
"""


import os
import yaml
import logging
import pendulum
import shutil
import time

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

from ckan_operators.package_operator import GetOrCreatePackage
from ckan_operators.resource_operator import GetOrCreateResource, EditResourceMetadata
from ckan_operators.datastore_operator import (
    DeleteDatastoreResource,
    stream_to_datastore,
)
from readers.base import select_reader
from utils import misc_utils
from utils_operators.slack_operators import task_failure_slack_alert, MessageFactory, SlackTownCrier


def create_dag(package_name, config, schedule, default_args):
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
        dir_path = Variable.get("tmp_dir")
        dag_tmp_dir = dir_path + "/" + package_name
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

        # ----------------------init tasks
        create_tmp_dir = create_tmp_dir(dag_id=package_name, dir_path=dir_path)
        get_or_create_package = get_or_create_package(package_name, package_metadata)
        done_inserting_into_datastore = EmptyOperator(
            task_id="done_inserting_into_datastore", trigger_rule = "none_failed"
        )

        #------------------ Slack Notification ------------------
        # @task(trigger_rule="none_failed_min_one_success")
        # def scribe(package_name, yaml_file_content, record_counts):
        #     message = MessageFactory(package_name, yaml_file_content, record_counts).scribe()
        #     return message

        @task
        def scribe(package_name, package, **context):
            """
            Generate a formatted message for each resource in a YAML file.
            Usually, the outpout would be fed into the SlackTownCrier to be announced on Slack.

            Returns:
                Str: The final message.
                None: If no resources are found in the YAML file.
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
                    #record_count = task_list["insert_records_" + resource_name.replace(" ", "")]["record_count"]
                    # if resource_format:
                    #     record_count = context["ti"].xcom_pull(task_ids="insert_records_" + resource_name.replace(" ", ""))["record_count"]
                    #     message_line = f"\t\t\t\t- {resource_name} `{resource_format}`: {record_count} records"
                    #     message_lines.append(message_line)
                    # else:
                    #     logging.error(f"No format found for resource: {resource_name}")
                except Exception as e:
                    logging.error(e)
                    continue
            
            # The following `if` statement makes sure that the blanc/meaningless messages won't be created.
            final_message = "\n".join(message_lines) if len(message_lines) > 2 else None
            
            return final_message

        @task
        def slack_town_crier(dag_id, message_header, message_content, message_body):
            return SlackTownCrier(dag_id, message_header, message_content, message_body).announce()

        # slack_town_crier(
        #     dag_id = package_name,
        #     message_header = "Slack Town Crier - Integration Template",
        #     message_content = scribe(package_name, config, record_count=10),
        #     message_body = "",
        # )

        

        ############################################################
        # ---------------Resource level task lists------------------
        task_list = {}
        record_counts = {}
        resources = package["resources"]
        resource_list = resources.keys()

        for resource_name in resource_list:
            # clean the resource name so the DAG can label its tasks with it
            resource_label = resource_name.replace(" ", "")

            # get resource config
            resource_config = resources[resource_name]
            attributes = resource_config["attributes"]
            resource_filename = resource_name + ".csv"
            resource_filepath = dag_tmp_dir + "/" + resource_filename

            ############################ Backup ################################
            backup_resource_filename = "backup_" + resource_filename
            backup_resource_filepath = dag_tmp_dir + "/" + backup_resource_filename
            ####################################################################

            # download source data
            @task(task_id="download_data_" + resource_label)
            def read_from_readers(package_name, resource_name, resource_config):
                reader = select_reader(
                    package_name=package_name,
                    resource_name=resource_name,
                    resource_config=resource_config,
                )
                reader.write_to_csv()

                if os.listdir(dag_tmp_dir):
                    logging.info(
                        f"Reader output file {resource_filename} Successfully."
                    )
                    logging.info(f"File list: {os.listdir(dag_tmp_dir)}")

                else:
                    raise Exception("Reader failed!")

                return True

            # get or create resource
            @task(
                task_id="get_or_create_resource_" + resource_label,
                multiple_outputs=True,
            )
            def get_or_create_resource(package_name, resource_name):
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
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                if resource["is_new"]:
                    return "brand_new_" + resource_label

                # return "existing_" + resource_label
                return "does_" + resource_label + "_need_update"

            # compare if new file and existing file are exactly same
            # determine if the resource needs to be updated
            @task.branch(task_id="does_" + resource_label + "_need_update")
            def does_resource_need_update(
                resource_label, resource_filepath, backup_resource_filepath
            ):
                # backup_resource_filename = "backup_" + resource_filename
                # backup_resource_filepath = dag_tmp_dir + "/" + backup_resource_filename

                equal = misc_utils.file_equal(
                    resource_filepath, backup_resource_filepath
                )

                if equal:
                    return "dont_update_resource_" + resource_label
                else:
                    #return "update_resource_" + resource_label
                    return "delete_resource_" + resource_label

            # delete datastore resource
            @task(task_id="delete_resource_" + resource_label)
            def delete_resource(resource_label, **context):
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                delete = DeleteDatastoreResource(resource_id=resource["id"])
                return delete.delete_datastore_resource()

            # stream to ckan datastore
            @task(task_id="insert_records_" + resource_label , trigger_rule="none_failed_min_one_success")
            def insert_records_to_datastore(
                file_path, attributes, resource_label, **context
            ):
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )

                #json.loads("test")

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
                logging.info(f"Staring caching {resource_label}")
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                ckan = misc_utils.connect_to_ckan()
                return ckan.action.datastore_cache(resource_id=resource["id"])

            # clean up resource files, rename most recent resource_file to backup
            @task(task_id="clean_backups_" + resource_label, trigger_rule = "one_success")
            def clean_backups(resource_label, resource_filepath, backup_resource_filepath, **kwargs):
                # when the resource is updated successfully
                succeeded_to_update = kwargs['ti'].xcom_pull(task_ids="datastore_cache_" + resource_label)
                if succeeded_to_update:
                    shutil.move(resource_filepath, backup_resource_filepath)
                    logging.info(">>>>>>> BACKUP RESOURCE OVERWRITTEN BY THE NEW FILE <<<<<<<")
                    # logging.info(f"File list: {os.listdir(dag_tmp_dir)}")

                # stop overwrting the original file in case of failure
                # failure_pathway_tail_task = kwargs['ti'].xcom_pull(task_ids="restore_backup_records_" + resource_label)
                elif kwargs['ti'].xcom_pull(task_ids="restore_backup_records_" + resource_label):
                    os.remove(resource_filepath)
                    logging.info(">>>>>>> DISCARDED THE NEW CORRUPTED RESOURCE <<<<<<<")

                # stop overwrting the original file in case of no update
                else:
                    os.remove(resource_filepath)
                    logging.info(">>>>>>> NO NEW RECORD - NEW RESOURCE DISCARDED! <<<<<<<")

                mod_time = os.path.getmtime(backup_resource_filepath)
                mod_time = time.ctime(mod_time)
                logging.info(f">>>>>>> BACKUP FILE: '{backup_resource_filepath}', Last Modification Date and Time: {mod_time} <<<<<<<")

                return True

            ##############################################################################################
            #------------------ Failure Protocol ------------------
            # Delete the incomplete new resource from CKAN
            @task(task_id="delete_failed_resource_" + resource_label, trigger_rule="one_failed")
            def delete_failed_resource(resource_label, **context):
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                delete = DeleteDatastoreResource(resource_id=resource["id"])
                return delete.delete_datastore_resource()

            # stream to ckan datastore
            @task(task_id="restore_backup_records_" + resource_label)
            def restore_backup_records_(
                file_path, attributes, resource_label, **context
            ):
                resource = context["ti"].xcom_pull(
                    task_ids="get_or_create_resource_" + resource_label
                )
                return stream_to_datastore(
                    resource_id=resource["id"],
                    file_path=file_path,
                    attributes=attributes,
                    do_not_cache=True,
                )
            ##############################################################################################
            # -----------------Init tasks
            task_list["download_data_" + resource_label] = read_from_readers(
                package_name=package_name,
                resource_name=resource_name,
                resource_config=resource_config,
            )

            task_list[
                "get_or_create_resource_" + resource_label
            ] = get_or_create_resource(
                package_name=package_name, resource_name=resource_name
            )

            task_list["new_or_existing_" + resource_label] = new_or_existing(
                resource_label
            )

            task_list["brand_new_" + resource_label] = EmptyOperator(
                task_id="brand_new_" + resource_label
            )
            # task_list["existing_" + resource_label] = EmptyOperator(
            #     task_id="existing_" + resource_label
            # )

            task_list[
                "does_" + resource_label + "_need_update"
            ] = does_resource_need_update(
                resource_label=resource_label,
                # resource_filename=resource_filename,
                resource_filepath=resource_filepath,
                backup_resource_filepath=backup_resource_filepath,
            )

            # task_list["update_resource_" + resource_label] = EmptyOperator(
            #     task_id="update_resource_" + resource_label
            # )
            task_list["dont_update_resource_" + resource_label] = EmptyOperator(
                task_id="dont_update_resource_" + resource_label
            )

            task_list["delete_resource_" + resource_label] = delete_resource(
                resource_label=resource_label
            )

            # task_list["ready_insert_" + resource_label] = EmptyOperator(
            #     task_id="ready_insert_" + resource_label,
            #     trigger_rule="none_failed_min_one_success",
            # )

            task_list["insert_records_" + resource_label] = insert_records_to_datastore(
                file_path=resource_filepath,
                attributes=attributes,
                resource_label=resource_label,
            )
            
            # record_counts[resource_name] = task_list["insert_records_" + resource_label]["record_count"]

            task_list["datastore_cache_" + resource_label] = datastore_cache(
                resource_label=resource_label
            )

            ##############################################################################################
            #------------------ Failure Protocol ------------------
            # task_list["failed_to_insert_" + resource_label] = EmptyOperator(
            #     task_id="failed_to_insert_" + resource_label,
            #     trigger_rule="one_failed",
            # )
            
            task_list["delete_failed_resource_" + resource_label] = delete_failed_resource(
                resource_label=resource_label
            )
            
            task_list["restore_backup_records_" + resource_label] = restore_backup_records_(
                file_path=backup_resource_filepath,
                attributes=attributes,
                resource_label=resource_label,
            )
            ##############################################################################################

            # Clean up
            task_list["clean_backups_" + resource_label] = clean_backups(
                #resource_filename=resource_filename, 
                resource_label=resource_label,
                resource_filepath=resource_filepath,
                backup_resource_filepath=backup_resource_filepath,
            )

            # ----Task Flow----
            (
                create_tmp_dir
                >> get_or_create_package
                >> task_list["download_data_" + resource_label]
                >> task_list["get_or_create_resource_" + resource_label]
                >> task_list["new_or_existing_" + resource_label]
            )

            # task_list["new_or_existing_" + resource_label] >> [
            #     task_list["brand_new_" + resource_label],
            #     # task_list["existing_" + resource_label],
            #     task_list["does_" + resource_label + "_need_update"],
            # ]

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
                # >> task_list["ready_insert_" + resource_label]
                >> task_list["insert_records_" + resource_label]
            )
            # (
            #     task_list["existing_" + resource_label]
            #     >> task_list["does_" + resource_label + "_need_update"]
            # )

            # task_list["does_" + resource_label + "_need_update"] >> [
            #     #task_list["update_resource_" + resource_label],
            #     task_list["delete_resource_" + resource_label],
            #     task_list["dont_update_resource_" + resource_label],
            # ]

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
                #>> done_inserting_into_datastore
            )
            (
                # task_list["update_resource_" + resource_label]
                task_list["delete_resource_" + resource_label]
                # >> task_list["ready_insert_" + resource_label]
                >> task_list["insert_records_" + resource_label]
            )
            (
                #task_list["ready_insert_" + resource_label]
                task_list["insert_records_" + resource_label]
                >> Label("Success")
                >> task_list["datastore_cache_" + resource_label]
                >> task_list["clean_backups_" + resource_label]
                >> done_inserting_into_datastore
            )
            ##############################################################################################
            #------------------ Failure Protocol ------------------
            (
                task_list["insert_records_" + resource_label]
                >> Label("Fail")
                #>> task_list["failed_to_insert_" + resource_label]
                >> task_list["delete_failed_resource_" + resource_label]
                >> task_list["restore_backup_records_" + resource_label]
                >> task_list["clean_backups_" + resource_label]
            )
            ##############################################################################################
            # (
            #     done_inserting_into_datastore
            #     >> task_list["clean_backups_" + resource_label]
            # )

        # Define the tasks
        #record_count = task_list["insert_records_" + resource_name.replace(" ", "")]["record_count"]
        scribe_task = scribe(package_name, package)#, record_counts)#, record_count=10)
        slack_town_crier_task = slack_town_crier(
            dag_id = package_name,
            message_header = "Slack Town Crier - Integration Template",
            message_content = scribe_task,
            message_body = "",
        )

        # Set up the task dependencies
        done_inserting_into_datastore >> scribe_task >> slack_town_crier_task

    return integration_template()


def dag_factory():
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
