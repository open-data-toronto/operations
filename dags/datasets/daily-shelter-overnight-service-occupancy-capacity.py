import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path
import ckanapi

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from ckan_operators.datastore_operator import (
    BackupDatastoreResourceOperator, DeleteDatastoreResourceRecordsOperator,
    InsertDatastoreResourceRecordsOperator,
    RestoreDatastoreResourceBackupOperator)
from ckan_operators.package_operator import GetPackageOperator
from ckan_operators.resource_operator import (GetOrCreateResourceOperator,
                                              ResourceAndFileOperator)
from dateutil import parser
from utils import agol_utils, airflow_utils
from utils_operators.directory_operator import CreateLocalDirectoryOperator, DeleteLocalDirectoryOperator
from utils_operators.file_operators import DownloadFileOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator

#RESOURCES = {
#    "daily-shelter-overnight-service-occupancy-capacity-2021": {
#        "file_url": "http://opendata.toronto.ca/shelter.support.housing.administration\daily-shelter-and-overnight-occupancy-and-capacity/Daily shelter overnight occupancy - 2021.csv",
#        "name": "daily-shelter-overnight-service-occupancy-capacity-2021", 
#        "metadata": {
#            "format": "csv",
#            "is_preview": False,
#            "extract_job": f"Airflow: daily-shelter-overnight-service-occupancy-capacity-2021",
#            "url_type": "datastore",
#        }}}

SRC_URL = "http://opendata.toronto.ca/shelter.support.housing.administration/daily-shelter-and-overnight-occupancy-and-capacity/daily-shelter-overnight-service-occupancy-capacity-2021.csv"
PACKAGE_NAME = "daily-shelter-overnight-service-occupancy-capacity"
RESOURCE_NAME = "daily-shelter-overnight-service-occupancy-capacity-2021"

EXPECTED_COLUMNS = [
    {"id": "OCCUPANCY_DATE", "type": "text"},
    {"id": "ORGANIZATION_ID", "type": "text"},
    {"id": "ORGANIZATION_NAME", "type": "text"},
    {"id": "SHELTER_ID", "type": "text"},
    {"id": "SHELTER_GROUP", "type": "text"},
    {"id": "LOCATION_ID", "type": "text"},
    {"id": "LOCATION_NAME", "type": "text"},
    {"id": "LOCATION_ADDRESS", "type": "text"},
    {"id": "LOCATION_POSTAL_CODE", "type": "text"},
    {"id": "LOCATION_CITY", "type": "text"},
    {"id": "LOCATION_PROVINCE", "type": "text"},
    {"id": "PROGRAM_ID", "type": "text"},
    {"id": "PROGRAM_NAME", "type": "text"},
    {"id": "SECTOR", "type": "text"},
    {"id": "PROGRAM_MODEL", "type": "text"},
    {"id": "OVERNIGHT_SERVICE_TYPE", "type": "text"},
    {"id": "PROGRAM_AREA", "type": "text"},
    {"id": "SERVICE_USER_COUNT", "type": "text"},
    {"id": "CAPACITY_TYPE", "type": "text"},
    {"id": "CAPACITY_ACTUAL_BED", "type": "text"},
    {"id": "CAPACITY_FUNDING_BED", "type": "text"},
    {"id": "OCCUPIED_BEDS", "type": "text"},
    {"id": "UNOCCUPIED_BEDS", "type": "text"},
    {"id": "UNAVAILABLE_BEDS", "type": "text"},
    {"id": "CAPACITY_ACTUAL_ROOM", "type": "text"},
    {"id": "CAPACITY_FUNDING_ROOM", "type": "text"},
    {"id": "OCCUPIED_ROOMS", "type": "text"},
    {"id": "UNOCCUPIED_ROOMS", "type": "text"},
    {"id": "UNAVAILABLE_ROOMS", "type": "text"},
    {"id": "OCCUPANCY_RATE_BEDS", "type": "text"},
    {"id": "OCCUPANCY_RATE_ROOMS", "type": "text"},
]

TMP_DIR = Variable.get("tmp_dir")

ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
active_env = Variable.get("active_env")
ckan_address = ckan_creds[active_env]["address"]
ckan_apikey = ckan_creds[active_env]["apikey"]

ckan = ckanapi.RemoteCKAN(apikey=ckan_apikey, address=ckan_address)

with DAG(
    PACKAGE_NAME,
    default_args=airflow_utils.get_default_args(
        {
            "on_failure_callback": task_failure_slack_alert,
            "start_date": datetime(2020, 11, 24, 13, 35, 0),
            "retries": 0,
            "owner": "Mackenzie"
            # "retry_delay": timedelta(minutes=3),
        }
    ),
    description="Take data from opendata.toronto.ca (CSV) and put into datastore",
    schedule_interval="0 17 * * *",
    catchup=False,
    tags=["dataset"],
) as dag:

    def is_resource_new(get_or_create_resource_task_id, resource_name, **kwargs):
        resource = kwargs["ti"].xcom_pull(task_ids=get_or_create_resource_task_id)

        if resource["is_new"]:
            return "new_resource"

        return "existing_resource"



    def transform_data(**kwargs):
        ti = kwargs.pop("ti")
        tmp_dir = Path(ti.xcom_pull(task_ids="tmp_dir"))
        data_fp = Path(ti.xcom_pull(task_ids="get_data")["path"])

        data = pd.read_csv(data_fp, names = [name["id"] for name in EXPECTED_COLUMNS], encoding='utf-8', skiprows=1)
        print(data.head())
        print(data.iloc[0])
        #data = data.drop("_id", 1)

        #data = agol_utils.remove_geo_columns(data)

        filename = "new_data_transformed"
        filepath = tmp_dir / f"{filename}.parquet"

        data.to_parquet(filepath, engine="fastparquet", compression=None)

        return filepath

    def insert_new_records(**kwargs):
        ti = kwargs.pop("ti")
        fp = Path(ti.xcom_pull(task_ids="transform_data"))
        r = ti.xcom_pull(task_ids="get_or_create_resource")

        data = pd.read_parquet(fp)
        records = data.to_dict(orient="records")
        
        ckan.action.datastore_create( 
            id=r["id"], 
            fields= EXPECTED_COLUMNS, 
            records=records
        )



    tmp_dir = CreateLocalDirectoryOperator(
            task_id = "tmp_dir", 
            path = TMP_DIR + "/" + PACKAGE_NAME
        )   


    # success message to slack
    success_message_slack = GenericSlackOperator(
        task_id = "success_message_slack",
        message_header = "Files to Datastore " + PACKAGE_NAME,
        message_content = "",
        message_body = ""
    )

#    backups_dir = CreateLocalDirectoryOperator(
#        task_id="backups_dir", path=Path(Variable.get("backups_dir")) / PACKAGE_NAME,
#    )

    src = DownloadFileOperator(
        task_id="get_data",
        file_url=SRC_URL,
        dir =Path(Variable.get("tmp_dir")) / PACKAGE_NAME,
        filename="src_data.csv",
    )

    package = GetPackageOperator(
        task_id="get_package",
        address=ckan_address,
        apikey=ckan_apikey,
        package_name_or_id=PACKAGE_NAME,
    )

    #create_data_dictionary = PythonOperator(
    #    task_id="create_data_dictionary", python_callable=build_data_dict,
    #)

    get_or_create_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_resource",
        address=ckan_address,
        apikey=ckan_apikey,
        package_name_or_id=PACKAGE_NAME,
        resource_name=RESOURCE_NAME,
        resource_attributes=dict(
            format="csv",
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {PACKAGE_NAME}",
            package_id=PACKAGE_NAME
        ),
    )

    new_resource_branch = BranchPythonOperator(
        task_id="new_or_existing_resource", 
        python_callable=is_resource_new,
        op_kwargs={"get_or_create_resource_task_id": "get_or_create_resource", "resource_name": RESOURCE_NAME }
    )

    transformed_data = PythonOperator(
        task_id="transform_data", python_callable=transform_data,
    )

    backup_data = BackupDatastoreResourceOperator(
        task_id="backup_data",
        address=ckan_address,
        apikey=ckan_apikey,
        resource_task_id="get_or_create_resource",
        dir_task_id="tmp_dir",
        #sort_columns=["LOC_ID"],
    )

    #fields = PythonOperator(
    #    task_id="get_fields", python_callable=get_fields, trigger_rule="none_failed"
    #)

    delete_tmp_data = DeleteLocalDirectoryOperator(
            task_id = "del_tmp_dir", 
            path = TMP_DIR + "/" + PACKAGE_NAME
    )

    sync_timestamp = ResourceAndFileOperator(
        task_id="sync_timestamp",
        address=ckan_address,
        apikey=ckan_apikey,
        download_file_task_id="get_data",
        resource_task_id="get_or_create_resource",
        upload_to_ckan=False,
        sync_timestamp=True,
        trigger_rule="one_success",
    )

    delete_records = DeleteDatastoreResourceRecordsOperator(
        task_id="delete_records",
        address=ckan_address,
        apikey=ckan_apikey,
        backup_task_id="backup_data",
    )

    insert_records = PythonOperator(
        task_id="insert_new_records", 
        python_callable=insert_new_records,
        trigger_rule="one_success",
    )

    #insert_records = InsertDatastoreResourceRecordsOperator(
    #    task_id="insert_records",
    #    address=ckan_address,
    #    apikey=ckan_apikey,
    #    parquet_filepath_task_id="transform_data",
    #    resource_task_id="get_or_create_resource",
    #    trigger_rule="one_success"
#
    #)


    #records_loaded_branch = BranchPythonOperator(
    #    task_id="were_records_loaded", python_callable=were_records_loaded,
    #)

    restore_backup = RestoreDatastoreResourceBackupOperator(
        task_id="restore_backup",
        address=ckan_address,
        apikey=ckan_apikey,
        backup_task_id="backup_data",
        trigger_rule="all_failed",
    )

    new_resource = DummyOperator(task_id = "new_resource")
    existing_resource = DummyOperator(task_id = "existing_resource")

    tmp_dir >> package >> src >> transformed_data >> get_or_create_resource >> new_resource_branch >> [new_resource, existing_resource]

    existing_resource >> backup_data >> delete_records >> insert_records >> delete_tmp_data >> sync_timestamp >> success_message_slack

    new_resource >> insert_records >> sync_timestamp >> success_message_slack

    [delete_records, insert_records] >> restore_backup

