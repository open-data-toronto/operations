import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import ckanapi
import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from utils import airflow_utils, ckan_utils

PACKAGE_NAME = "covid-19-cases-in-toronto"
SOURCE_FILE_NAME = "covid19cases.csv"


def send_failure_msg():
    airflow_utils.message_slack(
        name=PACKAGE_NAME,
        message_type="error",
        msg="Job not finished",
        active_env=Variable.get("active_env"),
        prod_webhook=Variable.get("active_env") == "prod",
    )


with DAG(
    PACKAGE_NAME,
    default_args=airflow_utils.get_default_args(
        {
            "on_failure_callback": send_failure_msg,
            "start_date": datetime(2020, 11, 24, 13, 35, 0),
            "retries": 3,
            "retry_delay": timedelta(minutes=3),
        }
    ),
    description="Take COVID19 data from QA (filestore) and put in PROD (datastore)", 
    schedule_interval="59 14 * * 3",
    tags=["dataset", "priority"],
    catchup=False,
) as dag:

    def send_success_msg(**kwargs):
        # messages slack with this task's name and the environment it was run in
        ti = kwargs.pop("ti")
        msg_task_id = ti.xcom_pull(task_ids="msg_task_id")
        msg = ti.xcom_pull(task_ids=msg_task_id)

        airflow_utils.message_slack(
            name=PACKAGE_NAME,
            message_type="success",
            msg=msg,
            active_env=Variable.get("active_env"),
            prod_webhook=Variable.get("active_env") == "prod",
        )

    
    def backup_old_data(**kwargs):
        # make a copy of current datastore resource in a parquet file

        # find the backup folder, based on the environment's airflow variables
        ti = kwargs.pop("ti")
        package = ti.xcom_pull(task_ids="get_target_package") 
        backups = Path(Variable.get("backups_dir")) / PACKAGE_NAME

        # gets the number of records (for a datastore resource) 
        resource = package["resources"][0]
        record_count = TARGET_CKAN.action.datastore_search(id=resource["id"], limit=0)[
            "total"
        ]

        # get the datastore object's records and put them in memory
        datastore_response = TARGET_CKAN.action.datastore_search(
            id=resource["id"], limit=record_count
        )

        data = pd.DataFrame(datastore_response["records"])

        # get rid of the first row 
        if "_id" in data.columns.values:
            data = data.drop("_id", axis=1)

        # hash the file
        data_hash = hashlib.md5()
        data_hash.update(
            data.sort_values(by="Assigned_ID").to_csv(index=False).encode("utf-8")
        )
        unique_id = data_hash.hexdigest()

        # store it in parquet (compressed file format)
        data_path = backups / f"data.{unique_id}.parquet"
        if not data_path.exists():
            data.to_parquet(path=data_path, engine="fastparquet", compression=None)

        # store the field names in a json
        fields = [f for f in datastore_response["fields"] if f["id"] != "_id"]

        fields_path = backups / f"fields.{unique_id}.json"
        if not fields_path.exists():
            with open(fields_path, "w") as f:
                json.dump(fields, f)

        # return where the data and fields are, and the number of columns and rows
        return {
            "fields": fields_path,
            "data": data_path,
            "records": data.shape[0],
            "columns": data.shape[1],
        }

    
    def get_new_data(**kwargs):
        # gets data from a package and stores it in a parquet file

        # make a tmp dir with the DAG's name
        ti = kwargs.pop("ti")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))

        # get the package object for COVID data from QA
        package = SOURCE_CKAN.action.package_show(id=PACKAGE_NAME)

        # get the first resource in the package for COVID
        resource = [r for r in package["resources"] if r["name"] == SOURCE_FILE_NAME][0]

        # get resource's data from the datastore
        file_content = requests.get(resource["url"]).content 
        data = pd.read_csv(BytesIO(file_content))

        # compress data into parquet in the dir made above and return its filepath
        filename = "new_data_raw"
        filepath = tmp_dir / f"{filename}.parquet"

        data.to_parquet(path=filepath, engine="fastparquet", compression=None)

        return filepath

    def get_target_package():

        # get a list of datastore resources for the covid19 cases package
        package = TARGET_CKAN.action.package_show(id=PACKAGE_NAME)

        datastore_resources = [
            r
            for r in package["resources"]
            if r["datastore_active"] or r["url_type"] == "datastore"
        ]

        # make sure there was only one datastore resource in the package and return the package
        assert (
            len(datastore_resources) == 1 and len(package["resources"]) == 1
        ), logging.error(
            "Expected 1 resource but got {} total and {} in the datastore".format(
                len(package["resources"]), len(datastore_resources)
            )
        )

        return package

    def prep_new_data(**kwargs):
        # reads data from "get_new_data", rewrites data formats to Y-m-d, and saves to parquet
        
        # put parquet file from the output of the "get_new_data" function in memory
        ti = kwargs.pop("ti")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))
        new_data_fp = Path(ti.xcom_pull(task_ids="get_new_data"))

        data = pd.read_parquet(new_data_fp)

        # parse date fields into Y-m-d
        for date_field in ["Episode Date", "Reported Date"]:
            data[date_field] = (pd.to_datetime(data[date_field])).dt.strftime(
                "%Y-%m-%d"
            )

        # write the data into parquet in the same tmp folder
        filename = "new_data_prepared"
        filepath = tmp_dir / f"{filename}.parquet"

        data.to_parquet(path=filepath, engine="fastparquet", compression=None)

        return filepath

    def get_new_data_unique_id(**kwargs):
        # sort data by Assigned_ID and return uncompressed, hashed values in a CSV
        ti = kwargs.pop("ti")
        data_fp = Path(ti.xcom_pull(task_ids="prep_new_data"))
        data = pd.read_parquet(data_fp).sort_values(by="Assigned_ID")

        data_hash = hashlib.md5()
        data_hash.update(data.to_csv(index=False).encode("utf-8"))

        return data_hash.hexdigest()

    def confirm_data_is_new(**kwargs):
        # Check to see if there are new records in the new data, otherwise indicates we dont need to run an update
        ti = kwargs.pop("ti")
        data_to_load_unique_id = ti.xcom_pull(task_ids="get_new_data_unique_id")
        backups = Path(Variable.get("backups_dir")) / PACKAGE_NAME

        for f in os.listdir(backups):
            if os.path.isfile(backups / f) and data_to_load_unique_id in f:
                logging.info(
                    f"Data has already been loaded, ID: {data_to_load_unique_id}"
                )
                return "build_nothing_to_load_msg"

        return "delete_old_records"

    def delete_old_records(**kwargs):
        # Deletes whole existing data source
        package = kwargs.pop("ti").xcom_pull(task_ids="get_target_package")
        resource = package["resources"][0]

        res = TARGET_CKAN.action.datastore_delete(id=resource["id"], filters={})

        return res

    def insert_new_records(**kwargs):
        # inserts records from get_target_package into the old resource 
        ti = kwargs.pop("ti")
        package = ti.xcom_pull(task_ids="get_target_package")
        resource = package["resources"][0]

        data_fp = Path(ti.xcom_pull(task_ids="prep_new_data"))
        data = pd.read_parquet(data_fp).sort_values(by="Assigned_ID")
        records = data.to_dict(orient="records")

        res = TARGET_CKAN.action.datastore_create(id=resource["id"], records=records)

        return res

    def build_message(**kwargs):
        # build the success / failure message for this DAG run
        ti = kwargs.pop("ti")
        unique_id = ti.xcom_pull(task_ids="get_new_data_unique_id")

        if "already_loaded" in kwargs:
            return (
                f"Data is not new, UID of backup files: {unique_id}. Nothing to load."
            )

        backup_details = ti.xcom_pull(task_ids="backup_old_data")
        previous_data_records = backup_details["records"] 

        new_data_fp = ti.xcom_pull(task_ids="prep_new_data")
        new_data = pd.read_parquet(new_data_fp)

        return "COVID data refreshed: from {} to {} cases".format(
            previous_data_records, new_data.shape[0]
        )

    def delete_source_resource(**kwargs):
        package = SOURCE_CKAN.action.package_show(id=PACKAGE_NAME)
        resource = [r for r in package["resources"] if r["name"] == SOURCE_FILE_NAME][0]
        res = SOURCE_CKAN.action.resource_delete(id=resource["id"])
        logging.info(res)

    def update_resource_last_modified(**kwargs):
        ti = kwargs.pop("ti")
        package = ti.xcom_pull(task_ids="get_target_package")
        resource = package["resources"][0]

        res = ckan_utils.update_resource_last_modified(
            ckan=TARGET_CKAN,
            resource_id=resource["id"],
            new_last_modified=datetime.now(),
        )

        return res

    ACTIVE_ENV = Variable.get("active_env")
    CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
    TARGET_CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])
    SOURCE_CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS["qa"])

    create_tmp_dir = PythonOperator(
        task_id="create_tmp_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": PACKAGE_NAME, "dir_variable_name": "tmp_dir"},
    )

    create_backups_dir = PythonOperator(
        task_id="create_backups_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": PACKAGE_NAME, "dir_variable_name": "backups_dir"},
    )

    source_data = PythonOperator(task_id="get_new_data", python_callable=get_new_data,)

    target_package = PythonOperator(
        task_id="get_target_package", python_callable=get_target_package,
    )

    old_data = PythonOperator(
        task_id="backup_old_data", python_callable=backup_old_data,
    )

    prepare_data = PythonOperator(
        task_id="prep_new_data", python_callable=prep_new_data,
    )

    new_data_unique_id = PythonOperator(
        task_id="get_new_data_unique_id", python_callable=get_new_data_unique_id,
    )

    data_is_new = BranchPythonOperator(
        task_id="confirm_data_is_new", python_callable=confirm_data_is_new,
    )

    delete_old = PythonOperator(
        task_id="delete_old_records", python_callable=delete_old_records,
    )

    insert_new = PythonOperator(
        task_id="insert_new_records", python_callable=insert_new_records,
    )

    loaded_msg = PythonOperator(
        task_id="build_loaded_msg", python_callable=build_message,
    )

    nothing_to_load_msg = PythonOperator(
        task_id="build_nothing_to_load_msg",
        python_callable=build_message,
        op_kwargs={"already_loaded": True},
    )

    send_loaded_notification = PythonOperator(
        task_id="send_success_msg",
        python_callable=send_success_msg,
        op_kwargs={"msg_task_id": "build_loaded_msg"},
    )

    send_nothing_to_load_msg = PythonOperator(
        task_id="send_nothing_to_load_msg",
        python_callable=send_success_msg,
        op_kwargs={"msg_task_id": "build_nothing_to_load_msg"},
    )

    delete_tmp_files = PythonOperator(
        task_id="delete_tmp_files",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["get_new_data", "prep_new_data"]},
        trigger_rule="one_success",
    )

    delete_tmp_dir = PythonOperator(
        task_id="delete_tmp_dir",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": PACKAGE_NAME},
    )

    delete_source = PythonOperator(
        task_id="delete_source_resource", python_callable=delete_source_resource,
    )

    begin_cleanup = DummyOperator(task_id="begin_cleanup", trigger_rule="none_failed",)

    update_timestamp = PythonOperator(
        task_id="update_resource_last_modified",
        python_callable=update_resource_last_modified,
    )


    create_tmp_dir >> source_data >> prepare_data >> new_data_unique_id >> data_is_new

    old_data >> data_is_new

    delete_old >> insert_new >> update_timestamp >> loaded_msg

    data_is_new >> delete_old

    target_package >> create_backups_dir >> old_data

    loaded_msg >> send_loaded_notification

    data_is_new >> nothing_to_load_msg >> send_nothing_to_load_msg

    [send_nothing_to_load_msg, send_loaded_notification] >> begin_cleanup

    begin_cleanup >> delete_tmp_files >> delete_tmp_dir

    begin_cleanup >> delete_source
