import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path

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
from utils_operators.directory_operator import CreateLocalDirectoryOperator
from utils_operators.file_operators import DownloadFileOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator


SRC_URL = "http://opendata.toronto.ca/childrens.services/licensed-child-care-centres/child-care.csv"  # noqa: E501
PACKAGE_NAME = "licensed-child-care-centres-old"
RESOURCE_NAME = "Child care centres"

EXPECTED_COLUMNS = [
    "LOC_ID",
    "LOC_NAME",
    "AUSPICE",
    "ADDRESS",
    "PCODE",
    "ward",
    "PHONE",
    "bldg_type",
    "BLDGNAME",
    "IGSPACE",
    "TGSPACE",
    "PGSPACE",
    "KGSPACE",
    "SGSPACE",
    "TOTSPACE",
    "subsidy",
    "run_date",
    "latitude",
    "longitude",
]


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
            "on_failure_callback": task_failure_slack_alert,
            "start_date": datetime(2020, 11, 24, 13, 35, 0),
            "retries": 0,
            "etl_mapping":[{
                "source": SRC_URL,
                "target_package_name": PACKAGE_NAME,
                "target_resource_name": RESOURCE_NAME
            }]
        }
    ),
    description="Take data from opendata.toronto.ca (CSV) and put into datastore",
    schedule_interval="0 17 * * *",
    catchup=False,
    tags=["dataset"],
) as dag:

    def is_resource_new(**kwargs):
        package = kwargs["ti"].xcom_pull(task_ids="get_package")
        logging.info(f"resources found: {[r['name'] for r in package['resources']]}")
        is_new = RESOURCE_NAME not in [r["name"] for r in package["resources"]]

        if is_new:
            return "resource_is_new"

        return "resource_is_not_new"

    def is_data_new(**kwargs):
        ti = kwargs["ti"]
        fields = ti.xcom_pull(task_ids="get_fields")
        if fields is not None:
            return "data_is_new"

        backups_dir = Path(ti.xcom_pull(task_ids="backups_dir"))
        backup_data = ti.xcom_pull(task_ids="backup_data")

        df = pd.read_parquet(backup_data["data"])

        if df.shape[0] == 0:
            return "data_is_new"

        checksum = hashlib.md5()
        checksum.update(df.sort_values(by="LOC_ID").to_csv(index=False).encode("utf-8"))
        checksum = checksum.hexdigest()

        for f in os.listdir(backups_dir):
            if not os.path.isfile(backups_dir / f):
                continue
            logging.info(f"File in backups: {f}")

            if os.path.isfile(backups_dir / f) and checksum in f:
                logging.info(f"Data has already been loaded, ID: {checksum}")
                return "data_is_not_new"

        logging.info(f"Data has not been loaded, new ID: {checksum}")

        return "data_is_new"

    def transform_data(**kwargs):
        ti = kwargs.pop("ti")
        tmp_dir = Path(ti.xcom_pull(task_ids="tmp_dir"))
        data_fp = Path(ti.xcom_pull(task_ids="get_data")["path"])

        data = pd.read_csv(data_fp)

        data["geometry"] = data.apply(
            lambda x: json.dumps(
                {"type": "Point", "coordinates": [x["longitude"], x["latitude"]]}
            )
            if x["longitude"] and x["latitude"]
            else "",
            axis=1,
        )

        data = agol_utils.remove_geo_columns(data)

        filename = "new_data_transformed"
        filepath = tmp_dir / f"{filename}.parquet"

        data.to_parquet(filepath, engine="fastparquet", compression=None)

        return filepath

    def validate_expected_columns(**kwargs):
        ti = kwargs["ti"]
        data_fp = Path(ti.xcom_pull(task_ids="get_data")["path"])

        df = pd.read_csv(data_fp)

        for col in df.columns.values:
            assert col in EXPECTED_COLUMNS, f"{col} not in list of expected columns"

        for col in EXPECTED_COLUMNS:
            assert col in df.columns.values, f"Expected column {col} not in data file"

    def is_file_new(**kwargs):
        ti = kwargs["ti"]
        data_file_info = ti.xcom_pull(task_ids="get_data")
        resource = ti.xcom_pull(task_ids="get_or_create_resource")

        logging.info(f"resource: {resource} | data_file_info: {data_file_info}")

        last_modified_string = data_file_info["last_modified"]
        file_last_modified = parser.parse(last_modified_string)
        last_modified_attr = resource["last_modified"]

        if not last_modified_attr:
            last_modified_attr = resource["created"]

        resource_last_modified = parser.parse(last_modified_attr + " UTC")

        difference_in_seconds = (
            file_last_modified.timestamp() - resource_last_modified.timestamp()
        )

        logging.info(
            f"{difference_in_seconds}secs between file and resource last modified times"
        )

        if difference_in_seconds == 0:
            return "file_is_not_new"

        return "file_is_new"

    def build_data_dict(**kwargs):
        data_fp = Path(kwargs["ti"].xcom_pull(task_ids="transform_data"))
        data = pd.read_parquet(Path(data_fp))

        fields = []

        for field, dtype in data.dtypes.iteritems():
            ckan_type_map = {"int64": "int", "object": "text", "float64": "float"}
            fields.append({"type": ckan_type_map[dtype.name], "id": field})

        return fields

    def build_message(**kwargs):
        ti = kwargs["id"]
        records_inserted = ti.xcom_pull("records_inserted")

        if records_inserted is None:
            resource = ti.xcom_pull("update_resource_last_modified")
            last_modified = parser.parse(resource["last_modified"]).strftime(
                "%Y-%m-%d %H:%M"
            )

            return (
                f"New file, no new data. New last modified timestamp: {last_modified}"
            )

        new_data_fp = Path(ti.xcom_pull("transform_data"))
        new_data = pd.read_parquet(new_data_fp)

        return f"Refreshed: {new_data.shape[0]} records"

    def get_fields(**kwargs):
        ti = kwargs["ti"]
        backup_data = ti.xcom_pull(task_ids="backup_data")

        if backup_data is not None:
            with open(Path(backup_data["fields_file_path"]), "r") as f:
                fields = json.load(f)
        else:
            fields = ti.xcom_pull(task_ids="create_data_dictionary")
            assert fields is not None, "No fields"

        return fields

    def were_records_loaded(**kwargs):
        inserted_records_count = kwargs["ti"].xcom_pull(task_ids="insert_records")

        if inserted_records_count is not None and inserted_records_count > 0:
            return "new_records_notification"

        return "no_new_data_notification"

    def send_new_records_notification(**kwargs):
        count = kwargs["ti"].xcom_pull("insert_records")

        airflow_utils.message_slack(
            PACKAGE_NAME,
            f"Refreshed {count} records",
            "success",
            Variable.get("active_env") == "prod",
            Variable.get("active_env"),
        )

    ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
    active_env = Variable.get("active_env")
    ckan_address = ckan_creds[active_env]["address"]
    ckan_apikey = ckan_creds[active_env]["apikey"]

    tmp_dir = CreateLocalDirectoryOperator(
        task_id="tmp_dir", path=Path(Variable.get("tmp_dir")) / PACKAGE_NAME,
    )

    backups_dir = CreateLocalDirectoryOperator(
        task_id="backups_dir", path=Path(Variable.get("backups_dir")) / PACKAGE_NAME,
    )

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

    new_resource_branch = BranchPythonOperator(
        task_id="new_resource_branch", python_callable=is_resource_new,
    )

    transformed_data = PythonOperator(
        task_id="transform_data", python_callable=transform_data,
    )

    create_data_dictionary = PythonOperator(
        task_id="create_data_dictionary", python_callable=build_data_dict,
    )

    get_or_create_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_resource",
        address=ckan_address,
        apikey=ckan_apikey,
        package_name_or_id=PACKAGE_NAME,
        resource_name=RESOURCE_NAME,
        resource_attributes=dict(
            format="geojson",
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {PACKAGE_NAME}",
        ),
    )

    backup_data = BackupDatastoreResourceOperator(
        task_id="backup_data",
        address=ckan_address,
        apikey=ckan_apikey,
        resource_task_id="get_or_create_resource",
        dir_task_id="backups_dir",
        sort_columns=["LOC_ID"],
    )

    fields = PythonOperator(
        task_id="get_fields", python_callable=get_fields, trigger_rule="none_failed"
    )

    file_new_branch = BranchPythonOperator(
        task_id="file_new_branch", python_callable=is_file_new,
    )

    new_data_branch = BranchPythonOperator(
        task_id="is_data_new", python_callable=is_data_new,
    )

    delete_tmp_data = PythonOperator(
        task_id="delete_tmp_data",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": PACKAGE_NAME, "recursively": True},
        trigger_rule="one_success",
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

    send_nothing_notification = PythonOperator(
        task_id="send_nothing_notification",
        python_callable=airflow_utils.message_slack,
        op_args=(
            PACKAGE_NAME,
            "No new data file",
            "success",
            active_env == "prod",
            active_env,
        ),
    )

    delete_records = DeleteDatastoreResourceRecordsOperator(
        task_id="delete_records",
        address=ckan_address,
        apikey=ckan_apikey,
        backup_task_id="backup_data",
    )

    insert_records = InsertDatastoreResourceRecordsOperator(
        task_id="insert_records",
        address=ckan_address,
        apikey=ckan_apikey,
        parquet_filepath_task_id="transform_data",
        resource_task_id="get_or_create_resource",
    )

    new_records_notification = PythonOperator(
        task_id="new_records_notification",
        python_callable=send_new_records_notification,
    )

    no_new_data_notification = PythonOperator(
        task_id="no_new_data_notification",
        python_callable=airflow_utils.message_slack,
        op_args=(
            PACKAGE_NAME,
            "Updated resource last_modified time only: new file but no new data",
            "success",
            active_env == "prod",
            active_env,
        ),
    )

    records_loaded_branch = BranchPythonOperator(
        task_id="were_records_loaded", python_callable=were_records_loaded,
    )

    restore_backup = RestoreDatastoreResourceBackupOperator(
        task_id="restore_backup",
        address=ckan_address,
        apikey=ckan_apikey,
        backup_task_id="backup_data",
        trigger_rule="all_failed",
    )

    validated_columns = PythonOperator(
        task_id="validate_expected_columns", python_callable=validate_expected_columns,
    )

    backups_dir >> backup_data

    tmp_dir >> src >> validated_columns >> transformed_data >> file_new_branch

    package >> get_or_create_resource >> [file_new_branch, new_resource_branch]

    new_resource_branch >> DummyOperator(
        task_id="resource_is_new"
    ) >> create_data_dictionary >> fields

    new_resource_branch >> DummyOperator(
        task_id="resource_is_not_new"
    ) >> backup_data >> fields

    file_new_branch >> DummyOperator(task_id="file_is_new") >> new_data_branch

    file_new_branch >> DummyOperator(
        task_id="file_is_not_new"
    ) >> send_nothing_notification

    fields >> new_data_branch

    new_data_branch >> DummyOperator(
        task_id="data_is_new"
    ) >> delete_records >> insert_records >> sync_timestamp

    new_data_branch >> DummyOperator(task_id="data_is_not_new") >> sync_timestamp

    sync_timestamp >> records_loaded_branch

    records_loaded_branch >> new_records_notification

    records_loaded_branch >> no_new_data_notification

    [
        no_new_data_notification,
        new_records_notification,
        send_nothing_notification,
    ] >> delete_tmp_data

    [delete_records, insert_records] >> restore_backup
