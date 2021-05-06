import hashlib
import json
import logging
import os
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from ckan_plugin.operators.datastore_operator import (
    BackupDatastoreResourceOperator,
    DeleteDatastoreResourceRecordsOperator,
    InsertDatastoreResourceRecordsOperator,
)
from ckan_plugin.operators.package_operator import GetPackageOperator
from ckan_plugin.operators.resource_operator import (
    GetOrCreateResourceOperator,
    ResourceAndFileOperator,
)
from dateutil import parser
from utils import agol_utils, airflow_utils
from utils_plugin.operators.directory_operator import CreateLocalDirectoryOperator
from utils_plugin.operators.file_operator import DownloadFileOperator

RESOURCE_NAME = "EarlyON Child and Family Centres"
SRC_URL = "http://opendata.toronto.ca/childrens.services/child-family-programs/earlyon.json"  # noqa: E501


def send_failure_message():
    airflow_utils.message_slack(
        name=Path(os.path.abspath(__file__)).name.replace(".py", ""),
        message_type="error",
        msg="Job not finished",
        active_env=Variable.get("active_env"),
        prod_webhook=Variable.get("active_env") == "prod",
    )


with DAG(
    Path(os.path.abspath(__file__)).name.replace(".py", ""),
    default_args=airflow_utils.get_default_args(
        {
            "on_failure_callback": send_failure_message,
            "start_date": days_ago(1),
            "retries": 0,
            # "retry_delay": timedelta(minutes=3),
        }
    ),
    description="Take earlyon.json from opendata.toronto.ca and put into datastore",
    schedule_interval="0 17 * * *",
    catchup=False,
    tags=["dataset"],
) as dag:

    def is_resource_new(**kwargs):
        package = kwargs["ti"].xcom_pull(task_ids="get_package")
        logging.info(f"resources found: {[r['name'] for r in package['resources']]}")
        is_new = RESOURCE_NAME not in [r["name"] for r in package["resources"]]

        if is_new:
            return "create_data_dictionary"

        return "backup_data"

    def build_data_dict(**kwargs):
        data_fp = Path(kwargs["ti"].xcom_pull(task_ids="transform_data"))
        data = pd.read_parquet(Path(data_fp))

        fields = []
        for field, dtype in data.dtypes.iteritems():
            ckan_type_map = {"int64": "int", "object": "text", "float64": "float"}
            fields.append({"type": ckan_type_map[dtype.name], "id": field})

        return fields

    def transform_data(**kwargs):
        ti = kwargs["ti"]
        data_file_info = ti.xcom_pull(task_ids="get_data")
        tmp_dir = Path(ti.xcom_pull(task_ids="tmp_dir"))

        with open(Path(data_file_info["path"])) as f:
            src_file = json.load(f)

        logging.info(f"tmp_dir: {tmp_dir} | data_file_info: {data_file_info}")

        data = pd.DataFrame(src_file)

        data["geometry"] = data.apply(
            lambda x: json.dumps(
                {"type": "Point", "coordinates": [x["long"], x["lat"]]}
            )
            if x["long"] and x["lat"]
            else "",
            axis=1,
        )

        data = agol_utils.remove_geo_columns(data)

        filepath = tmp_dir / "new_data_transformed.parquet"

        data.to_parquet(path=filepath, engine="fastparquet", compression=None)

        return filepath

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
            return "file_is_new"
            return "file_is_not_new"

        return "file_is_new"

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
        checksum.update(df.sort_values(by="loc_id").to_csv(index=False).encode("utf-8"))
        checksum = checksum.hexdigest()

        for f in os.listdir(backups_dir):
            if not os.path.isfile(backups_dir / f):
                continue

            logging.info(f"File in backups: {f}")
            if os.path.isfile(backups_dir / f) and checksum in f:
                logging.info(f"Data is already backed up, ID: {checksum}")
                return "data_is_not_new"

        logging.info(f"Data is not yet in backups, new ID: {checksum}")

        return "data_is_new"

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
            dag.dag_id,
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
        task_id="tmp_dir", path=Path(Variable.get("tmp_dir")) / dag.dag_id,
    )

    backups_dir = CreateLocalDirectoryOperator(
        task_id="backups_dir", path=Path(Variable.get("backups_dir")) / dag.dag_id,
    )

    src = DownloadFileOperator(
        task_id="get_data",
        file_url=SRC_URL,
        dir_task_id="tmp_dir",
        filename="src_data.json",
    )

    package = GetPackageOperator(
        task_id="get_package",
        address=ckan_address,
        apikey=ckan_apikey,
        package_name_or_id=dag.dag_id,
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
        package_name_or_id=dag.dag_id,
        resource_name=RESOURCE_NAME,
        resource_attributes=dict(
            format="geojson",
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {dag.dag_id}",
        ),
        trigger_rule="none_failed",
    )

    backup_data = BackupDatastoreResourceOperator(
        task_id="backup_data",
        address=ckan_address,
        apikey=ckan_apikey,
        resource_task_id="get_or_create_resource",
        dir_task_id="backups_dir",
        sort_columns=["loc_id"],
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
        op_kwargs={"dag_id": dag.dag_id, "recursively": True},
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
            dag.dag_id,
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
            dag.dag_id,
            "Updated resource last_modified time only: new file but no new data",
            "success",
            active_env == "prod",
            active_env,
        ),
    )

    records_loaded_branch = BranchPythonOperator(
        task_id="were_records_loaded", python_callable=were_records_loaded,
    )

    backups_dir >> backup_data

    tmp_dir >> src >> transformed_data >> file_new_branch

    package >> get_or_create_resource >> [file_new_branch, new_resource_branch]

    new_resource_branch >> create_data_dictionary >> fields

    new_resource_branch >> backup_data >> fields

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
