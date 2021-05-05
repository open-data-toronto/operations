import json
import logging
import os
from pathlib import Path

import ckanapi
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
from utils import agol_utils, airflow_utils, ckan_utils
from utils_plugin.operators.directory_operator import CreateLocalDirectoryOperator
from utils_plugin.operators.file_operator import DownloadFileOperator

PACKAGE_ID = Path(os.path.abspath(__file__)).name.replace(".py", "")
RESOURCE_NAME = "EarlyON Child and Family Centres"
SRC_URL = "http://opendata.toronto.ca/childrens.services/child-family-programs/earlyon.json"  # noqa: E501

ACTIVE_ENV = Variable.get("active_env")


def send_failure_message():
    airflow_utils.message_slack(
        name=PACKAGE_ID,
        message_type="error",
        msg="Job not finished",
        active_env=ACTIVE_ENV,
        prod_webhook=ACTIVE_ENV == "prod",
    )


with DAG(
    PACKAGE_ID,
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
    ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
    CKAN = ckanapi.RemoteCKAN(**ckan_creds[ACTIVE_ENV])

    def is_resource_new(**kwargs):
        package = kwargs["ti"].xcom_pull(task_ids="get_package")
        logging.info(f"resources found: {[r['name'] for r in package['resources']]}")
        is_new = RESOURCE_NAME not in [r["name"] for r in package["resources"]]

        if is_new:
            return "resource_is_new"

        return "resource_is_not_new"

    @dag.task()
    def build_data_dict(data_fp):
        data = pd.read_parquet(Path(data_fp))

        fields = []
        for field, dtype in data.dtypes.iteritems():
            ckan_type_map = {"int64": "int", "object": "text", "float64": "float"}
            fields.append({"type": ckan_type_map[dtype.name], "id": field})

        return fields

    @dag.task()
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

        last_modified_string = data_file_info["file_last_modified"]
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

    def is_data_new(**kwargs):
        # ti = kwargs["ti"]
        # backups_dir = Path(ti.xcom_pull(task_ids="backups_dir"))

        # logging.info(f"checksum: {checksum}")
        # logging.info(f"backups_dir: {backups_dir}")

        # backups = Path(backups_dir)
        # for f in os.listdir(backups):
        #     if not os.path.isfile(backups / f):
        #         continue
        #     logging.info(f"File in backups: {f}")

        #     if os.path.isfile(backups / f) and checksum in f:
        #         logging.info(f"Data has already been loaded, ID: {checksum}")
        #         return "data_is_not_new"

        # logging.info(f"Data has not been loaded, new ID: {checksum}")
        return "data_is_new"

    @dag.task(trigger_rule="none_failed")
    def get_fields(fields, **kwargs):
        backup_data = Path(kwargs["ti"].xcom_pull(task_ids="backup_data"))

        if backup_data is not None:
            with open(Path(backup_data["fields"]), "r") as f:
                fields = json.load(f)

        return fields

    @dag.task()
    def update_resource_last_modified(resource, source_file):
        return ckan_utils.update_resource_last_modified(
            ckan=CKAN,
            resource_id=resource["id"],
            new_last_modified=parser.parse(source_file["file_last_modified"]),
        )

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
        address=ckan_creds[ACTIVE_ENV]["address"],
        apikey=ckan_creds[ACTIVE_ENV]["apikey"],
        package_name_or_id=dag.dag_id,
    )

    new_resource_branch = BranchPythonOperator(
        task_id="new_resource_branch", python_callable=is_resource_new,
    )

    transformed_data = transform_data()

    # checksum = make_checksum(transformed_data)

    data_dict = build_data_dict(transformed_data)

    get_or_create_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_resource",
        address=ckan_creds[ACTIVE_ENV]["address"],
        apikey=ckan_creds[ACTIVE_ENV]["apikey"],
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
        address=ckan_creds[ACTIVE_ENV]["address"],
        apikey=ckan_creds[ACTIVE_ENV]["apikey"],
        resource_task_id="get_or_create_resource",
        dir_task_id="backups_dir",
    )

    fields = get_fields(data_dict)

    file_new_branch = BranchPythonOperator(
        task_id="file_new_branch", python_callable=is_file_new,
    )

    new_data_branch = BranchPythonOperator(
        task_id="new_data_branch", python_callable=is_data_new,
    )

    delete_tmp_data = PythonOperator(
        task_id="delete_tmp_data",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": dag.dag_id, "recursively": True},
        trigger_rule="one_success",
    )

    sync_timestamp = ResourceAndFileOperator(
        task_id="sync_timestamp",
        address=ckan_creds[ACTIVE_ENV]["address"],
        apikey=ckan_creds[ACTIVE_ENV]["apikey"],
        download_file_task_id="get_data",
        resource_task_id="get_or_create_resource",
        upload_to_ckan=False,
        sync_timestamp=True,
    )

    send_nothing_notification = PythonOperator(
        task_id="send_nothing_notification",
        python_callable=airflow_utils.message_slack,
        op_args=(
            dag.dag_id,
            "No new data file",
            "success",
            ACTIVE_ENV == "prod",
            ACTIVE_ENV,
        ),
    )

    delete_records = DeleteDatastoreResourceRecordsOperator(
        task_id="delete_records",
        address=ckan_creds[ACTIVE_ENV]["address"],
        apikey=ckan_creds[ACTIVE_ENV]["apikey"],
        backup_task_id="backup_data",
    )

    insert_records = InsertDatastoreResourceRecordsOperator(
        task_id="insert_records",
        address=ckan_creds[ACTIVE_ENV]["address"],
        apikey=ckan_creds[ACTIVE_ENV]["apikey"],
        parquet_filepath_task_id="transform_data",
        resource_task_id="get_or_create_resource",
    )

    new_records_notification = PythonOperator(
        task_id="new_records_notification",
        python_callable=airflow_utils.message_slack,
        op_args=(
            dag.dag_id,
            "Refreshed records",
            "success",
            ACTIVE_ENV == "prod",
            ACTIVE_ENV,
        ),
    )

    no_new_data_notification = PythonOperator(
        task_id="no_new_data_notification",
        python_callable=airflow_utils.message_slack,
        op_args=(
            dag.dag_id,
            "Updated resource last_modified time only: new file but no new data",
            "success",
            ACTIVE_ENV == "prod",
            ACTIVE_ENV,
        ),
    )

    backups_dir >> backup_data

    tmp_dir >> src >> [file_new_branch, transformed_data]

    package >> get_or_create_resource >> new_resource_branch

    new_resource_branch >> DummyOperator(
        task_id="resource_is_new"
    ) >> data_dict >> fields

    new_resource_branch >> DummyOperator(
        task_id="resource_is_not_new"
    ) >> backup_data >> fields

    file_new_branch >> DummyOperator(task_id="file_is_new") >> [
        new_data_branch,
        sync_timestamp,
    ]

    file_new_branch >> DummyOperator(
        task_id="file_is_not_new"
    ) >> send_nothing_notification

    fields >> new_data_branch

    new_data_branch >> DummyOperator(
        task_id="data_is_new"
    ) >> delete_records >> insert_records >> new_records_notification

    new_data_branch >> DummyOperator(
        task_id="data_is_not_new"
    ) >> no_new_data_notification

    sync_timestamp >> [new_records_notification, no_new_data_notification]

    [
        no_new_data_notification,
        new_records_notification,
        send_nothing_notification,
    ] >> delete_tmp_data
