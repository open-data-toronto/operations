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

# from airflow.utils.task_group import TaskGroup
from ckan_operators.datastore_operator import (
    BackupDatastoreResourceOperator,
    DeleteDatastoreResourceRecordsOperator,
    InsertDatastoreResourceRecordsOperator,
)
from ckan_operators.package_operator import GetPackageOperator
from ckan_operators.resource_operator import (
    GetOrCreateResourceOperator,
    ResourceAndFileOperator,
)
from dateutil import parser
from utils import airflow_utils
from utils_operators.directory_operator import (
    CreateLocalDirectoryOperator,
    DeleteLocalDirectoryOperator,
)
from utils_operators.file_operator import DownloadFileOperator

PACKAGE_NAME = "fatal-and-suspected-non-fatal-opioid-overdoses-in-the-shelter-system"

RESOURCES = {
    "summary-suspected-opiod-overdoses-in-shelters": {
        "file_url": "http://opendata.toronto.ca/shelter.support.housing.administration/fatal-and-non-fatal-suspected-opioid-overdoses-in-the-shelter-system/summary-suspected-opiod-overdoses-in-shelters.csv",  # noqa: E501
        "name": "summary-suspected-opiod-overdoses-in-shelters",
        "metadata": {
            "format": "csv",
            "is_preview": False,
            "extract_job": f"Airflow: {PACKAGE_NAME}",
        },
        "datastore": True,
        "expected_columns": [
            "year",
            "year_stage",
            "suspected_non_fatal_overdoses_incidents",
            "fatal_overdoses_incident",
        ],
    },
    "suspected-opiod-overdoses-in-shelters": {
        "file_url": "http://opendata.toronto.ca/shelter.support.housing.administration/fatal-and-non-fatal-suspected-opioid-overdoses-in-the-shelter-system/suspected-opiod-overdoses-in-shelters.csv",  # noqa: E501
        "name": "suspected-opiod-overdoses-in-shelters",
        "metadata": {
            "format": "csv",
            "is_preview": True,
            "extract_job": f"Airflow: {PACKAGE_NAME}",
        },
        "datastore": True,
        "expected_columns": [
            "site_name",
            "address",
            "year",
            "year_stage",
            "suspected_non_fatal_overdoses",
        ],
    },
}


def send_failure_message():
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
        resource_name = kwargs["resource_name"]

        logging.info(f"resources found: {[r['name'] for r in package['resources']]}")
        is_new = resource_name not in [r["name"] for r in package["resources"]]

        prefix = "summary" if "summary" in resource_name else "granular"

        if is_new:
            return f"{prefix}_resource_is_new"

        return f"{prefix}_resource_is_not_new"

    def validate_expected_columns(**kwargs):
        ti = kwargs["ti"]
        data_file_info = ti.xcom_pull(task_ids=kwargs["download_file_task_id"])

        df = pd.read_csv(Path(data_file_info["path"]))

        for col in [x.lower() for x in df.columns.values.tolist()]:
            assert (
                col in kwargs["expected_columns"]
            ), f"{col} not in list of expected columns"

        for col in kwargs["expected_columns"]:
            assert col in [
                x.lower() for x in df.columns.values.tolist()
            ], f"Expected column {col} not in data file"

    def transform_data(**kwargs):
        ti = kwargs["ti"]
        data_file_info = ti.xcom_pull(task_ids=kwargs["download_file_task_id"])
        tmp_dir = Path(ti.xcom_pull(task_ids="tmp_dir"))

        df = pd.read_csv(Path(data_file_info["path"]))
        df = df.rename(columns={col: col.lower() for col in df.columns.values})
        df["year"] = df["year"].astype(str)

        filepath = tmp_dir / f"{kwargs['resource_name']}_transformed.parquet"

        df.to_parquet(path=filepath, engine="fastparquet", compression=None)

        return filepath

        return "resource_is_not_new"

    def build_data_dict(**kwargs):
        ti = kwargs["ti"]
        data_fp = Path(ti.xcom_pull(task_ids="transform_data_task_id"))
        tmp_dir = Path(ti.xcom_pull(task_ids="tmp_dir"))

        data = pd.read_parquet(Path(data_fp))
        fields_file_name = kwargs["fields_file_name"]
        fields = []

        for col, col_type in data.dtypes.items():
            col_type = col_type.name.lower()

            if col_type == "object" or col.lower() == "year":
                field_type = "text"
            elif col_type == "int64":
                field_type = "int"
            elif col_type == "float64":
                field_type = "float"

            fields.append({"type": field_type, "id": col})

        fields_path = tmp_dir / f"{fields_file_name}.json"
        if not fields_path.exists():
            with open(fields_path, "w") as f:
                json.dump(fields, f)

        return fields_path

    def is_data_new(**kwargs):
        ti = kwargs["ti"]
        backups_dir = Path(ti.xcom_pull(task_ids="backups_dir"))
        backup_data = ti.xcom_pull(task_ids=kwargs["backup_data_task_id"])
        data_file_info = ti.xcom_pull(task_ids=kwargs["download_file_task_id"])
        resource = ti.xcom_pull(task_ids=kwargs["resource_task_id"])
        prefix = "summary" if "summary" in kwargs["resource_name"] else "granular"

        last_modified_string = data_file_info["last_modified"]
        file_last_modified = parser.parse(last_modified_string)
        last_modified_attr = resource["last_modified"]

        if not last_modified_attr:
            last_modified_attr = resource["created"]

        resource_last_modified = parser.parse(last_modified_attr + " UTC")

        difference_in_seconds = (
            file_last_modified.timestamp() - resource_last_modified.timestamp()
        )
        if difference_in_seconds == 0:
            return f"{prefix}_file_is_not_new"

        df = pd.read_parquet(backup_data["data"])
        if df.shape[0] == 0:
            return f"{prefix}_data_is_new"

        checksum = hashlib.md5()
        checksum.update(df.to_csv(index=False).encode("utf-8"))
        checksum = checksum.hexdigest()

        for f in os.listdir(backups_dir):
            if not os.path.isfile(backups_dir / f):
                continue

            logging.info(f"File in backups: {f}")
            if os.path.isfile(backups_dir / f) and checksum in f:
                logging.info(f"Data is already backed up, ID: {checksum}")
                return f"{prefix}_data_is_not_new"

        logging.info(f"Data is not yet in backups, new ID: {checksum}")

        return f"{prefix}_data_is_new"

    def build_notification_message(**kwargs):
        ti = kwargs.pop("ti")
        summary = kwargs.pop("summary_resource")
        granular = kwargs.pop("granular_resource")

        lines = []

        # summary
        if "is_not_new" in ti.xcom_pull(task_ids="is_summary_file_new"):
            lines.append(f"- `{summary['name']}`: no new file")
        else:
            summary_sync_ts = ti.xcom_pull(task_ids="sync_summary_ts")
            last_modified = summary_sync_ts["file_last_modified"]
            line = f"- `{summary['name']}` last_modified synced to: {last_modified}"

            if "is_new" in ti.xcom_pull(task_ids="is_summary_data_new"):
                summary_insert_count = ti.xcom_pull(task_ids="insert_summary_rows")
                line = line + f" | records inserted: {summary_insert_count}"

            lines.append(line)

        # granular
        if "is_not_new" in ti.xcom_pull(task_ids="is_granular_file_new"):
            lines.append(f"- `{granular['name']}`: no new file")
        else:
            granular_sync_ts = ti.xcom_pull(task_ids="sync_granular_ts")
            last_modified = granular_sync_ts["file_last_modified"]
            line = f"- `{granular['name']}` last_modified synced to: {last_modified}"

            if "is_new" in ti.xcom_pull(task_ids="is_granular_data_new"):
                granular_insert_count = ti.xcom_pull(task_ids="insert_granular_rows")
                line = line + f" | records inserted: {granular_insert_count}"

            lines.append(line)

        msg = "\n".join(lines)

        logging.info(f"Message to send: {msg}")

        return msg

    def send_notification_message(**kwargs):
        msg = kwargs["ti"].xcom_pull(task_ids="build_message")
        logging.info(f"Sending message: {msg}")

        airflow_utils.message_slack(
            PACKAGE_NAME,
            msg,
            "success",
            Variable.get("active_env") == "prod",
            Variable.get("active_env"),
        )

    ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
    active_env = Variable.get("active_env")
    ckan_address = ckan_creds[active_env]["address"]
    ckan_apikey = ckan_creds[active_env]["apikey"]
    summary_resource = RESOURCES.pop("summary-suspected-opiod-overdoses-in-shelters")
    granular_resource = RESOURCES.pop("suspected-opiod-overdoses-in-shelters")

    # create directories
    tmp_dir = CreateLocalDirectoryOperator(
        task_id="tmp_dir", path=Path(Variable.get("tmp_dir")) / PACKAGE_NAME,
    )

    backups_dir = CreateLocalDirectoryOperator(
        task_id="backups_dir", path=Path(Variable.get("backups_dir")) / PACKAGE_NAME,
    )

    # download data files
    get_summary_data = DownloadFileOperator(
        task_id="get_summary_data",
        file_url=summary_resource["file_url"],
        dir_task_id="tmp_dir",
        filename=f"{summary_resource['name']}_data.csv",
    )

    get_granular_data = DownloadFileOperator(
        task_id="get_granular_data",
        file_url=granular_resource["file_url"],
        dir_task_id="tmp_dir",
        filename=f"{granular_resource['name']}_data.csv",
    )

    # validate columns
    validate_summary_expected_columns = PythonOperator(
        task_id="validate_summary_expected_columns",
        python_callable=validate_expected_columns,
        op_kwargs={
            "download_file_task_id": "get_summary_data",
            "expected_columns": summary_resource["expected_columns"],
        },
    )

    validate_granular_expected_columns = PythonOperator(
        task_id="validate_granular_expected_columns",
        python_callable=validate_expected_columns,
        op_kwargs={
            "download_file_task_id": "get_granular_data",
            "expected_columns": granular_resource["expected_columns"],
        },
    )

    # transform data
    transform_summary_data = PythonOperator(
        task_id="transform_summary_data",
        python_callable=transform_data,
        op_kwargs={
            "download_file_task_id": "get_granular_data",
            "resource_name": summary_resource["name"],
        },
    )

    transform_granular_data = PythonOperator(
        task_id="transform_granular_data",
        python_callable=transform_data,
        op_kwargs={
            "download_file_task_id": "get_granular_data",
            "resource_name": granular_resource["name"],
        },
    )

    # create data dictionaries
    make_summary_data_dict = PythonOperator(
        task_id="make_summary_data_dict",
        python_callable=build_data_dict,
        op_kwargs={
            "transform_data_task_id": "transform_summary_data",
            "fields_file_name": "summary_generated_fields",
        },
    )

    make_granular_data_dict = PythonOperator(
        task_id="make_granular_data_dict",
        python_callable=build_data_dict,
        op_kwargs={
            "transform_data_task_id": "transform_granular_data",
            "fields_file_name": "granular_generated_fields",
        },
    )

    # create empty datastore resource with dictionary
    insert_summary_data_dict = InsertDatastoreResourceRecordsOperator(
        task_id="insert_summary_data_dict",
        address=ckan_address,
        apikey=ckan_apikey,
        fields_json_path_task_id="make_summary_data_dict",
        parquet_filepath_task_id=None,
        resource_task_id="get_or_create_summary_resource",
    )

    insert_granular_data_dict = InsertDatastoreResourceRecordsOperator(
        task_id="insert_granular_data_dict",
        address=ckan_address,
        apikey=ckan_apikey,
        fields_json_path_task_id="make_granular_data_dict",
        parquet_filepath_task_id=None,
        resource_task_id="get_or_create_granular_resource",
    )

    # get package and get/create resources
    get_package = GetPackageOperator(
        task_id="get_package",
        address=ckan_address,
        apikey=ckan_apikey,
        package_name_or_id=PACKAGE_NAME,
    )

    get_or_create_summary_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_summary_resource",
        address=ckan_address,
        apikey=ckan_apikey,
        package_name_or_id=PACKAGE_NAME,
        resource_name=summary_resource["name"],
        resource_attributes=summary_resource["metadata"],
    )

    get_or_create_granular_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_granular_resource",
        address=ckan_address,
        apikey=ckan_apikey,
        package_name_or_id=PACKAGE_NAME,
        resource_name=granular_resource["name"],
        resource_attributes=granular_resource["metadata"],
    )

    # backup existing data and dictionaries
    backup_summary_data = BackupDatastoreResourceOperator(
        task_id="backup_summary_data",
        address=ckan_address,
        apikey=ckan_apikey,
        resource_task_id="get_or_create_summary_resource",
        dir_task_id="backups_dir",
        trigger_rule="one_success",
    )

    backup_granular_data = BackupDatastoreResourceOperator(
        task_id="backup_granular_data",
        address=ckan_address,
        apikey=ckan_apikey,
        resource_task_id="get_or_create_granular_resource",
        dir_task_id="backups_dir",
        trigger_rule="one_success",
    )

    # branch: if resource is not new, backup existing fields
    is_summary_resource_new = BranchPythonOperator(
        task_id="is_summary_resource_new",
        python_callable=is_resource_new,
        op_kwargs={"resource_name": summary_resource["name"]},
    )
    summary_resource_not_new = DummyOperator(task_id="summary_resource_is_not_new")
    summary_resource_is_new = DummyOperator(task_id="summary_resource_is_new")

    is_granular_resource_new = BranchPythonOperator(
        task_id="is_granular_resource_new",
        python_callable=is_resource_new,
        op_kwargs={"resource_name": granular_resource["name"]},
    )
    granular_resource_not_new = DummyOperator(task_id="granular_resource_is_not_new")
    granular_resource_is_new = DummyOperator(task_id="granular_resource_is_new")

    # branch: data NOT new (data checksum), nothing to load
    is_summary_data_new = BranchPythonOperator(
        task_id="is_summary_data_new",
        python_callable=is_data_new,
        op_kwargs={
            "resource_name": summary_resource["name"],
            "backup_data_task_id": "backup_summary_data",
            "download_file_task_id": "get_summary_data",
        },
    )
    summary_data_not_new = DummyOperator(task_id="summary_data_is_not_new")
    summary_data_is_new = DummyOperator(task_id="summary_data_is_new")
    summary_file_not_new = DummyOperator(task_id="summary_file_not_new")

    is_granular_data_new = BranchPythonOperator(
        task_id="is_granular_data_new",
        python_callable=is_data_new,
        op_kwargs={
            "resource_name": granular_resource["name"],
            "backup_data_task_id": "backup_granular_data",
            "download_file_task_id": "get_granular_data",
        },
    )
    granular_data_not_new = DummyOperator(task_id="granular_data_is_not_new")
    granular_data_is_new = DummyOperator(task_id="granular_data_is_new")
    granular_file_not_new = DummyOperator(task_id="granular_file_not_new")

    # delete & insert records
    delete_summary_rows = DeleteDatastoreResourceRecordsOperator(
        task_id="delete_summary_rows",
        address=ckan_address,
        apikey=ckan_apikey,
        backup_task_id="backup_summary_data",
    )

    delete_granular_rows = DeleteDatastoreResourceRecordsOperator(
        task_id="delete_granular_rows",
        address=ckan_address,
        apikey=ckan_apikey,
        backup_task_id="backup_granular_data",
    )

    insert_summary_rows = InsertDatastoreResourceRecordsOperator(
        task_id="insert_summary_rows",
        address=ckan_address,
        apikey=ckan_apikey,
        parquet_filepath_task_id="transform_summary_data",
        resource_task_id="get_or_create_summary_resource",
    )

    insert_granular_rows = InsertDatastoreResourceRecordsOperator(
        task_id="insert_granular_rows",
        address=ckan_address,
        apikey=ckan_apikey,
        parquet_filepath_task_id="transform_granular_data",
        resource_task_id="get_or_create_granular_resource",
    )

    # sync file and resource last_modified times
    sync_summary_ts = ResourceAndFileOperator(
        task_id="sync_summary_ts",
        address=ckan_address,
        apikey=ckan_apikey,
        download_file_task_id="get_summary_data",
        resource_task_id="get_or_create_summary_resource",
        upload_to_ckan=False,
        sync_timestamp=True,
        trigger_rule="one_success",
    )

    sync_granular_ts = ResourceAndFileOperator(
        task_id="sync_granular_ts",
        address=ckan_address,
        apikey=ckan_apikey,
        download_file_task_id="get_granular_data",
        resource_task_id="get_or_create_granular_resource",
        upload_to_ckan=False,
        sync_timestamp=True,
        trigger_rule="one_success",
    )

    # notification message
    build_message = PythonOperator(
        task_id="build_message",
        trigger_rule="none_failed",
        python_callable=build_notification_message,
        op_kwargs={
            "summary_resource": summary_resource,
            "granular_resource": granular_resource,
        },
    )

    send_message = PythonOperator(
        task_id="send_message", python_callable=send_notification_message,
    )

    # cleanup
    delete_tmp_data = DeleteLocalDirectoryOperator(
        task_id="delete_tmp_data", path=Path(Variable.get("tmp_dir")) / PACKAGE_NAME,
    )

    # sequence
    tmp_dir >> get_summary_data >> validate_summary_expected_columns
    validate_summary_expected_columns >> transform_summary_data

    tmp_dir >> get_granular_data >> validate_granular_expected_columns
    validate_granular_expected_columns >> transform_granular_data

    backups_dir >> get_package

    get_package >> get_or_create_summary_resource >> is_summary_resource_new
    get_package >> get_or_create_granular_resource >> is_granular_resource_new

    is_summary_resource_new >> [summary_resource_not_new, summary_resource_is_new]
    summary_resource_not_new >> backup_summary_data
    summary_resource_is_new >> make_summary_data_dict >> insert_summary_data_dict
    insert_summary_data_dict >> backup_summary_data
    [backup_summary_data, transform_summary_data] >> is_summary_data_new

    is_granular_resource_new >> [granular_resource_not_new, granular_resource_is_new]
    granular_resource_not_new >> backup_granular_data
    granular_resource_is_new >> make_granular_data_dict >> insert_granular_data_dict
    insert_granular_data_dict >> backup_granular_data

    granular_file_not_new >> build_message
    [backup_granular_data, transform_granular_data] >> is_granular_data_new

    is_summary_data_new >> [
        summary_data_not_new,
        summary_data_is_new,
        summary_file_not_new,
    ]
    summary_file_not_new >> build_message
    summary_data_not_new >> sync_summary_ts
    summary_data_is_new >> delete_summary_rows >> insert_summary_rows >> sync_summary_ts
    sync_summary_ts >> build_message

    is_granular_data_new >> [
        granular_data_not_new,
        granular_data_is_new,
        granular_file_not_new,
    ]
    granular_data_not_new >> sync_granular_ts
    granular_data_is_new >> delete_granular_rows >> insert_granular_rows
    insert_granular_rows >> sync_granular_ts >> build_message

    build_message >> send_message >> delete_tmp_data
