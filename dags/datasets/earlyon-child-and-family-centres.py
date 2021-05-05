import hashlib
import json
import logging
import os
from pathlib import Path

import ckanapi
import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from ckan_plugin.operators.package_operator import GetPackageOperator
from ckan_plugin.operators.resource_operator import GetOrCreateResourceOperator
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

    @dag.task()
    def get_data(tmp_dir):
        response = requests.get(SRC_URL)
        assert response.status_code == 200, f"Response status: {response.status_code}"

        data = pd.DataFrame(response.json())
        filepath = Path(tmp_dir) / "new_data_raw.parquet"

        logging.info(f"Read {data.shape[0]} records")
        data.to_parquet(path=filepath, engine="fastparquet", compression=None)

        file_last_modified = response.headers["last-modified"]
        logging.info(f"Created file: {filepath}. Last modified: file_last_modified")

        return {"path": filepath, "file_last_modified": file_last_modified}

    @dag.task()
    def backup_previous_data(**kwargs):
        ti = kwargs["ti"]
        package = ti.xcom_pull(task_ids="get_package")
        backups = Path(ti.xcom_pull(task_ids="backups_dir"))

        resource_id = [r for r in package["resources"] if r["name"] == RESOURCE_NAME][
            0
        ]["id"]
        logging.info(f"Resource ID: {resource_id}")

        record_count = CKAN.action.datastore_search(id=resource_id, limit=0)["total"]
        if record_count == 0:
            return None

        datastore_response = CKAN.action.datastore_search(
            id=resource_id, limit=record_count
        )
        records = datastore_response["records"]
        logging.info(f"Example record retrieved: {json.dumps(records[0])}")

        data = pd.DataFrame(records)
        logging.info(f"Columns: {data.columns.values}")

        if "_id" in data.columns.values:
            data = data.drop("_id", axis=1)

        data_hash = hashlib.md5()
        data_hash.update(
            data.sort_values(by="loc_id").to_csv(index=False).encode("utf-8")
        )
        unique_id = data_hash.hexdigest()

        logging.info(f"Unique ID generated: {unique_id}")

        data_path = backups / f"data.{unique_id}.parquet"
        if not data_path.exists():
            data.to_parquet(path=data_path, engine="fastparquet", compression=None)

        fields = [f for f in datastore_response["fields"] if f["id"] != "_id"]

        fields_path = backups / f"fields.{unique_id}.json"
        if not fields_path.exists():
            with open(fields_path, "w") as f:
                json.dump(fields, f)

        return {
            "fields": fields_path,
            "data": data_path,
            "records": data.shape[0],
            "columns": data.shape[1],
        }

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
    def transform_data(tmp_dir, data_file_info):
        logging.info(f"tmp_dir: {tmp_dir} | data_file_info: {data_file_info}")

        tmp_dir = Path(tmp_dir)
        data_fp = Path(data_file_info["path"])

        data = pd.read_parquet(data_fp)

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

    @dag.task()
    def get_resource(package):
        return [r for r in package["resources"] if r["name"] == RESOURCE_NAME][0]

    def is_file_new(resource, data_file_info):
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

    def is_data_new(checksum, **kwargs):
        ti = kwargs["ti"]
        backups_dir = Path(ti.xcom_pull(task_ids="backups_dir"))

        logging.info(f"checksum: {checksum}")
        logging.info(f"backups_dir: {backups_dir}")

        backups = Path(backups_dir)
        for f in os.listdir(backups):
            if not os.path.isfile(backups / f):
                continue
            logging.info(f"File in backups: {f}")

            if os.path.isfile(backups / f) and checksum in f:
                logging.info(f"Data has already been loaded, ID: {checksum}")
                return "data_is_not_new"

        logging.info(f"Data has not been loaded, new ID: {checksum}")
        return "data_is_new"

    @dag.task()
    def make_checksum(transformed_data_fp):
        data = pd.read_parquet(Path(transformed_data_fp))
        data_hash = hashlib.md5()
        data_hash.update(
            data.sort_values(by="loc_id").round(10).to_csv(index=False).encode("utf-8")
        )
        checksum = data_hash.hexdigest()
        logging.info(f"checksum: {checksum}")

        return checksum

    @dag.task()
    def delete_records(resource):
        logging.info(resource)

        resource_id = resource["id"]

        delete = CKAN.action.datastore_delete(id=resource_id, filters={})
        logging.info(f"datastore_delete: {delete}")

        count = CKAN.action.datastore_search(id=resource_id, limit=0)["total"]
        logging.info(f"datastore_search: {delete}")

        assert count == 0, f"Resource not empty after cleanup: {count}"

    @dag.task()
    def insert_new_records(resource, transformed_data_fp, fields):
        data = pd.read_parquet(Path(transformed_data_fp))
        records = data.to_dict(orient="records")

        ckan_utils.insert_datastore_records(
            ckan=CKAN,
            resource_id=resource["id"],
            records=records,
            fields=fields,
            chunk_size=int(Variable.get("ckan_insert_chunk_size")),
        )

        return len(records)

    @dag.task(trigger_rule="none_failed")
    def get_fields(backup_data, fields):
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

    @dag.task(trigger_rule="none_failed")
    def refresh_package():
        return CKAN.action.package_show(id=dag.dag_id)

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

    source_file = get_data(tmp_dir)

    package = GetPackageOperator(
        task_id="get_package",
        address=ckan_creds[ACTIVE_ENV]["address"],
        apikey=ckan_creds[ACTIVE_ENV]["apikey"],
        package_name_or_id=dag.dag_id,
    )

    new_resource_branch = BranchPythonOperator(
        task_id="new_resource_branch", python_callable=is_resource_new,
    )

    transformed_data = transform_data(tmp_dir, source_file)

    checksum = make_checksum(transformed_data)

    backup_data = backup_previous_data()

    data_dict = build_data_dict(transformed_data)

    get_or_create_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_resource",
        package_name_or_id=PACKAGE_ID,
        resource_name=RESOURCE_NAME,
        resource_attributes=dict(
            format="geojson",
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {dag.dag_id}",
        ),
    )

    package_refresh = refresh_package()

    refresh_package = GetPackageOperator(
        task_id="refresh_package",
        address=ckan_creds[ACTIVE_ENV]["address"],
        apikey=ckan_creds[ACTIVE_ENV]["apikey"],
        package_name_or_id=dag.dag_id,
    )

    package >> new_resource_branch >> DummyOperator(
        task_id="resource_is_new"
    ) >> get_or_create_resource

    new_resource_branch >> DummyOperator(
        task_id="resource_is_not_new"
    ) >> backup_data >> get_or_create_resource

    resource = get_resource(package_refresh)

    file_new_branch = BranchPythonOperator(
        task_id="file_new_branch",
        python_callable=is_file_new,
        op_args=(resource, source_file),
    )

    new_data_branch = BranchPythonOperator(
        task_id="new_data_branch", python_callable=is_data_new, op_args=(checksum),
    )

    delete_tmp_data = PythonOperator(
        task_id="delete_tmp_data",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": dag.dag_id, "recursively": True},
        trigger_rule="one_success",
    )

    records_deleted = delete_records(resource)

    updated_resource = update_resource_last_modified(resource, source_file)

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

    file_new_branch >> DummyOperator(task_id="file_is_new") >> [
        new_data_branch,
        updated_resource,
    ]

    file_new_branch >> DummyOperator(
        task_id="file_is_not_new"
    ) >> send_nothing_notification

    fields = get_fields(backup_data, data_dict)

    records_inserted = insert_new_records(resource, transformed_data, fields)

    new_records_notification = PythonOperator(
        task_id="new_records_notification",
        python_callable=airflow_utils.message_slack,
        op_args=(
            dag.dag_id,
            f"Refreshed: {records_inserted} records",
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

    new_data_branch >> DummyOperator(
        task_id="data_is_new"
    ) >> records_deleted >> records_inserted >> new_records_notification

    new_data_branch >> DummyOperator(
        task_id="data_is_not_new"
    ) >> no_new_data_notification

    updated_resource >> [new_records_notification, no_new_data_notification]

    [
        no_new_data_notification,
        new_records_notification,
        send_nothing_notification,
    ] >> delete_tmp_data
