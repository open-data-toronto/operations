from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
import pandas as pd
import ckanapi
import logging
import hashlib
from pathlib import Path
import requests
import json
import os
from dateutil import parser

from utils import airflow_utils
from utils import agol_utils
from utils import ckan_utils

job_settings = {
    "description": "Take earlyon.json from opendata.toronto.ca and put into datastore",
    "schedule": "0 17 * * *",
    "start_date": days_ago(1),
}

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = JOB_FILE.name[:-3]
PACKAGE_ID = JOB_NAME.replace("_", "-")

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])
SRC_FILE = "http://opendata.toronto.ca/childrens.services/child-family-programs/earlyon.json"  # noqa: E501
RESOURCE_NAME = "EarlyON Child and Family Centres"


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=JOB_NAME,
        message_type="error",
        msg="Job not finished",
        active_env=ACTIVE_ENV,
        prod_webhook=ACTIVE_ENV == "prod",
    )


with DAG(
    JOB_NAME,
    default_args=airflow_utils.get_default_args(
        {
            "on_failure_callback": send_failure_msg,
            "start_date": job_settings["start_date"],
            "retries": 0,
            # "retry_delay": timedelta(minutes=3),
        }
    ),
    description=job_settings["description"],
    schedule_interval=job_settings["schedule"],
    catchup=False,
    tags=["dataset"],
) as dag:

    @dag.task()
    def get_data(tmp_dir):
        response = requests.get(SRC_FILE)
        data = pd.DataFrame(response.json())
        filepath = Path(tmp_dir) / "new_data_raw.parquet"
        logging.info(f"Read {data.shape[0]} records")
        data.to_parquet(filepath)

        file_last_modified = response.headers["last-modified"]

        return {"path": filepath, "file_last_modified": file_last_modified}

    @dag.task(trigger_rule="none_failed")
    def backup_previous_data(package, backups_dir):
        backups = Path(backups_dir)

        resource_id = [r for r in package["resources"] if r["name"] == RESOURCE_NAME][
            0
        ]["id"]
        logging.info(f"Resource ID: {resource_id}")

        record_count = CKAN.action.datastore_search(id=resource_id, limit=0)["total"]

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
            data.to_parquet(data_path)

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

    def is_resource_new(package):
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

        data.to_parquet(filepath)

        return filepath

    @dag.task()
    def get_resource(package):
        return [r for r in package["resources"] if r["name"] == RESOURCE_NAME][0]

    @dag.task()
    def is_file_new(resource, data_file_info):
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
            return "file_is_new"

        return "file_is_not_new"

    @dag.task()
    def is_data_new(checksum, backups_dir):
        backups = Path(backups_dir) / JOB_NAME
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
    def get_checksum(transformed_data_fp):
        data = pd.read_parquet(Path(transformed_data_fp))
        data_hash = hashlib.md5()
        data_hash.update(
            data.sort_values(by="loc_id").round(10).to_csv(index=False).encode("utf-8")
        )
        return data_hash.hexdigest()

    @dag.task()
    def delete_previous_records(resource):
        resource_id = resource["id"]
        CKAN.action.datastore_delete(id=resource_id, filters={})
        count = CKAN.action.datastore_search(id=resource_id, limit=0)["total"]

        assert count == 0, f"Resource not empty after cleanup: {count}"

    @dag.task()
    def create_resource(package, data_dict):
        resource = CKAN.action.resource_create(
            package_id=package["id"],
            name=RESOURCE_NAME,
            format="geojson",
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {JOB_NAME}",
        )

        CKAN.action.datastore_create(id=resource["id"], fields=data_dict)

    @dag.task()
    def insert_new_records(
        resource, transformed_data_fp, backup_data,
    ):
        resource_id = resource["id"]
        data = pd.read_parquet(Path(transformed_data_fp))
        records = data.to_dict(orient="records")

        with open(Path(backup_data["fields"]), "r") as f:
            fields = json.load(f)

        ckan_utils.insert_datastore_records(
            ckan=CKAN,
            resource_id=resource_id,
            records=records,
            fields=fields,
            chunk_size=int(Variable.get("ckan_insert_chunk_size")),
        )

        return len(records)

    @dag.task(trigger_rule="one_success")
    def update_resource_last_modified(resource, source_file):
        return ckan_utils.update_resource_last_modified(
            ckan=CKAN,
            resource_id=resource["id"],
            new_last_modified=parser.parse(source_file["file_last_modified"]),
        )

    @dag.task()
    def build_message(transformed_data_fp, record_count, resource):
        if record_count > 0:
            new_data = pd.read_parquet(Path(transformed_data_fp))
            return f"Refreshed: {new_data.shape[0]} records"

        last_modified = parser.parse(resource["last_modified"]).strftime(
            "%Y-%m-%d %H:%M"
        )

        return f"New file, no new data. New last modified timestamp: {last_modified}"

    tmp_dir = PythonOperator(
        task_id="tmp_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": JOB_NAME, "dir_variable_name": "tmp_dir"},
    )

    backups_dir = PythonOperator(
        task_id="backups_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": JOB_NAME, "dir_variable_name": "backups_dir"},
    )

    source_file = get_data(tmp_dir)

    package = PythonOperator(
        task_id="get_package",
        python_callable=CKAN.action.package_show,
        op_kwargs={"id": PACKAGE_ID},
    )

    new_resource_branch = BranchPythonOperator(
        task_id="new_resource_branch",
        python_callable=is_resource_new,
        op_args=(package),
    )
    resource_is_new = DummyOperator(task_id="resource_is_new")
    resource_is_not_new = DummyOperator(task_id="resource_is_not_new")

    transformed_data = transform_data(tmp_dir, source_file)

    checksum = get_checksum(transformed_data)

    backup_data = backup_previous_data(package, backups_dir)

    data_dict = build_data_dict(transformed_data)

    new_resource = create_resource(package, data_dict)

    new_resource >> backup_data

    package_refresh = PythonOperator(
        task_id="get_package_again",
        python_callable=CKAN.action.package_show,
        op_kwargs={"id": PACKAGE_ID},
        trigger_rule="none_failed",
    )

    new_resource_branch >> resource_is_new >> new_resource >> data_dict
    new_resource_branch >> resource_is_not_new >> backup_data
    [data_dict, backup_data] >> package_refresh

    resource = get_resource(package_refresh)

    file_new_branch = BranchPythonOperator(
        task_id="file_new_branch",
        python_callable=is_file_new,
        op_args=(resource, source_file),
    )

    new_data_branch = BranchPythonOperator(
        task_id="new_data_branch",
        python_callable=is_data_new,
        op_args=(checksum, backups_dir),
    )
    data_is_new = DummyOperator(task_id="data_is_new")
    data_is_not_new = DummyOperator(task_id="data_is_not_new")

    delete_tmp_data = PythonOperator(
        task_id="delete_tmp_data",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": JOB_NAME, "recursively": True},
        trigger_rule="none_failed",
    )

    records_deleted = delete_previous_records(resource)

    updated_resource = update_resource_last_modified(resource, source_file)

    file_new_branch >> DummyOperator(task_id="file_is_new") >> new_data_branch
    file_new_branch >> DummyOperator(task_id="file_is_not_new") >> delete_tmp_data

    records_inserted = insert_new_records(resource, transformed_data, backup_data)

    new_data_branch >> data_is_new >> records_deleted >> records_inserted
    new_data_branch >> data_is_not_new >> updated_resource
    records_inserted >> updated_resource

    msg = build_message(transformed_data, records_inserted, updated_resource)

    send_notification = PythonOperator(
        task_id="backups_dir",
        python_callable=airflow_utils.message_slack,
        op_args=(JOB_NAME, msg, "success", ACTIVE_ENV == "prod", ACTIVE_ENV,),
    )

    send_notification >> delete_tmp_data
