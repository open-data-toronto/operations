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
from ckan_operators.datastore_operator import (
    BackupDatastoreResourceOperator,
    DeleteDatastoreResourceRecordsOperator,
    InsertDatastoreResourceRecordsOperator,
    RestoreDatastoreResourceBackupOperator,
    DeleteDatastoreResourceOperator,
)

from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator
from ckan_operators.package_operator import GetPackageOperator
from ckan_operators.resource_operator import (
    GetOrCreateResourceOperator,
    ResourceAndFileOperator,
)
from dateutil import parser
from utils import agol_utils, airflow_utils
from utils_operators.directory_operator import CreateLocalDirectoryOperator, DeleteLocalDirectoryOperator
from utils_operators.file_operators import DownloadFileOperator

RESOURCE_NAME = "EarlyON Child and Family Centres Locations"
SRC_URL = "http://opendata.toronto.ca/childrens.services/child-family-programs/EarlyON_centre_registry.json"  # noqa: E501
PACKAGE_NAME = "earlyon-locations"
dag_id = "Earlyon-Centres-Registry"
tmp_folder = Path(Variable.get("tmp_dir")) / dag_id
filepath = tmp_folder / "data.parquet"
fields_filepath = tmp_folder / "fields.json"
schema_change = True
maker_attr_def = {
      "address": "Street Address",
      "languages": "Language of Service",
      "serviceName": "Drop-in",
      "consultant_phone": "416-338-6524",
      "ward": "13",
      "email": "shalfon@toronto.ca.test",
      "buildingName": "Toronto Kiwanis Boys & Girls Clubs",
      "ward_name": "TORONTO CENTRE",
      "loc_id": "13650",
      "programTypes": [
        
      ],
      "lat": "43.6642026056",
      "lng": "-79.3622372186",
      "agency": "Regent Park Community Health Centre",
      "program_name": "Location Name",
      "program": "<a id='13650' href='#modal13650_' data-toggle='modal' data-target='#myModal' onclick='fillData(13650)'>101 Spruce St EarlyON Child and Family Centre</a>",
      "consultant_fullname": "Shani Halfon",
      "contact_fullname": "Maleda Mulu",
      "website": "www.wsncc.org",
      "website_name": "Regent Park Community Health Centre Website",
      "website_description": " Website for Agency Regent Park Community Health Centre",
      "phone": "416-362-0805 X 230",
      "full_address": "101 Spruce St, Toronto, ON M5A 2J3",
      "dropinHours": "Wednesday: 9:00 a.m. - 11:30 a.m.  ",
      "registeredHours": "Thursday: 9:00 a.m. - 11:30 a.m.  ",
      "virtualHours": "N/A",
      "service_system_manager": "City of Toronto",
      "centre_type": "Centre",
      "french_language_program": "Location offers French-language programming",
      "indigenous_program": 'Location offers Indigenous programming',
      "located_in_school": "No",
      "school_name": "BANTING AND BEST PUBLIC SCHOOL",
      "contact_title": "PROGRAM MANAGER",
      "contact_email": "maledam@regentparkchc.org.test",
      "major_intersection": "Kingston / McCowan",
      "geometry":"markers geometry"
}
delete_col = ['program', 'programTypes']
EXPECTED_COLUMNS = [x for x in (m for m in maker_attr_def) if x not in delete_col]

def send_failure_message():
    airflow_utils.message_slack(
        name=PACKAGE_NAME,
        message_type="error",
        msg="Job not finished",
        active_env=Variable.get("active_env"),
        prod_webhook=Variable.get("active_env") == "prod",
    )


with DAG(
    dag_id,
    default_args = airflow_utils.get_default_args(
        {
            "owner": "Gary Qi",
            "depends_on_past": False,
            "email": ["gary.qi@toronto.ca"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 0,
            "on_failure_callback": task_failure_slack_alert,
            "start_date": days_ago(1),
        }
    ),
    description="Take new earlyon.json from opendata.toronto.ca and put into datastore",
    schedule_interval="0 18 * * *",
    catchup=False,
    tags=["earlyon","gary"],
) as dag:

    # def is_resource_new(**kwargs):
    #     package = kwargs["ti"].xcom_pull(task_ids="get_package")
    #     logging.info(f"resources found: {[r['name'] for r in package['resources']]}")
    #     is_new = RESOURCE_NAME not in [r["name"] for r in package["resources"]]
    #     if is_new:
    #         return "resource_is_new"
    #     return "resource_is_not_new"
    def is_resource_new(**kwargs):
        resource = kwargs["ti"].xcom_pull(task_ids="get_or_create_resource")

        if resource["is_new"]:
            return "new_resource"

        return "existing_resource"

    def build_data_dict(**kwargs):
        data = pd.read_parquet(Path(filepath))

        fields = []
        logging.info(f"EXPECTED_COLUMNS: {EXPECTED_COLUMNS}")

        for field, dtype in data[EXPECTED_COLUMNS].dtypes.iteritems():
            ckan_type_map = {"int64": "int", "object": "text", "float64": "float"}
            fields.append({"type": ckan_type_map[dtype.name], "id": field, "info": { "notes": maker_attr_def[field]}})
        logging.info(f"fields: {fields}")

        with open(fields_filepath, 'w') as fields_file:
            json.dump(fields, fields_file)

        return fields_filepath

    def validate_expected_columns(**kwargs):
        ti = kwargs["ti"]
        data_file_info = ti.xcom_pull(task_ids="get_data")

        with open(Path(data_file_info["path"])) as f:
            # markers = json.load(f)['markers']
            src_json_data = json.load(f)
        markers = src_json_data['markers']

        df = pd.DataFrame(markers)

        # for col in df.columns.values:
        #     assert col in EXPECTED_COLUMNS, f"{col} not in list of expected columns"

        for col in EXPECTED_COLUMNS:
            assert col in [*df.columns.values,'geometry'], f"Expected column {col} not in data file"

    def transform_data(**kwargs):
        ti = kwargs["ti"]
        data_file_info = ti.xcom_pull(task_ids="get_data")

        with open(Path(data_file_info["path"])) as f:
            src_json_data = json.load(f)
        markers = src_json_data['markers']

        logging.info(f"tmp_folder: {tmp_folder} | data_file_info: {data_file_info}")

        data = pd.DataFrame(markers)

        data["geometry"] = data.apply(
            lambda x: json.dumps(
                {"type": "Point", "coordinates": [x["lng"], x["lat"]]}
            )
            if x["lng"] and x["lat"]
            else "",
            axis=1,
        )

        # data = agol_utils.remove_geo_columns(data)    let's not remove lat lng in this case
        data = data.drop(delete_col, axis=1)


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
            return "file_is_not_new"

        return "file_is_new"

    def is_data_new(**kwargs):
        ti = kwargs["ti"]
        fields = ti.xcom_pull(task_ids="get_fields")
        if fields is not None:
            return "data_is_new"

        backup_data = ti.xcom_pull(task_ids="backup_data")

        df = pd.read_parquet(backup_data["data"])

        if df.shape[0] == 0:
            return "data_is_new"

        checksum = hashlib.md5()
        checksum.update(df.sort_values(by="loc_id").to_csv(index=False).encode("utf-8"))
        checksum = checksum.hexdigest()

        for f in os.listdir(backup_data):
            if not os.path.isfile(tmp_dir / f):
                continue

            logging.info(f"File in backups: {f}")
            if os.path.isfile(tmp_dir / f) and checksum in f:
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
        task_id="tmp_dir", path=tmp_folder,
    )

    src = DownloadFileOperator(
        task_id="get_data",
        file_url=SRC_URL,
        dir = tmp_folder, 
        filename="src_data.json",
    )

    get_package = GetPackageOperator(
        task_id="get_package",
        address=ckan_address,
        apikey=ckan_apikey,
        package_name_or_id=PACKAGE_NAME,
    )

    res_new_or_existing = BranchPythonOperator(
        task_id="res_new_or_existing", python_callable=is_resource_new,
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
            package_id=PACKAGE_NAME,
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {dag_id}",
        ),
    )

    backup_data = BackupDatastoreResourceOperator(
        task_id="backup_data",
        address=ckan_address,
        apikey=ckan_apikey,
        resource_task_id="get_or_create_resource",
        dir_task_id="tmp_dir",
        sort_columns=["loc_id"],
    )

    get_fields = PythonOperator(
        task_id="get_fields", python_callable=get_fields, trigger_rule="none_failed"
    )

    # file_new_branch = BranchPythonOperator(
    #     task_id="file_new_branch", python_callable=is_file_new,
    # )

    # new_data_branch = BranchPythonOperator(
    #     task_id="is_data_new", python_callable=is_data_new,
    # )

    delete_tmp_dir = DeleteLocalDirectoryOperator(
        task_id = "delete_tmp_dir",
        path = tmp_folder,
        #on_success_callback=task_success_slack_alert,
    )

    # delete_tmp_dir = PythonOperator(
    #     task_id="delete_tmp_dir",
    #     python_callable=airflow_utils.delete_tmp_data_dir,
    #     op_kwargs={"dag_id": dag_id, "recursively": True},
    #     trigger_rule="one_success",
    # )

    modify_metadata = ResourceAndFileOperator(
        task_id="modify_metadata",
        address=ckan_address,
        apikey=ckan_apikey,
        download_file_task_id="get_data",
        resource_task_id="get_or_create_resource",
        upload_to_ckan=False,
        sync_timestamp=True,
        trigger_rule="one_success",
    )

    # send_nothing_notification = PythonOperator(
    #     task_id="send_nothing_notification",
    #     python_callable=airflow_utils.message_slack,
    #     op_args=(
    #         PACKAGE_NAME,
    #         "No new data file",
    #         "success",
    #         active_env == "prod",
    #         active_env,
    #     ),
    # )

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
        fields_json_path_task_id='create_data_dictionary',
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

    new_resource = DummyOperator(task_id="new_resource", dag=dag)
    existing_resource = DummyOperator(task_id="existing_resource", dag=dag)
    join_or = DummyOperator(task_id="join_or", dag=dag, trigger_rule="one_success")
    # join_and = DummyOperator(task_id="join_and", dag=dag, trigger_rule="all_success")
    def delete_res_or_not():  # delete records or delete datastore schema
        if schema_change:
            return "delete_datastore"

        return "delete_records"

    delete_res_or_not = BranchPythonOperator(
        task_id="delete_res_or_not", python_callable=delete_res_or_not,
    )
    delete_datastore = DeleteDatastoreResourceOperator(   # delete datastore schema for the resource
        task_id="delete_datastore",
        address=ckan_address,
        apikey=ckan_apikey,
        resource_id_task_id = "get_or_create_resource",
        resource_id_task_key = "id",
    )
    job_failed = DummyOperator(
        task_id = "job_failed",
        trigger_rule="one_failed"
    )
    message_slack_abort = GenericSlackOperator(
        task_id = "message_slack_abort",
        message_header = dag_id + " failed, data records not changed",
    )

    message_slack_recover = GenericSlackOperator(
        task_id = "message_slack_recover",
        message_header = dag_id + " failed to update, data records restored",
    )

    tmp_dir >> src >> validated_columns >> transformed_data >>  get_package >>  get_or_create_resource >> res_new_or_existing >> [new_resource,  existing_resource ]
    new_resource >>  join_or
    existing_resource >> backup_data >> delete_res_or_not >> [delete_datastore, delete_records] >> join_or >> create_data_dictionary >> insert_records >> modify_metadata >> delete_tmp_dir >>records_loaded_branch >>  [new_records_notification, no_new_data_notification]  
    [src, get_or_create_resource] >> job_failed  >> message_slack_abort
    insert_records >> job_failed >> get_fields >> restore_backup >> message_slack_recover

