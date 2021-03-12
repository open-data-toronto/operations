from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import ckanapi
import logging
from io import BytesIO
from pathlib import Path
from airflow import DAG
import requests
import json
import os
import sys
import re
from dateutil import parser

sys.path.append(Variable.get("repo_dir"))
from utils import airflow as airflow_utils  # noqa: E402
from utils import ckan as ckan_utils  # noqa: E402

job_settings = {
    "description": "Take data files from Transportation's flashscrow endpoint https://flashcrow-etladmin.intra.dev-toronto.ca/open_data/tmcs into CKAN",  # noqa: E501
    "schedule": "5 4 * * 1",
    "start_date": datetime(2020, 11, 24, 13, 35, 0),
}

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = JOB_FILE.name[:-3]
PACKAGE_ID = JOB_NAME.replace("_", "-")

# ACTIVE_ENV = Variable.get("active_env")
ACTIVE_ENV = "dev"
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])
SRC_FILES = "https://flashcrow-etladmin.intra.dev-toronto.ca/open_data/tmcs/"


FILE_RESOURCE_MAP = {
    "tmcs_1980_1989.csv": "raw-data-1980-1989",
    "tmcs_1990_1999.csv": "raw-data-1990-1999",
    "tmcs_2000_2009.csv": "raw-data-2000-2009",
    "tmcs_2010_2019.csv": "raw-data-2010-2019",
    "tmcs_2020_2029.csv": "raw-data-2020-2029",
    "tmcs_count_metadata.csv": "count-metadata",
    "tmcs_locations.csv": "locations",
    "tmcs_preview.csv": "latest-counts-by-location",
}


def send_success_msg(**kwargs):
    ti = kwargs.pop("ti")
    msg = ti.xcom_pull(task_ids="build_message")

    airflow_utils.message_slack(
        name=JOB_NAME,
        message_type="success",
        msg=msg,
        prod_webhook=ACTIVE_ENV == "prod",
        active_env=ACTIVE_ENV,
    )


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=JOB_NAME,
        message_type="error",
        msg="Job not finished",
        active_env=ACTIVE_ENV,
        prod_webhook=ACTIVE_ENV == "prod",
    )


def get_raw_files(**kwargs):
    ti = kwargs.pop("ti")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))

    html = requests.get(SRC_FILES).content.decode("utf-8")
    hrefs = re.findall('href="(.*?)"', html, re.DOTALL)
    filenames = [f for f in hrefs if f.endswith(".csv")]

    raw_files = []
    for f in sorted(filenames, reverse=True):
        try:
            resource_name = FILE_RESOURCE_MAP[f]
        except KeyError:
            logging.warn(f"File not expected: {f}. Skipping")
            continue

        response = requests.get(SRC_FILES + f)

        file_content = response.content
        data = pd.read_csv(BytesIO(file_content), encoding="utf8")

        filepath = tmp_dir / f.replace("csv", "parquet")

        logging.info(f"Read {f}: {data.shape[0]} records")
        data.to_parquet(filepath)

        row = {
            "raw_data_filepath": filepath,
            "file_last_modified": response.headers["last-modified"],
            "target_resource": resource_name,
        }
        logging.info(row)

        raw_files.append(row)

    return raw_files


def transform_data_files(**kwargs):
    def validate_columns(df):
        expected_columns = [
            "count_id",
            "latest_count_id",
            "latest_count_date",
            "count_date",
            "location_id",
            "location",
            "lng",
            "lat",
            "centreline_type",
            "centreline_id",
            "px",
            "time_start",
            "time_end",
            "sb_cars_r",
            "sb_cars_t",
            "sb_cars_l",
            "nb_cars_r",
            "nb_cars_t",
            "nb_cars_l",
            "wb_cars_r",
            "wb_cars_t",
            "wb_cars_l",
            "eb_cars_r",
            "eb_cars_t",
            "eb_cars_l",
            "sb_truck_r",
            "sb_truck_t",
            "sb_truck_l",
            "nb_truck_r",
            "nb_truck_t",
            "nb_truck_l",
            "wb_truck_r",
            "wb_truck_t",
            "wb_truck_l",
            "eb_truck_r",
            "eb_truck_t",
            "eb_truck_l",
            "sb_bus_r",
            "sb_bus_t",
            "sb_bus_l",
            "nb_bus_r",
            "nb_bus_t",
            "nb_bus_l",
            "wb_bus_r",
            "wb_bus_t",
            "wb_bus_l",
            "eb_bus_r",
            "eb_bus_t",
            "eb_bus_l",
            "nx_peds",
            "sx_peds",
            "ex_peds",
            "wx_peds",
            "nx_bike",
            "sx_bike",
            "ex_bike",
            "wx_bike",
            "nx_other",
            "sx_other",
            "ex_other",
            "wx_other",
        ]

        for col in df.columns.values:
            assert col in expected_columns, f"{col} not in list of expected columns"

    def prep_data(df):
        df = df.copy()
        id_columns = [
            col
            for col in df.columns
            if col.endswith("_id") or col in ["centreline_type", "px"]
        ]
        df[id_columns] = df[id_columns].astype("Int64")

        for y in [
            col for col in df.columns if col in ["time_start", "time_end", "count_date"]
        ]:
            df[y] = pd.to_datetime(df[y])
            if "date" not in y:
                df[y] = df[y].dt.tz_localize("EST")

        return df

    def make_ckan_fields(df):
        fields = []

        for col, col_type in df.dtypes.items():
            col_type = col_type.name.lower()

            if col_type == "object":
                field_type = "text"
            elif col_type == "int64":
                field_type = "int"
            elif "datetime64" in col_type and col.endswith("_date"):
                field_type = "date"
            elif "datetime64" in col_type:
                field_type = "timestamp"
            elif col_type == "bool":
                field_type = "bool"
            elif col_type == "float64":
                field_type = "float"

            fields.append({"type": field_type, "id": col})

        return fields

    ti = kwargs.pop("ti")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))
    raw_files = ti.xcom_pull(task_ids="get_raw_files")

    processed_data_files = []

    for i in raw_files:
        path = Path(i["raw_data_filepath"])
        filename = path.name
        resource_name = i["target_resource"]

        df = pd.read_parquet(path)

        row = {
            **i,
        }

        validate_columns(df)
        data = prep_data(df)
        logging.info(f"{f} | {data.shape[0]} columns, {data.shape[1]} rows")
        if filename.startswith("tmcs_preview"):
            data["geometry"] = data.apply(
                lambda x: json.dumps(
                    {"type": "Point", "coordinates": [x["lng"], x["lat"]]}
                )
                if x["lng"] and x["lat"]
                else "",
                axis=1,
            )
            fpath = tmp_dir / f"{resource_name}.datastore.records.json"
            data = data[
                data["count_date"].dt.year > datetime.now().year - 1
            ]  # TODO: filter at source and remove
            logging.info(
                f"{f} | FILTERED: {data.shape[0]} columns, {data.shape[1]} rows"
            )
            data.to_json(fpath, orient="records", date_format="iso")

            fields_path = tmp_dir / f"{resource_name}.datastore.fields.json"
            fields = make_ckan_fields(df)

            with open(fields_path, "w") as ff:
                json.dump(fields, ff)

        else:
            fmt = "zip"
            fpath = tmp_dir / f"{resource_name}.{fmt}"
            compression_options = dict(method=fmt, archive_name=filename)
            data.to_csv(fpath, compression=compression_options, index=False)

        row["processed_data_file"] = fpath
        processed_data_files.append(row)

    return processed_data_files


def identify_resources_to_load(**kwargs):
    ti = kwargs.pop("ti")
    data_files = ti.xcom_pull(task_ids="transform_data_files")
    package = ti.xcom_pull(task_ids="get_package")

    resources_to_load = []
    for i in data_files:
        if "error" in i:
            resources_to_load.append(i)
            continue

        update = True
        raw_fpath = Path(i["raw_data_filepath"])
        resource_name = i["target_resource"]

        for resource in package["resources"]:
            if resource["name"] != resource_name:
                continue

            last_modified_attr = (
                resource["last_modified"]
                if resource["last_modified"]
                else resource["created"]
            )

            file_last_modified = parser.parse(i["file_last_modified"])

            resource_last_modified = parser.parse(last_modified_attr + " UTC")

            time_delta = (
                file_last_modified.timestamp() - resource_last_modified.timestamp()
            )

            update = time_delta != 0
            break

        if update:
            logging.info(
                "To refresh: {}s between file {} and resource {} last modified".format(
                    time_delta, raw_fpath.name, resource_name
                )
            )
            resources_to_load.append(i)
        else:
            logging.info(
                "No change: {} same last modified time as source file".format(
                    resource_name
                )
            )

    return resources_to_load


def insert_datastore_resources(**kwargs):
    ti = kwargs.pop("ti")
    resources_to_load = ti.xcom_pull(task_ids="identify_resources_to_load")
    package = ckan_utils.get_package(ckan=CKAN, package_id=PACKAGE_ID)

    datastore_resources = [
        r
        for r in resources_to_load
        if "processed_data_file" in r
        and "datastore" in Path(r["processed_data_file"]).name
    ]

    results = []
    for i in datastore_resources:
        resource_name = i["target_resource"]
        fpath = Path(i["processed_data_file"])

        try:
            with open(fpath, "r") as f:
                records = json.load(f)

            fields_path = Path(
                fpath.parent / fpath.name.replace(".records.", ".fields.")
            )
            with open(fields_path, "r") as f:
                fields = json.load(f)

            resource = ckan_utils.create_resource_if_new(
                ckan=CKAN,
                package=package,
                resource={
                    "package_id": PACKAGE_ID,
                    "name": resource_name,
                    "format": "geojson" if "geometry" in records[0] else "csv",
                    "is_preview": True,
                },
            )

            backup_records = None

            if resource["datastore_active"]:
                res = CKAN.action.datastore_search(id=resource["id"], limit=0)

                fields = [f for f in res["fields"] if f["id"] != "_id"]
                with open(fields_path, "w") as ff:
                    json.dump(fields, ff)

                logging.info(
                    f"Will use current data dict, backed up into {fields_path}"
                )

                backup_records = CKAN.action.datastore_search(
                    id=resource["id"], limit=res["total"]
                )["records"]

                backup_fpath = Path(
                    fpath.parent / fpath.name.replace(".records.", ".records.backup.")
                )
                with open(backup_fpath, "w") as ff:
                    json.dump(backup_records, ff)

                CKAN.action.datastore_delete(id=resource["id"])

            try:
                ckan_utils.insert_datastore_records(
                    ckan=CKAN,
                    resource_id=resource["id"],
                    records=records,
                    fields=fields,
                    chunk_size=int(Variable.get("ckan_insert_chunk_size")),
                )

                logging.info(
                    "Successfully loaded {} with {} cols and {} rows".format(
                        resource_name, len(fields), len(records)
                    )
                )
                results.append(i)

            except Exception as e:
                if backup_records is not None:
                    with open(backup_fpath, "r") as f:
                        records = json.load(f)

                    ckan_utils.insert_datastore_records(
                        ckan=CKAN,
                        resource_id=resource["id"],
                        records=records,
                        fields=fields,
                        chunk_size=int(Variable.get("ckan_insert_chunk_size")),
                    )

                    logging.warn(
                        "Something went wrong. Restoring {} rows for {}".format(
                            len(records), resource_name
                        )
                    )
                raise e

        except Exception as e:
            err = "Could not load datastore resource {}. Error: {}".format(
                resource_name, e
            )
            logging.error(err)
            results.append({**i, "error": err})

    return results


def upload_filestore_resources(**kwargs):
    ti = kwargs.pop("ti")
    resources_to_load = ti.xcom_pull(task_ids="identify_resources_to_load")
    package = ckan_utils.get_package(ckan=CKAN, package_id=PACKAGE_ID)

    filestore_resources = [
        r
        for r in resources_to_load
        if "processed_data_file" in r
        and "datastore" not in Path(r["processed_data_file"]).name
    ]

    results = []
    for i in filestore_resources:
        resource_name = i["target_resource"]
        fpath = Path(i["processed_data_file"])

        try:
            resource = ckan_utils.create_resource_if_new(
                ckan=CKAN,
                package=package,
                resource={
                    "package_id": PACKAGE_ID,
                    "name": resource_name,
                    "format": fpath.name.split(".")[-1].lower(),
                    "is_preview": False,
                },
            )

            CKAN.action.resource_patch(
                id=resource["id"], upload=open(fpath, "rb"),
            )

            logging.info(f"Uploaded: {resource_name}")
            results.append(i)

        except Exception as e:
            err = f"Could not upload file {resource_name}. Error: {e}"
            logging.error(err)
            results.append({**i, "error": err})

    return results


def update_resource_last_modified(**kwargs):
    ti = kwargs.pop("ti")
    insert_datastore_resources = ti.xcom_pull(task_ids="insert_datastore_resources")
    upload_filestore_resources = ti.xcom_pull(task_ids="upload_filestore_resources")
    package = ckan_utils.get_package(ckan=CKAN, package_id=PACKAGE_ID)

    results = []
    for i in upload_filestore_resources:
        if "error" in i:
            results.append(i)
            continue

        try:
            resource = [
                r for r in package["resources"] if r["name"] == i["target_resource"]
            ][0]
            ckan_utils.update_resource_last_modified(
                ckan=CKAN,
                resource_id=resource["id"],
                new_last_modified=parser.parse(i["file_last_modified"]),
            )
            logging.info(
                "Updated last modified to {} for {}".format(
                    i["file_last_modified"], i["target_resource"]
                )
            )
            results.append(i)
        except Exception as e:
            err = f"Could not update last modified date. Error: {e}"
            logging.error(err)
            results.append({**i, "error": err})

    for i in insert_datastore_resources:
        if "error" in i:
            results.append(i)
            continue

        try:
            resource = [
                r for r in package["resources"] if r["name"] == i["target_resource"]
            ][0]
            ckan_utils.update_resource_last_modified(
                ckan=CKAN,
                resource_id=resource["id"],
                new_last_modified=parser.parse(i["file_last_modified"]),
            )
            logging.info(
                "Updated last modified to {} for {}".format(
                    i["file_last_modified"], i["target_resource"]
                )
            )
            results.append(i)
        except Exception as e:
            err = f"Could not update last modified date. Error: {e}"
            logging.error(err)
            results.append({**i, "error": err})

    return results


def build_message(**kwargs):
    ti = kwargs.pop("ti")
    resources_to_load = ti.xcom_pull(task_ids="identify_resources_to_load")
    update_resource_last_modified = ti.xcom_pull(
        task_ids="update_resource_last_modified"
    )

    if len(resources_to_load) == 0:
        return "There aren't any new resources to load"

    successes = []
    errors = []

    for i in update_resource_last_modified:
        if "error" not in i:
            successes.append(i["target_resource"])
        else:
            errors.append(f"{i['target_resource']}: {i['error']}")

    for i in resources_to_load:
        if "error" in i:
            errors.append(f"{i['target_resource']}: {i['error']}")

    msg = ""
    if len(successes) > 0:
        msg = msg + "*Updated*\n- {}".format("\n- ".join(successes))

    if len(errors) > 0:
        msg = msg + "\n*Errors*\n- {}".format("\n- ".join(errors))

    return msg


def return_branch(**kwargs):
    resources_to_load = kwargs["ti"].xcom_pull(task_ids="identify_resources_to_load")

    if len(resources_to_load) == 0:
        return "no_files_are_not_new"

    return "yes_continue_with_refresh"


default_args = airflow_utils.get_default_args(
    {
        "on_failure_callback": send_failure_msg,
        "start_date": job_settings["start_date"],
        "retries": 0,
        # "retry_delay": timedelta(minutes=3),
    }
)

with DAG(
    JOB_NAME,
    default_args=default_args,
    description=job_settings["description"],
    schedule_interval=job_settings["schedule"],
    catchup=False,
) as dag:

    create_tmp_dir = PythonOperator(
        task_id="create_tmp_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": JOB_NAME, "dir_variable_name": "tmp_dir"},
    )

    extract = PythonOperator(
        task_id="get_raw_files", python_callable=get_raw_files, provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform_data_files",
        python_callable=transform_data_files,
        provide_context=True,
    )

    package = PythonOperator(
        task_id="get_package",
        python_callable=CKAN.action.package_show,
        op_kwargs={"id": PACKAGE_ID},
    )

    resources_to_load = PythonOperator(
        task_id="identify_resources_to_load",
        python_callable=identify_resources_to_load,
        provide_context=True,
    )

    are_there_new_files = BranchPythonOperator(
        task_id="are_there_new_files",
        provide_context=True,
        python_callable=return_branch,
    )

    no_files_are_not_new = DummyOperator(task_id="no_files_are_not_new")

    continue_with_refresh = DummyOperator(task_id="yes_continue_with_refresh")

    datastore_resources = PythonOperator(
        task_id="insert_datastore_resources",
        python_callable=insert_datastore_resources,
        provide_context=True,
    )

    filestore_resources = PythonOperator(
        task_id="upload_filestore_resources",
        python_callable=upload_filestore_resources,
        provide_context=True,
    )

    update_last_modified = PythonOperator(
        task_id="update_resource_last_modified",
        python_callable=update_resource_last_modified,
        provide_context=True,
    )

    notification_msg = PythonOperator(
        task_id="build_message",
        python_callable=build_message,
        provide_context=True,
        trigger_rule="none_failed",
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=send_success_msg,
        provide_context=True,
    )

    # delete_tmp_dir = PythonOperator(
    #     task_id="delete_tmp_dir",
    #     python_callable=airflow_utils.delete_tmp_data_dir,
    #     op_kwargs={"dag_id": JOB_NAME, "recursively": True},
    # )

    create_tmp_dir >> extract >> transform >> resources_to_load

    package >> resources_to_load >> are_there_new_files

    are_there_new_files >> continue_with_refresh >> [
        datastore_resources,
        filestore_resources,
    ]

    [
        datastore_resources,
        filestore_resources,
    ] >> update_last_modified >> notification_msg

    are_there_new_files >> no_files_are_not_new >> notification_msg

    notification_msg >> send_notification
