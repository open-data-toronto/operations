import logging
import os
import tempfile
from copy import deepcopy
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
from dateutil import parser
from utils import airflow_utils, ckan_utils

PACKAGE_ID = Path(os.path.abspath(__file__)).name.replace(".py", "")
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
            "start_date": datetime(2020, 11, 10, 13, 35, 0),
        }
    ),
    description="Get rain gauge data from the last time it was loaded to now",
    schedule_interval="30 14 * * *",
    catchup=False,
) as dag:

    CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
    CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])

    def send_success_msg(**kwargs):
        msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
        airflow_utils.message_slack(
            name=PACKAGE_ID,
            message_type="success",
            msg=msg,
            active_env=ACTIVE_ENV,
            prod_webhook=ACTIVE_ENV == "prod",
        )

    def api_request(**kwargs):
        path = kwargs.pop("path")
        key = kwargs.pop("key")

        logging.info(f"API call: {path}")
        APIKEY = Variable.get("flowworks_apikey")

        req = requests.get(
            f"https://developers.flowworks.com/fwapi/v1/{APIKEY}/{path}"
        ).json()

        assert "request ok" in req["msg"].lower(), f"{path}: {req['msg']}"

        return req[key]

    def identify_newest_resource(**kwargs):
        package = kwargs.pop("ti").xcom_pull(task_ids="get_package")
        resources = [r for r in package["resources"] if r["format"].lower() == "csv"]

        resource_years = []
        for r in resources:
            year = "".join([s for s in r["name"] if s.isdigit()])
            resource_years.append(int(year))

        resource = [r for r in resources if str(max(resource_years)) in r["name"]][0]

        return resource

    def get_resource_data(**kwargs):
        ti = kwargs.pop("ti")
        resource = ti.xcom_pull(task_ids="identify_newest_resource")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
        file_content = requests.get(resource["url"]).content
        filename = "resource_data"
        filepath = tmp_dir / f"{filename}.csv"

        pd.read_csv(BytesIO(file_content)).to_csv(filepath, index=False)

        return filepath

    def get_from_timestamp(**kwargs):
        ti = kwargs.pop("ti")
        filepath = ti.xcom_pull(task_ids="get_resource_data")
        resource_data = pd.read_csv(filepath)

        time_lastest_loaded = parser.parse(resource_data["date"].max())

        start_date = (time_lastest_loaded + timedelta(seconds=1)).strftime(
            "%Y%m%d%H%M%S"
        )

        return start_date

    def get_to_timestamp():
        return datetime.now().strftime("%Y%m%d%H%M%S")

    def get_site_datapoints(**kwargs):
        ti = kwargs.pop("ti")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
        rain_gauge_sites = ti.xcom_pull(task_ids="get_rain_gauge_sites")
        start_date = ti.xcom_pull(task_ids="get_from_timestamp")
        to_date = ti.xcom_pull(task_ids="get_to_timestamp")

        filename = "site_datapoints"
        filepath = tmp_dir / f"{filename}.csv"

        def get_datapoints(site):
            site = deepcopy(site)
            del site["channels"]
            channels = api_request(path=f"site/{site['id']}", key="sites")[0][
                "channels"
            ]
            rainfall_channels = [c for c in channels if c["name"].lower() == "rainfall"]

            assert len(rainfall_channels) == 1, "{} rainfall channels".format(
                len(rainfall_channels)
            )
            channel = rainfall_channels[0]

            datapoints = api_request(
                path="site/{}/channel/{}/data/startdate/{}/todate/{}".format(
                    site["id"], channel["id"], start_date, to_date
                ),
                key="datapoints",
            )
            logging.info(
                "Site: {} / Channel: {} / Records: {}".format(
                    site["name"], channel["id"], len(datapoints)
                )
            )
            for d in datapoints:
                d.update(site)
                d["rainfall"] = d.pop("value")

            return datapoints

        records_to_load = []
        for site in rain_gauge_sites:
            records_to_load.extend(get_datapoints(site=site))

        pd.DataFrame(records_to_load).to_csv(filepath, index=False)

        return filepath

    def update_resource_data(**kwargs):
        ti = kwargs.pop("ti")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
        datapoints_fp = ti.xcom_pull(task_ids="get_site_datapoints")
        resource_data_fp = ti.xcom_pull(task_ids="get_resource_data")
        filename = "data_to_load"
        filepath = tmp_dir / f"{filename}.csv"

        datapoints = pd.read_csv(datapoints_fp)
        resource_data = pd.read_csv(resource_data_fp)

        data_to_load = resource_data.append(datapoints)

        data_to_load.to_csv(filepath, index=False)

        return filepath

    def upload_yearly_resources(**kwargs):
        ti = kwargs.pop("ti")
        start_date = ti.xcom_pull(task_ids="get_from_timestamp")
        to_date = ti.xcom_pull(task_ids="get_to_timestamp")
        data_to_load_fp = ti.xcom_pull(task_ids="update_resource_data")
        package = ti.xcom_pull(task_ids="get_package")

        data_to_load = pd.read_csv(data_to_load_fp)
        time_lastest_loaded = datetime.strptime(start_date, "%Y%m%d%H%M%S")
        time_now = datetime.strptime(to_date, "%Y%m%d%H%M%S")

        years_to_load = [
            str(x) for x in range(time_lastest_loaded.year, time_now.year + 1)
        ]

        for year in years_to_load:
            year_data = data_to_load[data_to_load["date"].str.startswith(year)]
            resource_name = f"precipitation-data-{year}"

            resource_id = None
            for r in package["resources"]:
                if r["name"] == resource_name:
                    resource_id = r["id"]
                    break

            with tempfile.TemporaryDirectory() as tmp:
                folder = Path(tmp)
                path = folder / f"{resource_name}.csv"
                year_data.to_csv(path, index=False)

                if resource_id is None:
                    logging.info(f"resource does not exist for {year}")
                    new_resource = CKAN.action.resource_create(
                        package_id=package["id"],
                        name=resource_name,
                        format="csv",
                        upload=open(path, "rb"),
                    )
                    logging.info(
                        f"created {new_resource['name']} with {data_to_load.shape[0]} rows"
                    )
                else:
                    logging.info("resource for the year exists")
                    CKAN.action.resource_patch(
                        id=resource_id, upload=open(path, "rb"),
                    )
                    logging.info(f"updated resource: {r['name']}")

    def build_message(**kwargs):
        ti = kwargs.pop("ti")
        datapoints_fp = ti.xcom_pull(task_ids="get_site_datapoints")
        datapoints = pd.read_csv(datapoints_fp)
        start_date = ti.xcom_pull(task_ids="get_from_timestamp")
        time_lastest_loaded = datetime.strptime(start_date, "%Y%m%d%H%M%S").strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        return f"{datapoints.shape[0]} new records found since {time_lastest_loaded}"

    def return_branch(**kwargs):
        filepath = kwargs.pop("ti").xcom_pull(task_ids="get_site_datapoints")
        df = pd.read_csv(filepath)

        if df.shape[0] == 0:
            return "no_need_for_notification"

        return "update_resource_data"

    package = PythonOperator(
        task_id="get_package",
        op_kwargs={"ckan": CKAN, "package_id": PACKAGE_ID},
        python_callable=ckan_utils.get_package,
    )

    create_tmp_dir = PythonOperator(
        task_id="create_tmp_data_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": PACKAGE_ID, "dir_variable_name": "tmp_dir"},
    )

    newest_resource = PythonOperator(
        task_id="identify_newest_resource",
        python_callable=identify_newest_resource,
        provide_context=True,
    )

    resource = PythonOperator(
        task_id="get_resource_data",
        python_callable=get_resource_data,
        provide_context=True,
    )

    from_timestamp = PythonOperator(
        task_id="get_from_timestamp",
        python_callable=get_from_timestamp,
        provide_context=True,
    )

    to_timestamp = PythonOperator(
        task_id="get_to_timestamp", python_callable=get_to_timestamp,
    )

    rain_gauge_sites = PythonOperator(
        task_id="get_rain_gauge_sites",
        python_callable=api_request,
        op_kwargs={"path": "sites", "key": "sites"},
    )

    datapoints = PythonOperator(
        task_id="get_site_datapoints",
        python_callable=get_site_datapoints,
        provide_context=True,
    )

    branching = BranchPythonOperator(
        task_id="branching", provide_context=True, python_callable=return_branch,
    )

    update_data = PythonOperator(
        task_id="update_resource_data",
        python_callable=update_resource_data,
        provide_context=True,
    )

    upload_resources = PythonOperator(
        task_id="upload_yearly_resources",
        python_callable=upload_yearly_resources,
        provide_context=True,
    )

    msg = PythonOperator(
        task_id="build_message", python_callable=build_message, provide_context=True,
    )

    send_notification = PythonOperator(
        task_id="send_success_msg",
        python_callable=send_success_msg,
        provide_context=True,
    )

    delete_original_resource_tmp = PythonOperator(
        task_id="delete_original_resource_tmp_file",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["get_resource_data"]},
        provide_context=True,
        trigger_rule="one_success",
    )

    delete_new_records_tmp = PythonOperator(
        task_id="delete_new_records_tmp_file",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["get_site_datapoints"]},
        provide_context=True,
    )

    delete_new_resource_tmp = PythonOperator(
        task_id="delete_new_resource_tmp",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["update_resource_data"]},
        provide_context=True,
    )

    delete_tmp_dir = PythonOperator(
        task_id="delete_tmp_data_dir",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": PACKAGE_ID},
    )

    no_notification = DummyOperator(task_id="no_need_for_notification")

    [package, create_tmp_dir] >> newest_resource >> resource
    resource >> from_timestamp
    resource >> to_timestamp
    [from_timestamp, to_timestamp, rain_gauge_sites] >> datapoints
    datapoints >> branching
    branching >> no_notification
    branching >> update_data >> upload_resources >> msg >> send_notification

    no_notification >> delete_original_resource_tmp
    update_data >> delete_original_resource_tmp
    upload_resources >> delete_new_resource_tmp
    msg >> delete_new_records_tmp

    [
        delete_original_resource_tmp,
        delete_new_resource_tmp,
        delete_new_records_tmp,
    ] >> delete_tmp_dir
