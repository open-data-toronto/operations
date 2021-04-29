from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from distutils.dir_util import copy_tree
from datetime import datetime, timedelta
from airflow.models import Variable
from pathlib import Path
from airflow import DAG
import logging
import calendar
import requests
import ckanapi
import zipfile
import shutil
import os

from utils import airflow_utils
from utils import ckan_utils

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = JOB_FILE.name[:-3]
PACKAGE_ID = JOB_NAME.replace("_", "-")

dags = [
    {
        "period_range": "weekly",
        "dag_id": f"{JOB_NAME}_weekly",
        "description": "Gets weekly Oracle Infinity data and uploads to web-analytics",
        "schedule": "0 10 * * 7",
        "start_date": datetime(2000, 1, 1, 1, 1, 0),
        "resource_name": "web-analytics-weekly-report",
    },
    {
        "period_range": "monthly",
        "dag_id": f"{JOB_NAME}_monthly",
        "description": "Gets monthly Oracle Infinity data and uploads to web-analytics",
        "schedule": "0 12 1 * *",
        "start_date": datetime(2000, 1, 1, 1, 1, 0),
        "resource_name": "web-analytics-monthly-report",
    },
    {
        "period_range": "yearly",
        "dag_id": f"{JOB_NAME}_yearly",
        "description": "Gets yearly Oracle Infinity data & uploads to web-analytics",
        "schedule": "0 11 1 1 *",
        "start_date": datetime(2000, 1, 1, 1, 1, 0),
        "resource_name": "web-analytics-yearly-report",
    },
]

active_env = Variable.get("active_env")
ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
ckan = ckanapi.RemoteCKAN(**ckan_creds[active_env])

reports = {
    "Key Metrics": "828f5b9713ff44f2b25f1853fe6f3570",
    "New vs. Return Visitors": "yio034kvtm",
    "Hits by Hour of Day": "s5czhj1qsv",
    "Visits by Day of Week": "bc2pxestke",
    "Operating System Platform": "q9nspirva1",
    "Browser": "rvhr7yqhpm",
    "Screen Resolution": "povb8l7vf0",
    "Mobile Devices": "c5340efb15c34f27e36c96384da1b7b2",
    "Mobile Browser": "cf894819e3ed4fb77052752b93a77dd4",
    "Referring Site": "kdb5oyci75",
    "Search Engines": "e60c0172587e4d35be80004acd125621",
    "Countries": "m15hrmyqbh",
    "Cities": "o4jxiw73oh",
    "Top Pages": "mpv8f1ox49",
    "Entry Pages": "e84lg8gnx7",
    "Exit Pages": "mi3d3un8e2",
    "File Downloads": "c589ej49l7",
    "Email Address": "blluv2yufc",
    "Offsite Links": "86f96458a0c64ca29a75627c2283664d",
    "Anchor Tags": "655f7e85dd054330dde81d7d60e31b3a",
}

args = {
    "format": "csv",
    "timezone": "America/New_York",
    "suppressErrorCodes": "true",
    "autoDownload": "true",
    "download": "false",
    "totals": "true",
    "limit": "250",
}


def generate_report(report_name, report_id, begin, end, account_id, user, password):
    qs = "&".join(["{}={}".format(k, v) for k, v in args.items()])
    prefix = f"https://api.oracleinfinity.io/v1/account/{account_id}/dataexport"

    call = f"{prefix}/{report_id}/data?begin={begin}&end={end}&{qs}"
    logging.info(f"Begin: {begin} | End: {end} | {report_name.upper()} | {call}")

    response = requests.get(call, auth=(user, password))
    status = response.status_code

    assert status == 200, f"Response code: {status}. Reason: {response.reason}"

    return response


def create_dag(d):
    def send_success_msg(**kwargs):
        msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
        airflow_utils.message_slack(
            name=JOB_NAME,
            message_type="success",
            msg=msg,
            prod_webhook=active_env == "prod",
            active_env=active_env,
        )

    def send_failure_msg(self):
        airflow_utils.message_slack(
            name=JOB_NAME,
            message_type="error",
            msg="Job not finished",
            prod_webhook=active_env == "prod",
            active_env=active_env,
        )

    def is_resource_new(**kwargs):
        package = kwargs.pop("ti").xcom_pull(task_ids="get_package")
        resource_name = kwargs.pop("resource_name")

        resource = [r for r in package["resources"] if r["name"] == resource_name]

        assert (
            len(resource) <= 1
        ), f"Found {len(resource)} named {resource_name}. Must be 1 or 0."

        if len(resource) == 1:
            return "do_not_create_new_resource"

        return "create_new_resource"

    def get_resource(**kwargs):
        package = ckan_utils.get_package(ckan=ckan, package_id=PACKAGE_ID)
        resource_name = kwargs.pop("resource_name")

        resource = [r for r in package["resources"] if r["name"] == resource_name][0]

        return resource

    def create_new_resource(**kwargs):
        ti = kwargs.pop("ti")
        package = ti.xcom_pull(task_ids="get_package")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
        resource_name = kwargs.pop("resource_name")

        save_path = tmp_dir / f"{resource_name}.zip"

        with zipfile.ZipFile(save_path, "w") as file:
            file
            pass

        logging.info(
            "New resource. Creating empty placeholder Zip file to upload with resource"
        )

        res = ckan.action.resource_create(
            package_id=package["name"],
            name=resource_name,
            is_preview=False,
            format="ZIP",
            extract_job=f"Airflow: {kwargs['dag'].dag_id}",
            upload=open(save_path, "rb"),
        )

        logging.info(res)

        return save_path

    def download_data(**kwargs):
        ti = kwargs.pop("ti")
        resource = ti.xcom_pull(task_ids="get_resource")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))

        r = requests.get(resource["url"], stream=True)

        save_path = tmp_dir / f'src{Path(resource["url"]).suffix}'

        with open(save_path, "wb") as fd:
            for chunk in r.iter_content(
                chunk_size=128
            ):  # to-do: read up on chunk size here
                fd.write(chunk)

        return save_path

    def unzip_data(**kwargs):
        ti = kwargs.pop("ti")
        fp = ti.xcom_pull(task_ids="download_data")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))

        target_dir = tmp_dir / "src"

        with zipfile.ZipFile(fp, "r") as f:
            f.extractall(target_dir)
        if target_dir.exists() is False:
            target_dir.mkdir()

        return target_dir

    def get_filename_date_format(**kwargs):
        period_range = kwargs["period_range"]

        if period_range == "weekly":
            filename_date_format = "%Y%m%d"
        elif period_range == "monthly":
            filename_date_format = "%Y%m"
        elif period_range == "yearly":
            filename_date_format = "%Y"

        return filename_date_format

    def determine_latest_period_loaded(**kwargs):
        ti = kwargs.pop("ti")
        data_fp = Path(ti.xcom_pull(task_ids="unzip_data"))
        filename_date_format = ti.xcom_pull(task_ids="get_filename_date_format")

        dates_loaded = [
            datetime.strptime(p.name, filename_date_format)
            for p in data_fp.iterdir()
            if p.is_file() is False
        ]

        if not dates_loaded:
            return datetime(2018, 12, 30)

        return max(dates_loaded)

    def calculate_periods_to_load(**kwargs):
        ti = kwargs.pop("ti")
        latest_loaded = ti.xcom_pull(task_ids="determine_latest_period_loaded")
        period_range = kwargs["period_range"]

        def weeks(latest_loaded):
            logging.info("Calculating weeks to load")
            periods_to_load = []

            begin = latest_loaded + timedelta(days=1)
            end = begin + timedelta(days=6)

            while end < datetime.now():
                periods_to_load.append(
                    {
                        "begin": datetime.strftime(begin, "%Y/%m/%d/0"),
                        "end": datetime.strftime(end, "%Y/%m/%d/23"),
                    }
                )

                begin = end + timedelta(days=1)
                end = begin + timedelta(days=6)

            return periods_to_load

        def months(latest_loaded):
            logging.info("Calculating months to load")
            periods_to_load = []

            begin = latest_loaded + timedelta(days=32)
            month_end_day = calendar.monthrange(begin.year, begin.month)[1]
            end = datetime(begin.year, begin.month, month_end_day)

            while end < datetime.now():
                periods_to_load.append(
                    {
                        "begin": datetime.strftime(begin, "%Y/%m/1/0"),
                        "end": datetime.strftime(end, "%Y/%m/%d/23"),
                    }
                )

                begin = begin + timedelta(days=32)
                month_end_day = calendar.monthrange(begin.year, begin.month)[1]
                end = datetime(begin.year, begin.month, month_end_day)

            return periods_to_load

        def years(latest_loaded):
            logging.info("Calculating years to load")
            periods_to_load = []

            begin = datetime(latest_loaded.year + 1, 1, 1)
            end = datetime(begin.year, 12, 31)

            while end < datetime.now():
                periods_to_load.append(
                    {
                        "begin": datetime.strftime(begin, "%Y/1/1/0"),
                        "end": datetime.strftime(end, "%Y/12/31/23"),
                    }
                )

                begin = datetime(begin.year + 1, 1, 1)
                end = datetime(begin.year, 12, 31)

            return periods_to_load

        if period_range == "weekly":
            return weeks(latest_loaded)
        elif period_range == "monthly":
            return months(latest_loaded)
        elif period_range == "yearly":
            return years(latest_loaded)

    def make_new_extract_folders(**kwargs):
        logging.info("Created directory for storing extracts")

        ti = kwargs.pop("ti")
        filename_date_format = ti.xcom_pull(task_ids="get_filename_date_format")
        periods_to_load = ti.xcom_pull(task_ids="calculate_periods_to_load")
        dest_path = Path(ti.xcom_pull(task_ids="make_staging_folder"))

        dirs = []

        for period in periods_to_load:
            period_path_name = datetime.strptime(period["end"], "%Y/%m/%d/%H").strftime(
                filename_date_format
            )
            period_path = dest_path / period_path_name

            if period_path.exists() is False:
                period_path.mkdir()

            dirs.append(period_path)

            logging.info(period_path)

        return dirs

    def extract_new_report(**kwargs):
        ti = kwargs.pop("ti")
        periods_to_load = ti.xcom_pull(task_ids="calculate_periods_to_load")
        filename_date_format = ti.xcom_pull(task_ids="get_filename_date_format")
        dest_path = Path(ti.xcom_pull(task_ids="make_staging_folder"))

        account_id = Variable.get("oracle_infinity_account_id")
        user = Variable.get("oracle_infinity_user")
        password = Variable.get("oracle_infinity_password")

        report_name = kwargs["report_name"]
        report_id = reports[report_name]

        logging.info(f"Getting reports. Parameters: {args}")

        file_paths = []

        for period in periods_to_load:
            period_path_name = datetime.strptime(period["end"], "%Y/%m/%d/%H").strftime(
                filename_date_format
            )
            period_path = dest_path / period_path_name
            fpath = period_path / (report_name + ".csv")

            file_paths.append(fpath)

            response = generate_report(
                report_name=report_name,
                report_id=report_id,
                begin=period["begin"],
                end=period["end"],
                account_id=account_id,
                user=user,
                password=password,
            )

            with open(fpath, "wb") as f:
                f.write(response.content)

        return file_paths

    def are_there_new_periods(**kwargs):
        ti = kwargs.pop("ti")
        periods_to_load = ti.xcom_pull(task_ids="calculate_periods_to_load")

        if len(periods_to_load) > 0:
            return "new_periods_to_load"

        return "no_new_periods_to_load"

    def make_staging_folder(**kwargs):
        ti = kwargs.pop("ti")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
        resource_name = kwargs["resource_name"]

        staging = tmp_dir / resource_name

        staging.mkdir(parents=True, exist_ok=True)

        return staging

    def zip_files(**kwargs):
        ti = kwargs.pop("ti")
        resource_name = kwargs["resource_name"]
        dest_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
        staging_dir = Path(ti.xcom_pull(task_ids="make_staging_folder"))

        return shutil.make_archive(
            base_name=dest_dir / resource_name, format="zip", root_dir=staging_dir
        )

    def copy_previous_to_staging(**kwargs):
        ti = kwargs.pop("ti")
        from_dir = Path(ti.xcom_pull(task_ids="unzip_data"))
        dest_dir = Path(ti.xcom_pull(task_ids="make_staging_folder"))

        copy_tree(str(from_dir.absolute()), str(dest_dir.absolute()))

        return dest_dir

    def upload_zip(**kwargs):
        ti = kwargs.pop("ti")
        path = Path(ti.xcom_pull(task_ids="zip_files"))
        resource = ti.xcom_pull(task_ids="get_resource")

        res = ckan.action.resource_patch(id=resource["id"], upload=open(path, "rb"),)

        return res

    def build_message(**kwargs):
        ti = kwargs.pop("ti")
        periods_to_load = ti.xcom_pull(task_ids="calculate_periods_to_load")

        msg = [f"Loaded {d['period_range']} data:", ""]

        for p in periods_to_load:
            begin = "-".join(p["begin"].split("/")[:-1])
            end = "-".join(p["end"].split("/")[:-1])

            msg.append(f"- {begin} to {end}")

        return "\n".join(msg)

    dag = DAG(
        d["dag_id"],
        default_args=airflow_utils.get_default_args(
            {
                "on_failure_callback": send_failure_msg,
                "start_date": d["start_date"],
                "retries": 5,
                "retry_delay": timedelta(minutes=15),
            }
        ),
        description=d["description"],
        schedule_interval=d["schedule"],
        catchup=False,
    )

    with dag:

        package = PythonOperator(
            task_id="get_package",
            op_kwargs={"ckan": ckan, "package_id": PACKAGE_ID},
            python_callable=ckan_utils.get_package,
        )

        create_tmp_dir = PythonOperator(
            task_id="create_tmp_data_dir",
            python_callable=airflow_utils.create_dir_with_dag_name,
            op_kwargs={"dag_id": d["dag_id"], "dir_variable_name": "tmp_dir"},
        )

        is_resource_new_branch = BranchPythonOperator(
            task_id="is_resource_new",
            python_callable=is_resource_new,
            provide_context=True,
            op_kwargs={"resource_name": d["resource_name"]},
        )

        create_resource = PythonOperator(
            task_id="create_new_resource",
            python_callable=create_new_resource,
            provide_context=True,
            op_kwargs={"resource_name": d["resource_name"]},
        )

        no_new_resource = DummyOperator(task_id="do_not_create_new_resource")

        resource = PythonOperator(
            task_id="get_resource",
            python_callable=get_resource,
            trigger_rule="none_failed",
            op_kwargs={"resource_name": d["resource_name"]},
        )

        get_data = PythonOperator(
            task_id="download_data",
            python_callable=download_data,
            provide_context=True,
        )

        unzip_files = PythonOperator(
            task_id="unzip_data", python_callable=unzip_data, provide_context=True,
        )

        filename_date_format = PythonOperator(
            task_id="get_filename_date_format",
            python_callable=get_filename_date_format,
            op_kwargs={"period_range": d["period_range"]},
        )

        latest_loaded = PythonOperator(
            task_id="determine_latest_period_loaded",
            python_callable=determine_latest_period_loaded,
            provide_context=True,
        )

        periods_to_load = PythonOperator(
            task_id="calculate_periods_to_load",
            python_callable=calculate_periods_to_load,
            provide_context=True,
            op_kwargs={"period_range": d["period_range"]},
        )

        no_new_periods_to_load = DummyOperator(task_id="no_new_periods_to_load")

        new_periods_to_load = DummyOperator(task_id="new_periods_to_load")

        new_data_to_load = BranchPythonOperator(
            task_id="are_there_new_periods",
            python_callable=are_there_new_periods,
            provide_context=True,
            op_kwargs={"resource_name": d["resource_name"]},
        )

        staging_folder = PythonOperator(
            task_id="make_staging_folder",
            python_callable=make_staging_folder,
            provide_context=True,
            op_kwargs={"resource_name": d["resource_name"]},
        )

        extract_complete = DummyOperator(task_id="extract_complete")

        extract_new = PythonOperator(
            task_id="extract_new",
            python_callable=make_new_extract_folders,
            provide_context=True,
        )

        key_metrics = PythonOperator(
            task_id="key_metrics",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Key Metrics"},
        )

        new_v_return_visitors = PythonOperator(
            task_id="new_v_return_visitors",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "New vs. Return Visitors"},
        )

        hits_by_hour = PythonOperator(
            task_id="hits_by_hour",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Hits by Hour of Day"},
        )

        visits_by_day = PythonOperator(
            task_id="visits_by_day",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Visits by Day of Week"},
        )

        operating_system = PythonOperator(
            task_id="operating_system",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Operating System Platform"},
        )

        browser = PythonOperator(
            task_id="browser",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Browser"},
        )

        screen_resolution = PythonOperator(
            task_id="screen_resolution",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Screen Resolution"},
        )

        mobile_devices = PythonOperator(
            task_id="mobile_devices",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Mobile Devices"},
        )

        mobile_browser = PythonOperator(
            task_id="mobile_browser",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Mobile Browser"},
        )

        referring_site = PythonOperator(
            task_id="referring_site",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Referring Site"},
        )

        search_engines = PythonOperator(
            task_id="search_engines",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Search Engines"},
        )

        countries = PythonOperator(
            task_id="countries",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Countries"},
        )

        cities = PythonOperator(
            task_id="cities",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Cities"},
        )

        top_pages = PythonOperator(
            task_id="top_pages",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Top Pages"},
        )

        entry_pages = PythonOperator(
            task_id="entry_pages",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Entry Pages"},
        )

        exit_pages = PythonOperator(
            task_id="exit_pages",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Exit Pages"},
        )

        file_downloads = PythonOperator(
            task_id="file_downloads",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "File Downloads"},
        )

        email_address = PythonOperator(
            task_id="email_address",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Email Address"},
        )

        offsite_links = PythonOperator(
            task_id="offsite_links",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Offsite Links"},
        )

        anchor_tags = PythonOperator(
            task_id="anchor_tags",
            python_callable=extract_new_report,
            provide_context=True,
            op_kwargs={"report_name": "Anchor Tags"},
        )

        copy_previous = PythonOperator(
            task_id="copy_previous",
            python_callable=copy_previous_to_staging,
            provide_context=True,
        )

        zip_resource_files = PythonOperator(
            task_id="zip_files",
            python_callable=zip_files,
            provide_context=True,
            op_kwargs={"resource_name": d["resource_name"]},
        )

        upload_data = PythonOperator(
            task_id="upload_zip", python_callable=upload_zip, provide_context=True,
        )

        msg = PythonOperator(
            task_id="build_message",
            python_callable=build_message,
            provide_context=True,
        )

        send_notification = PythonOperator(
            task_id="send_success_msg",
            python_callable=send_success_msg,
            provide_context=True,
        )

        delete_tmp_dir = PythonOperator(
            task_id="delete_tmp_dir",
            python_callable=airflow_utils.delete_tmp_data_dir,
            op_kwargs={"dag_id": d["dag_id"], "recursively": True},
            trigger_rule="none_failed",
        )

        package >> is_resource_new_branch

        is_resource_new_branch >> create_resource

        is_resource_new_branch >> no_new_resource

        [create_resource, no_new_resource] >> resource >> get_data >> unzip_files

        [unzip_files, filename_date_format] >> latest_loaded

        latest_loaded >> periods_to_load >> new_data_to_load

        [copy_previous, extract_complete] >> zip_resource_files >> upload_data >> msg

        create_tmp_dir >> get_data

        new_data_to_load >> no_new_periods_to_load

        new_data_to_load >> new_periods_to_load >> staging_folder >> [
            extract_new,
            copy_previous,
        ]

        extract_new >> [
            key_metrics,
            new_v_return_visitors,
            hits_by_hour,
            visits_by_day,
            operating_system,
            browser,
            screen_resolution,
            mobile_devices,
            mobile_browser,
            referring_site,
            search_engines,
            countries,
            cities,
            top_pages,
            entry_pages,
            exit_pages,
            file_downloads,
            email_address,
            offsite_links,
            anchor_tags,
        ] >> extract_complete

        msg >> send_notification

        [send_notification, no_new_periods_to_load] >> delete_tmp_dir

    return dag


for d in dags:
    dag_id = d["dag_id"]

    globals()[dag_id] = create_dag(d)
