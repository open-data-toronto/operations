from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from distutils.dir_util import copy_tree
from datetime import datetime, timedelta
from airflow.models import Variable
from pathlib import Path
from airflow import DAG
import pandas as pd
import logging
import calendar
import requests
import ckanapi
import zipfile
import shutil
import json
import sys
import os

sys.path.append(Variable.get("repo_dir"))
from utils import airflow as airflow_utils  # noqa: E402
from utils import ckan as ckan_utils  # noqa: E402

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = JOB_FILE.name[:-3]
PACKAGE_ID = JOB_NAME.replace("_", "-")

dags = [
    {
        "period_range": "weekly",
        "dag_id": f"{JOB_NAME}_weekly",
        "description": "Gets weekly Oracle Infinity data and uploads to web-analytics",
        "schedule": "@once",
        "start_date": datetime(2020, 11, 10, 13, 35, 0),
        "resource_name": "web-analytics-weekly-report",
    },
    {
        "period_range": "monthly",
        "dag_id": f"{JOB_NAME}_monthly",
        "description": "Gets monthly Oracle Infinity data and uploads to web-analytics",
        "schedule": "@once",
        "start_date": datetime(2020, 11, 10, 13, 35, 0),
        "resource_name": "web-analytics-monthly-report",
    },
    # {
    #     "period_range": "yearly",
    #     "dag_id": f"{JOB_NAME}_yearly".
    #     "description": "Gets yearly Oracle Infinity data & uploads to web-analytics",
    #     "schedule": "@once",
    #     "start_date": datetime(2020, 11, 10, 13, 35, 0),
    # "resource_name": "web-analytics-yearly-report"
    # }
]

# active_env = Variable.get("active_env")
active_env = "dev"
ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
ckan = ckanapi.RemoteCKAN(**ckan_creds[active_env])


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

        dates_loaded = []
        for f in os.listdir(data_fp):
            time_range = f.split(".")[0]
            end_date = datetime.strptime(time_range, filename_date_format)
            dates_loaded.append(end_date)

        if not dates_loaded:
            return datetime(2016, 1, 1)

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

                end = begin + timedelta(days=6)
                begin = end + timedelta(days=1)

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

            begin = latest_loaded + timedelta(days=365)
            end = datetime(begin.year, 12, 31)

            while end < datetime.now():
                periods_to_load.append(
                    {
                        "begin": datetime.strftime(begin, "%Y/1/1/0"),
                        "end": datetime.strftime(end, "%Y/12/31/23"),
                    }
                )

                begin = begin + timedelta(days=365)
                end = datetime(begin.year, 12, 31)

            return periods_to_load

        if period_range == "weekly":
            return weeks(latest_loaded)
        elif period_range == "monthly":
            return months(latest_loaded)
        elif period_range == "yearly":
            return years(latest_loaded)

    def extract_new_reports(**kwargs):
        ti = kwargs.pop("ti")
        periods_to_load = ti.xcom_pull(task_ids="calculate_periods_to_load")
        filename_date_format = ti.xcom_pull(task_ids="get_filename_date_format")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
        dest_path = tmp_dir / "new"

        account_id = Variable.get("oracle_infinity_account_id")
        user = Variable.get("oracle_infinity_user")
        password = Variable.get("oracle_infinity_password")

        reports = {
            "Key Metrics": "x5vawmkc4m",
            "New vs. Return Visitors": "xmg3h9vx0q",
            "Hits by Hour of Day": "he4my9hqm3",
            "Visits by Day of Week": "b306ez6xl9",
            "Operating System Platform": "gdr9cnxhhe",
            "Browser": "aq6la9qe1y",
            "Screen Resolution": "kxol0nmtp7",
            "Mobile Devices": "k39zenjrov",
            "Mobile Browser": "c3vhba0tbr",
            "Referring Site": "zu2w468s89",
            "Search Engines": "k8vzq4e8go",
            "Countries": "m73c1tcrhq",
            "Cities": "q0armw9day",
            "Top Pages": "mpv8f1ox49",
            "Entry Pages": "jw6dgdkl7b",
            "Exit Pages": "donxi2vw5x",
            "File Downloads": "t8rzu1nm32",
            "Email Address": "xes0swn7f4",
            "Offsite Links": "l0y5yfde3v",
            "Anchor Tags": "mswxaifo96",
        }

        args = {
            "format": "json",
            "timezone": "America/New_York",
            "suppressErrorCodes": "true",
            "autoDownload": "true",
            "download": "false",
            "totals": "true",
            "limit": "250",
        }

        logging.info(f"Getting reports. Parameters: {args}")

        def generate(report_id, begin, end):

            qs = "&".join(["{}={}".format(k, v) for k, v in args.items()])

            prefix = f"https://api.oracleinfinity.io/v1/account/{account_id}/dataexport"

            call = f"{prefix}/{reports[report_id]}/data?begin={begin}&end={end}&{qs}"

            logging.info(f"Begin: {begin} | End: {end} | {report_id.upper()} | {call}")

            response = requests.get(call, auth=(user, password))

            status_code = response.status_code

            assert (
                status_code == 200
            ), f"Response code: {status_code}. Reason: {response.reason}"

            return response

        def convert(response):
            report = response.json()

            try:
                report = response.json()

                rows = [{x["guid"]: x["value"] for x in report["measures"]}]

                for dim in report["dimensions"]:
                    row = {dim["guid"]: dim["value"]}

                    for m in dim["measures"]:
                        row[m["guid"]] = m["value"]
                    rows.append(row)

                columns = rows[-1].keys()

                return pd.DataFrame(rows, columns=columns)

            except Exception:
                raise f"Parsing issue with call content: {json.dumps(report)}"

        dirs = []

        for period in periods_to_load:
            period_path_name = datetime.strptime(period["end"], "%Y/%m/%d/%H").strftime(
                filename_date_format
            )
            period_path = dest_path / period_path_name
            period_path.mkdir(parents=True, exist_ok=True)
            dirs.append(period_path)

            for report_id in list(reports.keys()):
                response = generate(
                    report_id=report_id, begin=period["begin"], end=period["end"]
                )

                fpath = period_path / (report_id + ".csv")
                convert(response).to_csv(fpath, index=False)

        return dirs

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

    def zip_new_reports(**kwargs):
        ti = kwargs.pop("ti")
        file_directories = [
            Path(f) for f in ti.xcom_pull(task_ids="extract_new_reports")
        ]
        dest_dir = Path(ti.xcom_pull(task_ids="make_staging_folder"))

        paths = []
        for fpath in file_directories:
            f = shutil.make_archive(
                base_name=dest_dir / fpath.name, format="zip", root_dir=fpath
            )
            paths.append(Path(f))

        return paths

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
        resource = Path(ti.xcom_pull(task_ids="get_resource"))

        res = ckan.action.resource_patch(
            id=resource["id"],
            upload=open(path, "rb"),
        )

        return res

    def build_message(**kwargs):
        ti = kwargs.pop("ti")
        resource_name = kwargs["resource_name"]
        periods_to_load = ti.xcom_pull(task_ids="calculate_periods_to_load")

        msg = [f"Loaded new periods to {resource_name}:", ""]

        for p in periods_to_load:
            begin = "-".join(p["begin"].split("/")[:-1])
            end = "-".join(p["end"].split("/")[:-1])

            msg.append(f" * {begin} to {end}")

        return "\n".join(msg)

    dag = DAG(
        d["dag_id"],
        default_args=airflow_utils.get_default_args(
            {
                "on_failure_callback": send_failure_msg,
                "start_date": d["start_date"],
                "retries": 0,
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
            task_id="unzip_data",
            python_callable=unzip_data,
            provide_context=True,
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

        new_reports = PythonOperator(
            task_id="extract_new_reports",
            python_callable=extract_new_reports,
            provide_context=True,
        )

        zip_reports = PythonOperator(
            task_id="zip_new_reports",
            python_callable=zip_new_reports,
            provide_context=True,
        )

        copy_previous = PythonOperator(
            task_id="copy_previous_to_staging",
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
            task_id="upload_zip",
            python_callable=upload_zip,
            provide_context=True,
        )

        msg = PythonOperator(
            task_id="build_message",
            python_callable=build_message,
            provide_context=True,
            op_kwargs={"resource_name": d["resource_name"]},
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

        [
            create_resource,
            no_new_resource,
        ] >> resource >> get_data >> unzip_files

        [unzip_files, filename_date_format] >> latest_loaded

        latest_loaded >> periods_to_load >> new_data_to_load

        [new_reports, staging_folder] >> zip_reports

        [copy_previous, zip_reports] >> zip_resource_files >> upload_data >> msg

        create_tmp_dir >> staging_folder

        create_tmp_dir >> get_data

        new_data_to_load >> no_new_periods_to_load

        new_data_to_load >> new_periods_to_load >> [new_reports, copy_previous]

        msg >> send_notification

        [send_notification, no_new_periods_to_load] >> delete_tmp_dir

    return dag


for d in dags:
    dag_id = d["dag_id"]

    globals()[dag_id] = create_dag(d)