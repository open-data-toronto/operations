import ckanapi
import requests
import logging
import calendar
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from distutils.dir_util import copy_tree
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from ckan_operators.package_operator import GetOrCreatePackageOperator

from utils import airflow_utils, ckan_utils
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator


# init hardcoded vars for these dags
active_env = Variable.get("active_env")
ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
ckan_address = ckan_creds[active_env]["address"]
ckan_apikey = ckan_creds[active_env]["apikey"]

ckan = ckanapi.RemoteCKAN(**ckan_creds[active_env])


reports = {
    "Page Views": "acd19558f6734dfc187e1add2680e287",
    "Page URL and File Clicks": "4213a24217ec4d77c8ec441bed6cc86e",
    "Search Terms": "tb9gvo6qd7",
}

args = {
    "format": "csv",
    "timezone": "America/New_York",
    "suppressErrorCodes": "true",
    "autoDownload": "true",
    "download": "false",
    "totals": "true",
    "limit": "400",
}

package_name = "portal-analytics"

package_metadata = {'title': 'Portal Analytics', 
                    'date_published': '2023-01-13', 
                    'refresh_rate': 'Monthly', 
                    'dataset_category': 'Document', 
                    'owner_division': 'Information & Technology', 
                    'owner_section': None, 
                    'owner_unit': None, 
                    'owner_email': 'opendata@toronto.ca', 
                    'civic_issues': None, 
                    'topics': 'City government', 
                    'tags': [{'name': 'analytics', 'vocabulary_id': None}, {'name': 'statistics', 'vocabulary_id': None}, {'name': 'portal analytics', 'vocabulary_id': None}, {'name': 'visits', 'vocabulary_id': None}], 
                    'information_url': None, 
                    'excerpt': "This dataset contains portal analytics (statistics) capturing visitors' usage of datasets published on the Open Data Portal.", 
                    'limitations': None,
                    'notes': "\r\n\r\nThis dataset contains portal analytics (statistics) capturing visitors' usage of datasets published on the City of Toronto [Open Data Portal](https://open.toronto.ca/catalogue/).\r\n"}

with DAG(
    'portal-analytics',
    default_args={
                "owner": "Yanan",
                "depends_on_past": False,
                "email": "yanan.zhang@toronto.ca",
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": 3,
                "on_failure_callback": task_failure_slack_alert,
                "tags": ["sustainment"]
            },
    description='Report stats for datasets on Open Data Portal',
    schedule_interval="@once",
    start_date=datetime(2023, 1, 10, 0, 0, 0),
    catchup = False,
) as dag:

    # Generate summarization report
    def generate_summary_report(report_name, report_id, begin, end, account_id, user, password):
        
        # generte oracle infinity API call
        qs = "&".join(["{}={}".format(k, v) for k, v in args.items()])
        prefix = f"https://api.oracleinfinity.io/v1/account/{account_id}/dataexport"

        call = f"{prefix}/{report_id}/data?begin={begin}&end={end}&{qs}"
        logging.info(f"Begin: {begin} | End: {end} | {report_name.upper()} | {call}")

        response = requests.get(call, auth=(user, password))
        status = response.status_code

        assert status == 200, f"Response code: {status}. Reason: {response.reason}"

        return response
    
    def is_resource_new(**kwargs):
        package = kwargs.pop("ti").xcom_pull(task_ids="get_or_create_package")
        resource_name = kwargs.pop("resource_name")
        
        if package["resources"] is None:
            return "create_new_resource"
        else:
            resource = [r for r in package["resources"] if r["name"] == resource_name]

            if len(resource) == 1:
                return "do_not_create_new_resource"
            else:
                return "create_new_resource"

    def get_resource(**kwargs):
        package = ckan.action.package_show(
            id=package_name,
        )
        resource_name = kwargs.pop("resource_name")

        resource = [r for r in package["resources"] if r["name"] == resource_name][0]
        logging.info(f"----resource list---{resource}-----")

        return resource

    def create_new_resource(**kwargs):
        ti = kwargs.pop("ti")
        package = ti.xcom_pull(task_ids="get_or_create_package")
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
            ):  
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

        if period_range == "monthly":
            filename_date_format = "%Y%m"

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
            return datetime(2020, 12, 2)

        return max(dates_loaded)
    
    # calculate months to load
    def calculate_periods_to_load(**kwargs):
        ti = kwargs.pop("ti")
        latest_loaded = ti.xcom_pull(task_ids="determine_latest_period_loaded")
        period_range = kwargs["period_range"]

        def months(latest_loaded):
            logging.info("Calculating months to load")
            periods_to_load = []

            logging.info(f"--------{latest_loaded}--------")
            begin = latest_loaded + timedelta(days=32)
            month_end_day = calendar.monthrange(begin.year, begin.month)[1]
            end = datetime(begin.year, begin.month, month_end_day)
            logging.info(f"-------{begin}--{month_end_day}--{end}--")

            while end < datetime.now():
                periods_to_load.append(
                    {
                        "begin": datetime.strftime(begin, "%Y/%m/1/0"),
                        "end": datetime.strftime(end, "%Y/%m/%d/23"),
                    }
                )

                begin = begin + timedelta(days=31)

                if begin.month == 12:
                    end = datetime(begin.year, begin.month, 31)
                else:
                    end = datetime(begin.year, begin.month + 1, 1) + timedelta(days=-1)
                
            return periods_to_load
        
        return months(latest_loaded)
    
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

            response = generate_summary_report(
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
        logging.info(f"------------{file_paths}--------")

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
        period_range = kwargs["period_range"]

        msg = [f"Loaded portal {period_range} data:", ""]

        for p in periods_to_load:
            begin = "-".join(p["begin"].split("/")[:-1])
            end = "-".join(p["end"].split("/")[:-1])

            msg.append(f"- {begin} to {end}")

        return "\n".join(msg)

    def send_success_msg(**kwargs):
        msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
        airflow_utils.message_slack(
            name=package_name,
            message_type="success",
            msg=msg,
            prod_webhook=active_env == "prod",
            active_env=active_env,
        )

    def send_failure_msg(self):
        airflow_utils.message_slack(
            name=package_name,
            message_type="error",
            msg="Job not finished",
            prod_webhook=active_env == "prod",
            active_env=active_env,
        )
    
    
    # get or create package
    get_or_create_package = GetOrCreatePackageOperator(
        task_id = "get_or_create_package",
        address = ckan_address,
        apikey = ckan_apikey,
        package_name_or_id = package_name,
        package_metadata = package_metadata,
    )
    
    create_tmp_dir = PythonOperator(
        task_id="create_tmp_data_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": "portal-analytics", "dir_variable_name": "tmp_dir"},
        )

    is_resource_new_branch = BranchPythonOperator(
        task_id="is_resource_new",
        python_callable=is_resource_new,
        op_kwargs={"resource_name": "portal-analytics-monthly-report"},
    )

    create_resource = PythonOperator(
        task_id="create_new_resource",
        python_callable=create_new_resource,
        op_kwargs={"resource_name": "portal-analytics-monthly-report"},
    )

    no_new_resource = DummyOperator(task_id="do_not_create_new_resource")

    get_resource = PythonOperator(
        task_id="get_resource",
        python_callable=get_resource,
        trigger_rule="none_failed",
        op_kwargs={"resource_name": "portal-analytics-monthly-report"},
    )

    get_data = PythonOperator(
        task_id="download_data", python_callable=download_data,
    )

    unzip_files = PythonOperator(task_id="unzip_data", python_callable=unzip_data,)
    
    filename_date_format = PythonOperator(
        task_id="get_filename_date_format",
        python_callable=get_filename_date_format,
        op_kwargs={"period_range": "monthly"},
    )
    
    latest_loaded = PythonOperator(
        task_id="determine_latest_period_loaded",
        python_callable=determine_latest_period_loaded,
    )
    
    periods_to_load = PythonOperator(
        task_id="calculate_periods_to_load",
        python_callable=calculate_periods_to_load,
        op_kwargs={"period_range": "monthly"},
    )
    
    no_new_periods_to_load = DummyOperator(task_id="no_new_periods_to_load")

    new_periods_to_load = DummyOperator(task_id="new_periods_to_load")

    new_data_to_load = BranchPythonOperator(
        task_id="are_there_new_periods",
        python_callable=are_there_new_periods,
        op_kwargs={"resource_name": "portal-analytics-monthly-report"},
    )

    staging_folder = PythonOperator(
        task_id="make_staging_folder",
        python_callable=make_staging_folder,
        op_kwargs={"resource_name": "portal-analytics-monthly-report"},
    )
    
    extract_complete = DummyOperator(task_id="extract_complete")

    extract_new = PythonOperator(
        task_id="extract_new", python_callable=make_new_extract_folders,
    )
    
    page_views = PythonOperator(
        task_id="page_views",
        python_callable=extract_new_report,
        op_kwargs={"report_name": "Page Views"},
    )
    
    page_url_and_file_clicks = PythonOperator(
        task_id="page_url_and_file_clicks",
        python_callable=extract_new_report,
        op_kwargs={"report_name": "Page URL and File Clicks"},
    )
    
    search_terms = PythonOperator(
        task_id="search_terms",
        python_callable=extract_new_report,
        op_kwargs={"report_name": "Search Terms"},
    )
    
    copy_previous = PythonOperator(
        task_id="copy_previous", python_callable=copy_previous_to_staging,
    )

    zip_resource_files = PythonOperator(
        task_id="zip_files",
        python_callable=zip_files,
        op_kwargs={"resource_name": "portal-analytics-monthly-report"},
    )

    upload_data = PythonOperator(task_id="upload_zip", python_callable=upload_zip,)

    msg = PythonOperator(
        task_id="build_message", 
        python_callable=build_message,
        op_kwargs={"period_range": "monthly"},   
    )

    send_notification = PythonOperator(
        task_id="send_success_msg", python_callable=send_success_msg,
    )

    delete_tmp_dir = PythonOperator(
        task_id="delete_tmp_dir",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": "portal-analytics", "recursively": True},
        trigger_rule="none_failed",
    )
   
    # create task flow
    get_or_create_package  >> is_resource_new_branch

    is_resource_new_branch >> create_resource

    is_resource_new_branch >> no_new_resource

    [create_resource, no_new_resource] >> get_resource >> get_data >> unzip_files
    
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
        page_views,
        page_url_and_file_clicks,
        search_terms,
    ] >> extract_complete

    msg >> send_notification

    [send_notification, no_new_periods_to_load] >> delete_tmp_dir