import csv
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import List

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

from utils import misc_utils
from ckan_operators.package_operator import GetOrCreatePackage
from ckan_operators.resource_operator import GetOrCreateResource
from utils_operators.slack_operators import task_failure_slack_alert, SlackWriter
from sustainment.portal_usage import portal_usage_utils

default_args = {
    "owner": "Yanan",
    "email": "yanan.zhang@toronto.ca",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": 3,
    "on_failure_callback": task_failure_slack_alert,
    "tags": ["sustainment"],
    "etl_mapping": [
        {
            "source": "https://api.oracleinfinity.io/",
            "target_package_name": "open-data-web-analytics",
            "target_resource_name": "portal-analytics-monthly-report",
        }
    ],
}


@dag(
    default_args=default_args,
    schedule="0 1 1 * *",
    start_date=datetime(2024, 11, 19),
    catchup=False,
)
def open_data_portal_usage():
    """Definition of open data portal statistics DAG"""

    ############################## Variables ########################
    dir_path = "/data/tmp/"
    package_name = "open-data-web-analytics"
    package_metadata = {
        "title": "Open Data Portal Usage",
        "date_published": "2023-01-13",
        "refresh_rate": "Monthly",
        "dataset_category": "Table",
        "owner_division": "Information & Technology",
        "owner_section": None,
        "owner_unit": None,
        "owner_email": "opendata@toronto.ca",
        "civic_issues": None,
        "topics": ["City government"],
        "tags": [
            {"name": "analytics", "vocabulary_id": None},
            {"name": "statistics", "vocabulary_id": None},
            {"name": "web analytics", "vocabulary_id": None},
            {"name": "open data", "vocabulary_id": None},
            {"name": "usage", "vocabulary_id": None},
        ],
        "information_url": None,
        "excerpt": "This dataset measures usership of the City of Toronto [Open Data Portal](https://open.toronto.ca).",
        "limitations": None,
        "notes": "This dataset measures usership of the City of Toronto [Open Data Portal](https://open.toronto.ca).",
    }

    ########################### Package Level Tasks ########################

    @task
    def create_tmp_dir(dag_id, dir_path):
        tmp_dir = misc_utils.create_dir_with_dag_id(dag_id, dir_path)
        return tmp_dir

    @task
    def get_or_create_package(package_name, package_metadata):

        package = GetOrCreatePackage(
            package_name=package_name,
            package_metadata=package_metadata,
        )
        return package.get_or_create_package()

    @task
    def determine_latest_period_loaded(**context):

        dir_path = Path(context["ti"].xcom_pull(task_ids="create_tmp_dir"))
        dates_loaded = portal_usage_utils.determine_latest_period_loaded(
            dir_path=dir_path
        )

        return str(dates_loaded)

    @task
    def calculate_periods_to_load(**context):

        latest_loaded = context["ti"].xcom_pull(
            task_ids="determine_latest_period_loaded"
        )

        latest_loaded = datetime.strptime(latest_loaded, "%Y-%m-%d %H:%M:%S")
        periods_to_load = portal_usage_utils.calculate_periods_to_load(latest_loaded)

        return periods_to_load

    @task.branch
    def does_report_need_update(periods_to_load: List[str]):
        """Determine if there is a need to generate the usage report."""
        if periods_to_load:
            return "get_or_create_package"
        else:
            return "no_new_reports_to_load"

    @task
    def build_message(periods_to_load: List[str]):
        """Build slack message"""
        if periods_to_load:
            msg = ["Loaded :opendata: portal monthly usage:"]
            for p in periods_to_load:
                begin = "-".join(p["begin"].split("/")[:-1])
                end = "-".join(p["end"].split("/")[:-1])
                msg.append(f"- {begin} to {end}")
            return "\n      :bar_chart:  ".join(msg)
        else:
            return "No new reports detected."

    @task(trigger_rule="none_failed")
    def send_to_slack(
        dag_id: str, message_header: str, message_content: str, message_body: str
    ):
        return SlackWriter(
            dag_id, message_header, message_content, message_body
        ).announce()

    # ----------------- Init Tasks -------------------------------
    create_tmp_dir = create_tmp_dir("open-data-portal-usage", dir_path)
    get_or_create_package = get_or_create_package(package_name, package_metadata)
    latest_period_loaded = determine_latest_period_loaded()
    periods_to_load = calculate_periods_to_load()
    no_new_reports_to_load = EmptyOperator(
        task_id="no_new_reports_to_load",
    )
    all_reports_inserted = EmptyOperator(
        task_id="all_reports_inserted",
        trigger_rule="none_failed_min_one_success",
    )
    does_report_need_update = does_report_need_update(periods_to_load)

    message_to_slack = send_to_slack(
        dag_id="open-data-portal-usage",
        message_header=":opendata: Open Data Portal Usage",
        message_content=build_message(periods_to_load),
        message_body="",
    )

    ########################### Report Level Tasks ########################
    tasks_list = {}
    report_list = list(portal_usage_utils.reports.keys())

    for report_name in report_list:
        report_label = report_name.replace(" ", "")

        @task(task_id="extract_report_" + report_label, pool="ckan_pool")
        def extract_report(report_name, report_label, **context):
            periods_to_load = context["ti"].xcom_pull(
                task_ids="calculate_periods_to_load"
            )
            dest_path = context["ti"].xcom_pull(task_ids="create_tmp_dir")

            return portal_usage_utils.extract_new_report(
                report_name, periods_to_load, dest_path
            )

        @task(task_id="get_or_create_resource_" + report_label, multiple_outputs=True)
        def get_or_create_resource(package_name, resource_name, report_label):

            ckan_resource = GetOrCreateResource(
                package_name=package_name,
                resource_name=resource_name,
                resource_attributes=dict(
                    format="csv",
                    is_preview=True,
                    url_type="datastore",
                    extract_job="Airflow: open-data-portal-usage",
                    url="placeholder",
                ),
            )

            return ckan_resource.get_or_create_resource()

        @task(task_id="insert_records_to_datastore_" + report_label, pool="ckan_pool")
        def insert_records_to_datastore(report_name, report_label, **context):
            """Insert records to ckan datastore by period.
            Returns:
                datastore: ckan datastore resource.
            """
            periods_to_load = context["ti"].xcom_pull(
                task_ids="calculate_periods_to_load"
            )
            resource = context["ti"].xcom_pull(
                task_ids="get_or_create_resource_" + report_label
            )
            dir_path = context["ti"].xcom_pull(task_ids="create_tmp_dir")
            ckan = misc_utils.connect_to_ckan()

            data_fields_descr = {
                "Page Views and Time Based Metrics": {
                    "Link Source -Page URL": "Page url the user visited.",
                    "Sessions": 'A session of continuous activity where all of a user’s events (hits) to the open data portal are recorded. The session starts the moment of the first event to the open data portal and continues until the session ends. A session ends at two hours, after 30 minutes of inactivity, or at a maximum of 5,000 events.Also referred to as "visits."',
                    "Users": 'The count of unique users who visited this page. Also referred to as "visitors."',
                    "Views": "An event to a URL designated as a page.",
                    "Avg Session Duration (Sec)": "Session duration is the difference in seconds between the time stamp of the first event in the session and the time stamp of the last event in session; and then it is averaged across all sessions. Sessions with a single page view (or bounced sessions) are excluded from this calculation.",
                    "Views per Session": "An average of pages viewed by a user during a session that demonstrates engagement.",
                    "Bounce Rate %": "A percentage of sessions where a user views a single page only and then exits.",
                    "Month": "The month and year of user activity.",
                },
                "File URL Clicks": {
                    "Link Tracking -URL Clicked": "Download url the user used to download a file from the dataset page. The url belongs to Toronto Open Data's middleware, CKAN, and is composed of ids for a dataset page (which CKAN calls a package) and a download on that page (which CKAN calls a resource).",
                    "Clicks": "Number of times users clicked to download a file.",
                    "Month": "The month and year of user activity.",
                    "Package Name": "Name of the CKAN package this file was associated at download, if any.",
                    "File Name": "The name of the file that was downloaded, if any, from CKAN",
                },
            }

            attributes = []
            text_type_fields = [
                "Link Source -Page URL",
                "Link Tracking -URL Clicked",
                "Month",
            ]
            for x in data_fields_descr[report_name].keys():
                attributes.append(
                    {
                        "id": x,
                        "type": "text" if x in text_type_fields else "float",
                        "info": {"notes": data_fields_descr[report_name][x]},
                    }
                )

            for period in periods_to_load:
                ym = period["begin"][:7].replace("/", "")
                with open(
                    dir_path + "/" + ym + "/" + report_name + "_" + ym + ".csv",
                    "r",
                ) as f:
                    reader = csv.DictReader(f)
                    records = list(reader)

                    for record in records:
                        record.update({"Month": ym})

                    # unique behavior for File URL Clicks dataset
                    if report_name == "File URL Clicks":
                        packages = ckan.action.package_search(rows=999)
                        packages = packages["results"]
                        package_id_dict = {p["id"]:p["name"] for p in packages}

                        for record in records:
                            if record["Link Tracking -URL Clicked"].startswith("ckan0.cf.opendata.inter."):
                                parts = record["Link Tracking -URL Clicked"].split("/")
                                package_id = parts[2]
                                file = parts[-1]
                                try:
                                    package_name = package_id_dict[package_id]
                                    record.update({"Package Name": package_name})
                                except:
                                    logging.warning(f"Failed to find {package_id}")
                                record.update({"File Name": file})

                    try:
                        ckan.action.datastore_create(
                            id=resource["id"],
                            records=records,
                            fields=attributes,
                            force=True,
                            do_not_cache=True,
                        )
                        logging.info(f"{len(records)} records inserted for period {ym}...")
                    except Exception as e:
                        # If failed to load records to datastore, delete the temp folder for that period
                        logging.exception(e)
                        logging.warning(
                            "Records failed to be inserted, cleaning temp folder.."
                        )
                        shutil.rmtree(dir_path + "/" + ym)

        @task(
            task_id="datastore_cache_" + report_label,
            pool="ckan_datastore_cache_pool",
        )
        def datastore_cache(report_label, **context):
            logging.info(f"Starting caching {report_label}")

            resource = context["ti"].xcom_pull(
                task_ids="get_or_create_resource_" + report_label
            )

            ckan = misc_utils.connect_to_ckan()
            return ckan.action.datastore_cache(resource_id=resource["id"])

        # -----------------Init Tasks------------------------
        tasks_list["extract_report_" + report_label] = extract_report(
            report_name=report_name, report_label=report_label
        )

        tasks_list["get_or_create_resource_" + report_label] = get_or_create_resource(
            package_name=package_name,
            resource_name=report_name,
            report_label=report_label,
        )

        tasks_list["insert_records_to_datastore_" + report_label] = (
            insert_records_to_datastore(
                report_name=report_name,
                report_label=report_label,
            )
        )

        tasks_list["datastore_cache_" + report_label] = datastore_cache(
            report_label=report_label
        )

        ####################### Task Flow ###########################

        (
            create_tmp_dir
            >> latest_period_loaded
            >> periods_to_load
            >> does_report_need_update
        )

        does_report_need_update >> Label("Update") >> get_or_create_package
        does_report_need_update >> Label("No Update") >> no_new_reports_to_load

        (
            get_or_create_package
            >> tasks_list["extract_report_" + report_label]
            >> tasks_list["get_or_create_resource_" + report_label]
            >> tasks_list["insert_records_to_datastore_" + report_label]
            >> tasks_list["datastore_cache_" + report_label]
            >> all_reports_inserted
            >> message_to_slack
        )


open_data_portal_usage()
