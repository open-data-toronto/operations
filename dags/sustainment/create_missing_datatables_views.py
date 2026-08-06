# create_missing_datatables_views.py
"""
Finds datastore-active resources that are missing a datatables_view and creates it.

This exists as a safety net: create_resource_views() only runs as a side effect of
datastore_cache's final resource_patch, so a resource can end up datastore_active
with no datatables_view if that step silently failed (e.g. res_format not covered
by ckan.datapusher.formats, geospatial cache errors, etc.) and never gets retried
by the ETL DAGs themselves.
"""
import ckanapi
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils_operators.slack_operators import task_failure_slack_alert, GenericSlackOperator
from ckan_operators.package_operator import GetAllPackagesOperator

from utils import airflow_utils

# standard arguments to be used throughout this DAG
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])

DEFAULT_ARGS = airflow_utils.get_default_args(
    {
        "owner": "Brendan",
        "depends_on_past": False,
        "email": ["brendan.schell@toronto.ca"],
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": task_failure_slack_alert,
        "retries": 0,
        "start_date": datetime(2026, 8, 6, 0, 0, 0)
    })

DESCRIPTION = "For each datastore-active resource in CKAN, ensure a datatables_view exists and create it if missing"
SCHEDULE = "0 6 * * *"  # once daily
TAGS = ["sustainment"]


def find_missing_datatables_views(**kwargs):
    output = []
    packages = kwargs.pop("ti").xcom_pull(task_ids="get_packages")["packages"]

    for package in packages:
        for resource in package["resources"]:
            if resource.get("datastore_active") != True:
                continue

            # skip resources CKAN can't return a view list for (eg deleted/orphaned) rather than failing the whole task
            try:
                views = CKAN.action.resource_view_list(id=resource["id"])
            except Exception:
                logging.exception("resource_view_list failed for resource {}".format(resource["id"]))
                continue

            if not any(view["view_type"] == "datatables_view" for view in views):
                output.append(resource["id"])

    logging.info("Found {} resources missing a datatables_view: {}".format(len(output), output))
    return {"output": output}


def create_missing_datatables_views(**kwargs):
    output = {}
    resource_ids = kwargs.pop("ti").xcom_pull(task_ids="find_missing_datatables_views")["output"]

    for resource_id in resource_ids:
        logging.info("Creating datatables_view for " + resource_id)
        try:
            CKAN.action.resource_view_create(
                resource_id=resource_id,
                title="Data Table",
                view_type="datatables_view",
                ellipsis_length=0,
                date_format="llll",
            )
            output[resource_id] = "created"
        except Exception:
            logging.exception("resource_view_create failed for resource {}".format(resource_id))
            output[resource_id] = "failed"

    if len(output):
        return {"output": output}


with DAG(
    "create_missing_datatables_views",
    description=DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE,
    tags=TAGS,
    catchup=False,
) as dag:

    get_packages = GetAllPackagesOperator(
        task_id="get_packages",
    )

    find_missing_datatables_views = PythonOperator(
        task_id="find_missing_datatables_views",
        python_callable=find_missing_datatables_views,
    )

    create_missing_datatables_views = PythonOperator(
        task_id="create_missing_datatables_views",
        python_callable=create_missing_datatables_views,
    )

    message_slack = GenericSlackOperator(
        task_id="message_slack",
        message_header="Missing Datatables View Report",
        message_content_task_id="create_missing_datatables_views",
        message_content_task_key="output",
        message_body=""
    )

    get_packages >> find_missing_datatables_views >> create_missing_datatables_views >> message_slack
