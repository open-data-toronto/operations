import json
import logging
import os
from datetime import datetime
from pathlib import Path

import ckanapi
import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from sustainment.update_data_quality_scores import dqs_logic, explanation_codes_logic
from utils import airflow_utils, ckan_utils
from utils_operators.slack_operators import (
    GenericSlackOperator,
    task_failure_slack_alert,
)
from ckan_operators.resource_operator import GetOrCreateResourceOperator

job_settings = {
    "description": "Calculates DQ scores across the catalogue",
    "schedule": "@once",
    "start_date": datetime(2023, 3, 27, 0, 0, 0),
}

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = "generate_data_quality_codes"

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN_ADDRESS = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])


METADATA_FIELDS = ["notes", "limitations", "topics", "owner_email", "civic_issues", "information_url"]

TIME_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
    "quarterly": 52 * 7 / 4,
    "semi-annually": 52 * 7 / 2,
    "annually": 365,
}


RESOURCE_EXPLANATION_CODES = "quality-scores-explanation-codes"
PACKAGE_DQS = "catalogue-quality-scores"


def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="insert_scores")
    airflow_utils.message_slack(
        name=JOB_NAME, **msg, active_env=ACTIVE_ENV, prod_webhook=ACTIVE_ENV == "prod",
    )

def get_dqs_dataset_resources():
    try:
        framework = CKAN.action.package_show(id=PACKAGE_DQS)
        logging.info(f"Found DQS Package: {PACKAGE_DQS}")
    except ckanapi.NotAuthorized:
        raise Exception("Not authorized to search for packages")
    except ckanapi.NotFound:
        raise Exception(f"DQS package not found: {PACKAGE_DQS}")
    
    for file in os.listdir('/data/tmp/generate_data_quality_codes'):
        logging.info(file)

    return {r["name"]: r for r in framework.pop("resources")}


def insert_scores(**kwargs):
    ti = kwargs.pop("ti")
    explanation_code_path = Path(ti.xcom_pull(task_ids="explanation_code_catalogue"))
    datastore_resource = ti.xcom_pull(task_ids="get_or_create_explanation_code_resource")

    df = pd.read_parquet(explanation_code_path)
    
    records = df.to_dict(orient="records")
    logging.info(records[:5])
    
    # collecting datastore fields
    fields = []
    for x in df.columns.values:
        if x in ['usability', 'metadata', 'freshness', 'completeness', 'accessibility']:
            datatype = "float8"
        elif x == "recorded_at":
            datatype = "timestamp"
        else:
            datatype = "text"
        fields.append({"id": x, "type": datatype, "info": ''})
    
    # insert into datastore
    try:
        logging.info(f"Inserting to datastore_resource: {RESOURCE_EXPLANATION_CODES}")
        CKAN.action.datastore_upsert(
        method="insert",
        resource_id=datastore_resource["id"],
        records=df.to_dict(orient="records"),
    )
        
    except Exception as e:
        # Create datastore resource if no existing one.
        logging.error(e)
        logging.info(
            "Datastore doesn't exist, creating data store resource {} ".format(
                RESOURCE_EXPLANATION_CODES
            )
        )
        
        CKAN.action.datastore_create(
            id=datastore_resource["id"], 
            fields=fields,
            records=records,
            force=True
        )
        
        logging.info(f"Inserting to datastore_resource: {RESOURCE_EXPLANATION_CODES}")
    return {
        "message_type": "success",
        "msg": f":done_green: Data quality codes generated for {df.shape[0]} resources",
    }


default_args = airflow_utils.get_default_args(
    {
        "owner": "Yanan",
        "depends_on_past": False,
        "email": "yanan.zhang@toronto.ca",
        "email_on_failure": False,
        "on_failure_callback": task_failure_slack_alert, 
        "start_date": job_settings["start_date"], 
        "pool": "big_job_pool",
        "retries": 1,
        "retry_delay": 3,
    }
)


with DAG(
    JOB_NAME,
    default_args=default_args,
    description=job_settings["description"],
    schedule_interval=job_settings["schedule"],
    tags=["sustainment"],
    catchup=False,
) as dag:

    create_tmp_dir = PythonOperator(
        task_id="create_tmp_data_dir",
        python_callable=airflow_utils.create_dir_with_dag_name,
        op_kwargs={"dag_id": JOB_NAME, "dir_variable_name": "tmp_dir"},
    )

    packages = PythonOperator(
        task_id="get_all_packages",
        python_callable=ckan_utils.get_all_packages,
        op_args=[CKAN],
    )

    raw_scores_explanation_codes = PythonOperator(
        task_id="explanation_code_catalogue",
        python_callable=explanation_codes_logic.explanation_code_catalogue,
        op_kwargs={
            "ckan": CKAN,
            "METADATA_FIELDS": METADATA_FIELDS,
            "TIME_MAP": TIME_MAP,
        },
        provide_context=True,
    )

    delete_raw_scores_explanation_code_tmp_file = PythonOperator(
        task_id="delete_raw_scores_explanation_code_tmp_file",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["explanation_code_catalogue"]},
        provide_context=True,
    )
    
    get_or_create_explanation_code_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_explanation_code_resource",
        address=CKAN_ADDRESS,
        apikey=CKAN_APIKEY,
        package_name_or_id=PACKAGE_DQS,
        resource_name=RESOURCE_EXPLANATION_CODES,
        resource_attributes=dict(
            format="csv",
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {PACKAGE_DQS}",
            package_id=PACKAGE_DQS,
            url = "placeholder"
        ),
    )

    add_scores = PythonOperator(
        task_id="insert_scores", python_callable=insert_scores, provide_context=True,
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        provide_context=True,
        python_callable=send_success_msg,
    )

    delete_tmp_dir = PythonOperator(
        task_id="delete_tmp_data_dir",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": JOB_NAME},
    )

    
    [packages, create_tmp_dir] >> raw_scores_explanation_codes 
    raw_scores_explanation_codes >> get_or_create_explanation_code_resource >> add_scores
    add_scores >> [delete_raw_scores_explanation_code_tmp_file, send_notification] 
    delete_raw_scores_explanation_code_tmp_file>> delete_tmp_dir
