import logging
import os
from datetime import datetime
from pathlib import Path

import ckanapi
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from sustainment.update_data_quality_scores import explanation_codes_logic, dqs_utils
from utils import airflow_utils, ckan_utils, misc_utils
from utils_operators.slack_operators import task_failure_slack_alert

from ckan_operators.resource_operator import GetOrCreateResourceOperator

job_settings = {
    "description": "Calculates DQ scores across the catalogue",
    "schedule": "20 19 * * *",
    "start_date": datetime(2023, 3, 27, 0, 0, 0),
}

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = "generate_data_quality_codes"

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN_ADDRESS = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]
ckan = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])


METADATA_FIELDS = [
    "notes",
    "topics",
    "owner_email",
    "information_url",
]

TIME_MAP = {
    "daily": 1,
    "weekly": 7,
    "bi-weekly": 14,
    "monthly": 30,
    "quarterly": 52 * 7 / 4,
    "semi-annually": 52 * 7 / 2,
    "annually": 365,
}

PENALTY_MAP = {
    "daily": 7,  # 7 days behind causes score of 0
    "weekly": 4,  # 4 weeks behind causes score of 0
    "bi-weekly": 4,  # 4 bi-weeks behind causes score of 0
    "monthly": 2,  # 2 months behind causes score of 0
    "quarterly": 2,  # 2 quarters behind causes score of 0
    "semi-annually": 2,  # 2 periods (1 year) causes score of 0
    "annually": 0.5,  # half a year (0.5 periods) causes a score of 0
}

RESOURCE_EXPLANATION_CODES = "quality-scores-explanation-codes-and-scores"
PACKAGE_DQS = "catalogue-quality-scores"

DIMENSIONS = [
    "metadata",
    "freshness",
    "accessibility",
    "completeness",
    "usability",
]  # Ranked in order

WEIGHTS_DATASTORE = [0.35, 0.35, 0.15, 0.1, 0.05]
WEIGHTS_FILESTORE = [0.41, 0.41, 0.18]

BINS = {
    "Bronze": 0.6,
    "Silver": 0.8,
    "Gold": 1,
}

DESCRIPTION = {
    "package": "Unique, normalized, name of dataset.",
    "resource": "Unique data file of dataset page.",
    "usability": (
        "Score for how easy it is to work with the data. "
        "Determined by portion of meaningful column names, "
        "column(s) contain constant values."
    ),
    "usability_code": "Explanation of known issues for usability dimension.",
    "metadata": (
        "Score for how complete metadata fields are (Description, "
        "Limitations, Topics, Contact Email) are, Higher scores "
        "mean more metadata fields filled."
    ),
    "metadata_code": "Explanation of known issues for metadata dimension.",
    "freshness": (
        "Score for how up to date the data is. "
        "The smaller the duration gaps between stated refresh rate "
        "and time last refresh, the higher the score."
    ),
    "freshness_code": "Explanation of known issues for freshness dimension.",
    "completeness": (
        "Score for how much of the data is missing. Determined by "
        "if more than half of the values in this dataset is missing."
    ),
    "completeness_code": "Explanation of known issues for completeness dimension.",
    "accessibility": (
        "Score for the degree to which data is easy to access. "
        "Determined by if the data can be accessed directly via API, "
        "any tags/keywords on the dataset, any automated data pipeline "
        "set up with open data."
    ),
    "accessibility_code": "Explanation of known issues for accessibility dimension.",
    "store_type": "Data file resource is stored as ckan datastore or filestore.",
    "division": "Data publisher division",
    "score": (
        "Summary of accessibility, completeness, freshness, metadata, and usability "
        "scores into a single score. Weights determined via rank weighting."
    ),
    "grade": "Bronze, Silver, Gold badge using the score column.",
    "recorded_at": "Timestamp the dataset was scored.",
}


def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="insert_scores")
    airflow_utils.message_slack(
        name=JOB_NAME,
        **msg,
        active_env=ACTIVE_ENV,
        prod_webhook=ACTIVE_ENV == "prod",
    )


def insert_scores(**kwargs):
    ti = kwargs.pop("ti")
    explanation_code_path = Path(ti.xcom_pull(task_ids="prepare_and_normalize_scores"))
    datastore_resource = ti.xcom_pull(
        task_ids="get_or_create_explanation_code_resource"
    )

    description = kwargs.pop("DESCRIPTION")

    df = pd.read_parquet(explanation_code_path)

    records = df.to_dict(orient="records")

    # collecting datastore fields
    fields = []
    for x in df.columns.values:
        if x in ["usability", "metadata", "freshness", "completeness", "accessibility"]:
            datatype = "float8"
        elif x == "recorded_at":
            datatype = "timestamp"
        else:
            datatype = "text"
        fields.append({"id": x, "type": datatype, "info": {"notes": description[x]}})
    logging.info(fields)

    # insert into datastore
    try:
        logging.info(f"Inserting to datastore_resource: {RESOURCE_EXPLANATION_CODES}")
        ckan.action.datastore_create(
            resource_id=datastore_resource["id"],
            records=df.to_dict(orient="records"),
            fields=fields,
            force=True,
        )

    except Exception as e:
        # Create datastore resource if no existing one.
        logging.error(e)
        logging.info(
            "Datastore doesn't exist, creating data store resource {} ".format(
                RESOURCE_EXPLANATION_CODES
            )
        )

        ckan.action.datastore_create(
            id=datastore_resource["id"], fields=fields, records=records, force=True
        )

        logging.info(f"Inserting to datastore_resource: {RESOURCE_EXPLANATION_CODES}")
    return {
        "message_type": "success",
        "msg": (
            ":done_green: Data quality explanation codes generated "
            + f"for {len(df['package'].unique().tolist())} packages, "
            + f"{df.shape[0]} resources"
        ),
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
        "etl_mapping": [
            {
                "source": "https://open.toronto.ca/dataset/catalogue-quality-scores/",
                "target_package_name": PACKAGE_DQS,
                "target_resource_name": RESOURCE_EXPLANATION_CODES,
            }
        ],
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
        op_args=[ckan],
    )

    check_force_run = PythonOperator(
        task_id="check_force_run",
        python_callable=dqs_utils.force_run_all,
    )

    run_schedule = BranchPythonOperator(
        task_id="run_schedule",
        python_callable=dqs_utils.get_run_schedule,
    )

    run_all_dataset = DummyOperator(task_id="run_all_dataset")
    run_daily_dataset = DummyOperator(task_id="run_daily_dataset")

    dataset_run_list = PythonOperator(
        task_id="dataset_run_list",
        trigger_rule="none_failed",
        python_callable=explanation_codes_logic.get_run_list,
    )

    raw_scores_explanation_codes = PythonOperator(
        task_id="explanation_code_catalogue",
        python_callable=explanation_codes_logic.explanation_code_catalogue,
        op_kwargs={
            "ckan": ckan,
            "METADATA_FIELDS": METADATA_FIELDS,
            "TIME_MAP": TIME_MAP,
            "PENALTY_MAP": PENALTY_MAP,
        },
        trigger_rule="none_failed",
        provide_context=True,
    )

    prepare_and_normalize_scores = PythonOperator(
        task_id="prepare_and_normalize_scores",
        python_callable=explanation_codes_logic.prepare_and_normalize_scores,
        op_kwargs={
            "DIMENSIONS": DIMENSIONS,
            "BINS": BINS,
            "WEIGHTS_DATASTORE": WEIGHTS_DATASTORE,
            "WEIGHTS_FILESTORE": WEIGHTS_FILESTORE,
        },
        provide_context=True,
    )

    delete_raw_scores_explanation_code_tmp_file = PythonOperator(
        task_id="delete_raw_scores_explanation_code_tmp_file",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["explanation_code_catalogue"]},
        provide_context=True,
    )

    delete_final_scores_explanation_code_tmp_file = PythonOperator(
        task_id="delete_final_scores_explanation_code_tmp_file",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["prepare_and_normalize_scores"]},
        provide_context=True,
    )

    delete_data_tmp_file = PythonOperator(
        task_id="delete_data_tmp_file",
        python_callable=misc_utils.delete_file,
        op_kwargs={
            "dag_tmp_dir": "/data/tmp/generate_data_quality_codes",
            "file_name": "data.csv",
        },
        provide_context=True,
    )

    get_or_create_explanation_code_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_explanation_code_resource",
        package_name_or_id=PACKAGE_DQS,
        resource_name=RESOURCE_EXPLANATION_CODES,
        resource_attributes=dict(
            format="csv",
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {PACKAGE_DQS}",
            package_id=PACKAGE_DQS,
            url="placeholder",
        ),
    )

    add_scores = PythonOperator(
        task_id="insert_scores",
        python_callable=insert_scores,
        provide_context=True,
        op_kwargs={"DESCRIPTION": DESCRIPTION},
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        provide_context=True,
        python_callable=send_success_msg,
    )

    [packages, create_tmp_dir, check_force_run] >> run_schedule

    run_schedule >> [run_daily_dataset, run_all_dataset] >> dataset_run_list

    dataset_run_list >> raw_scores_explanation_codes

    (
        raw_scores_explanation_codes
        >> prepare_and_normalize_scores
        >> get_or_create_explanation_code_resource
        >> add_scores
    )

    (
        add_scores
        >> [
            delete_raw_scores_explanation_code_tmp_file,
            delete_final_scores_explanation_code_tmp_file,
            delete_data_tmp_file
        ]
    )

    add_scores >> send_notification
