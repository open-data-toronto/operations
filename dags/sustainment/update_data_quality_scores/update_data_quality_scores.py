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
from sustainment.update_data_quality_scores import dqs_logic  # noqa: E402
from utils import airflow_utils, ckan_utils

job_settings = {
    "description": "Calculates DQ scores across the catalogue",
    "schedule": "0 18 * * 7",
    "start_date": datetime(2020, 11, 10, 5, 0, 0),
}

JOB_FILE = Path(os.path.abspath(__file__))
JOB_NAME = JOB_FILE.name[:-3]

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])


METADATA_FIELDS = [
    "notes",
    "topics",
    "owner_email",
    "information_url",
]

TIME_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
    "quarterly": 52 * 7 / 4,
    "semi-annually": 52 * 7 / 2,
    "annually": 365,
}

PENALTY_MAP = {
    "daily": 7,  # 7 days behind causes score of 0
    "weekly": 4,  # 4 weeks behind causes score of 0
    "monthly": 2,  # 2 months behind causes score of 0
    "quarterly": 2,  # 2 quarters behind causes score of 0
    "semi-annually": 2,  # 2 periods (1 year) causes score of 0
    "annually": 0.5,  # half a year (0.5 periods) causes a score of 0
}

# Mapping between penalty threshold and equation parameters
# The values are derived based on solving a system of sigmoid function.
THRESHOLD_MAP = {7: (2.0, 3.5), 4: (3.5, 2), 2: (6.9, 1), 0.5: (27.6, 0.25)}

RESOURCE_MODEL = "scoring-models"
MODEL_VERSION = "v0.1.0"

RESOURCE_SCORES = "catalogue-scorecard"
PACKAGE_DQS = "catalogue-quality-scores"

DIMENSIONS = [
    "usability",
    "metadata",
    "freshness",
    "completeness",
    "accessibility",
]  # Ranked in order

BINS = {
    "Bronze": 0.6,
    "Silver": 0.8,
    "Gold": 1,
}


def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="insert_scores")
    airflow_utils.message_slack(
        name=JOB_NAME,
        **msg,
        active_env=ACTIVE_ENV,
        prod_webhook=ACTIVE_ENV == "prod",
    )


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=JOB_NAME,
        message_type="error",
        msg="Job not finished",
        active_env=ACTIVE_ENV,
        prod_webhook=ACTIVE_ENV == "prod",
    )


def get_dqs_dataset_resources():
    try:
        framework = CKAN.action.package_show(id=PACKAGE_DQS)
        logging.info(f"Found DQS Package: {PACKAGE_DQS}")
    except ckanapi.NotAuthorized:
        raise Exception("Not authorized to search for packages")
    except ckanapi.NotFound:
        raise Exception(f"DQS package not found: {PACKAGE_DQS}")

    return {r["name"]: r for r in framework.pop("resources")}


def create_resource_for_model(**kwargs):
    ti = kwargs.pop("ti")
    resources = ti.xcom_pull(task_ids="get_dqs_dataset_resources")

    if RESOURCE_MODEL not in resources and CKAN.apikey:
        logging.info(f"Creating resource for model: {RESOURCE_MODEL}")
        r = requests.post(
            f"{CKAN.address}/api/3/action/resource_create",
            data={
                "package_id": PACKAGE_DQS,
                "name": RESOURCE_MODEL,
                "format": "json",
                "is_preview": False,
                "is_zipped": False,
            },
            headers={"Authorization": CKAN.apikey},
            files={"upload": (f"{RESOURCE_MODEL}.json", json.dumps({}))},
        )

        resources[RESOURCE_MODEL] = r.json()["result"]

    headers = {"Authorization": CKAN.apikey}

    models = requests.get(resources[RESOURCE_MODEL]["url"], headers=headers)

    return {"models": models.json(), "resource": resources[RESOURCE_MODEL]}


def add_model_to_resource(**kwargs):
    ti = kwargs.pop("ti")
    model_resource = ti.xcom_pull(task_ids="create_resource_for_model")
    weights = ti.xcom_pull(task_ids="calculate_model_weights")
    for i, (dim, wgt) in enumerate(zip(DIMENSIONS, weights)):
        logging.info(f"num {i}, dim {dim}, wgt {wgt}")

    model_resource["models"][MODEL_VERSION] = {
        "aggregation_methods": {
            "metrics_to_dimension": "avg",
            "dimensions_to_score": "sum_and_reciprocal",
        },
        "dimensions": [
            {"name": dim, "rank": i + 1, "weights": wgt}
            for i, (dim, wgt) in enumerate(zip(DIMENSIONS, weights))
        ],
        "bins": BINS,
    }

    return model_resource


def upload_models_to_resource(**kwargs):
    ti = kwargs.pop("ti")
    model_resource = ti.xcom_pull(task_ids="add_model_to_resource")

    res = requests.post(
        f"{CKAN.address}/api/3/action/resource_patch",
        data={"id": model_resource["resource"]["id"]},
        headers={"Authorization": CKAN.apikey},
        files={
            "upload": (
                f"{RESOURCE_MODEL}.json",
                json.dumps(model_resource["models"]),
            )
        },
    )

    return res.json()


def create_scores_resource(**kwargs):
    ti = kwargs.pop("ti")
    resources = ti.xcom_pull(task_ids="get_dqs_dataset_resources")
    df = ti.xcom_pull(task_ids="score_catalogue")

    if RESOURCE_SCORES not in resources:
        logging.info(f"Creating scores resource: {RESOURCE_SCORES}")
        resources[RESOURCE_SCORES] = CKAN.action.datastore_create(
            resource={
                "package_id": PACKAGE_DQS,
                "name": RESOURCE_SCORES,
                "format": "csv",
                "is_preview": True,
                "is_zipped": True,
            },
            fields=[{"id": x} for x in df.columns.values],
            records=[],
        )
    else:
        logging.info(f" {PACKAGE_DQS}: {RESOURCE_SCORES} already exists")

    return resources[RESOURCE_SCORES]


def insert_scores(**kwargs):
    ti = kwargs.pop("ti")
    final_scores_path = Path(ti.xcom_pull(task_ids="prepare_and_normalize_scores"))
    datastore_resource = ti.xcom_pull(task_ids="create_scores_resource")

    df = pd.read_parquet(final_scores_path)
    logging.info(f"Inserting to datastore_resource: {RESOURCE_SCORES}")
    records = df.to_dict(orient="records")
    logging.info(records[:5])
    CKAN.action.datastore_upsert(
        method="insert",
        resource_id=datastore_resource["id"],
        records=df.to_dict(orient="records"),
    )

    return {
        "message_type": "success",
        "msg": f"Data quality scores calculated for {df.shape[0]} datasets",
    }


default_args = airflow_utils.get_default_args(
    {
        "on_failure_callback": send_failure_msg,
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

    model_weights = PythonOperator(
        task_id="calculate_model_weights",
        python_callable=dqs_logic.calculate_model_weights,
        op_kwargs={"dimensions": DIMENSIONS},
    )

    packages = PythonOperator(
        task_id="get_all_packages",
        python_callable=ckan_utils.get_all_packages,
        op_args=[CKAN],
    )

    dqs_package_resources = PythonOperator(
        task_id="get_dqs_dataset_resources",
        python_callable=get_dqs_dataset_resources,
    )

    framework_resource = PythonOperator(
        task_id="create_resource_for_model",
        python_callable=create_resource_for_model,
        provide_context=True,
    )

    add_run_model = PythonOperator(
        task_id="add_model_to_resource",
        python_callable=add_model_to_resource,
        provide_context=True,
    )

    upload_models = PythonOperator(
        task_id="upload_models_to_resource",
        python_callable=upload_models_to_resource,
        provide_context=True,
    )

    raw_scores = PythonOperator(
        task_id="score_catalogue",
        python_callable=dqs_logic.score_catalogue,
        op_kwargs={
            "ckan": CKAN,
            "METADATA_FIELDS": METADATA_FIELDS,
            "TIME_MAP": TIME_MAP,
            "PENALTY_MAP": PENALTY_MAP,
            "THRESHOLD_MAP": THRESHOLD_MAP,
        },
        provide_context=True,
    )

    final_scores = PythonOperator(
        task_id="prepare_and_normalize_scores",
        python_callable=dqs_logic.prepare_and_normalize_scores,
        op_kwargs={
            "ckan": CKAN,
            "DIMENSIONS": DIMENSIONS,
            "BINS": BINS,
            "MODEL_VERSION": MODEL_VERSION,
        },
        provide_context=True,
    )

    delete_raw_scores_tmp_file = PythonOperator(
        task_id="delete_raw_scores_tmp_file",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["score_catalogue"]},
        provide_context=True,
    )

    scores_resource = PythonOperator(
        task_id="create_scores_resource",
        python_callable=create_scores_resource,
        provide_context=True,
    )

    add_scores = PythonOperator(
        task_id="insert_scores",
        python_callable=insert_scores,
        provide_context=True,
    )

    delete_final_scores_tmp_file = PythonOperator(
        task_id="delete_final_scores_tmp_file",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["prepare_and_normalize_scores"]},
        provide_context=True,
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

    dqs_package_resources >> framework_resource
    [framework_resource, model_weights] >> add_run_model
    [packages, create_tmp_dir] >> raw_scores
    [raw_scores, model_weights] >> final_scores
    final_scores >> scores_resource
    final_scores >> delete_raw_scores_tmp_file
    add_run_model >> upload_models
    scores_resource >> add_scores
    add_scores >> delete_final_scores_tmp_file
    [upload_models, add_scores] >> send_notification
    [delete_final_scores_tmp_file, delete_raw_scores_tmp_file] >> delete_tmp_dir
