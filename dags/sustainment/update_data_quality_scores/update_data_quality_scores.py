from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow import DAG
from pathlib import Path
import requests
import logging
import ckanapi
import json
import sys
import os

sys.path.append(Variable.get("repo_dir"))
import dags.utils as airflow_utils  # noqa: E402
import jobs.utils.common as common_utils  # noqa: E402
import dags.sustainment.update_data_quality_scores.dqs_logic as dqs_logic  # noqa: E402


job_settings = {
    "description": "Calculates DQ scores across the catalogue",
    "schedule": "0 16 * * 2,4",
    "start_date": days_ago(1),
}

job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])


METADATA_FIELDS = ["collection_method", "limitations", "topics", "owner_email"]

TIME_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
    "quarterly": 52 * 7 / 4,
    "semi-annually": 52 * 7 / 2,
    "annually": 365,
}

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
    msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
    airflow_utils.message_slack(name=job_name, **msg)


def send_failure_msg(self):
    airflow_utils.message_slack(
        name=job_name,
        message_type="error",
        msg="Job not finished",
    )


def get_dqs_package_resources():
    try:
        framework = CKAN.action.package_show(id=PACKAGE_DQS)
        logging.info(f"Found DQS Package: {PACKAGE_DQS}")
    except ckanapi.NotAuthorized:
        raise Exception("Not authorized to search for packages")
    except ckanapi.NotFound:
        raise Exception(f"DQS package not found: {PACKAGE_DQS}")

    return {r["name"]: r for r in framework.pop("resources")}


def create_model_resource(**kwargs):
    ti = kwargs.pop("ti")
    resources = ti.xcom_pull(task_ids="get_dqs_package_resources")

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

    return models.json()


def add_model(**kwargs):
    ti = kwargs.pop("ti")
    models = ti.xcom_pull(task_ids="create_model_resource")
    weights = ti.xcom_pull(task_ids="calculate_weights")

    models[MODEL_VERSION] = {
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

    return models


def upload_model(**kwargs):
    ti = kwargs.pop("ti")
    models = ti.xcom_pull(task_ids="add_model")

    requests.post(
        f"{CKAN.address}/api/3/action/resource_patch",
        data={"id": models[RESOURCE_MODEL]["id"]},
        headers={"Authorization": CKAN.apikey},
        files={
            "upload": (
                f"{RESOURCE_MODEL}.json",
                json.dumps(models),
            )
        },
    )


def create_scores_resource(**kwargs):
    resources = kwargs.pop("ti").xcom_pull(task_ids="get_dqs_resources")
    df = kwargs.pop("ti").xcom_pull(task_ids="score_catalogue")

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
    df = ti.xcom_pull(task_ids="score_catalogue")
    datastore_resource = ti.xcom_pull(task_ids="create_scores_resource")

    logging.info(f"Inserting to datastore_resource: {RESOURCE_SCORES}")
    CKAN.action.datastore_upsert(
        method="insert",
        resource_id=datastore_resource["resource_id"],
        records=df.to_dict(orient="records"),
    )

    return {
        "message_type": "success",
        "msg": f"Data quality scores calculated for {df.shape[0]} packages",
    }


default_args = airflow_utils.get_default_args(
    {
        "on_failure_callback": send_failure_msg,
        "start_date": job_settings["start_date"],
    }
)


with DAG(
    job_name,
    default_args=default_args,
    description=job_settings["description"],
    schedule_interval=job_settings["schedule"],
    tags=["sustainment"],
) as dag:

    model_weights = PythonOperator(
        task_id="calculate_weights",
        python_callable=dqs_logic.calculate_weights,
        op_kwargs={"dimensions": DIMENSIONS},
    )

    packages = PythonOperator(
        task_id="get_packages",
        python_callable=common_utils.get_all_packages,
        op_args=[CKAN],
    )

    dqs_package_resources = PythonOperator(
        task_id="get_dqs_package_resources",
        python_callable=get_dqs_package_resources,
    )

    framework_resource = PythonOperator(
        task_id="create_model_resource",
        python_callable=create_model_resource,
        provide_context=True,
    )

    add_run_model = PythonOperator(
        task_id="add_model",
        python_callable=add_model,
        provide_context=True,
    )

    upload_models = PythonOperator(
        task_id="upload_model",
        python_callable=upload_model,
        provide_context=True,
    )

    raw_scores = PythonOperator(
        task_id="score_catalogue",
        python_callable=dqs_logic.score_catalogue,
        op_kwargs={
            "ckan": CKAN,
            "METADATA_FIELDS": METADATA_FIELDS,
            "TIME_MAP": TIME_MAP,
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

    send_notification = PythonOperator(
        task_id="send_notification",
        provide_context=True,
        python_callable=send_success_msg,
    )

    job_completed = DummyOperator(task_id="job_completed")

    get_dqs_package_resources >> framework_resource
    [framework_resource, model_weights] >> add_run_model
    packages >> raw_scores
    [raw_scores, model_weights] >> [final_scores, scores_resource]
    [add_run_model, final_scores] >> upload_models
    [final_scores, upload_models, scores_resource] >> add_scores
    [add_scores, upload_models] >> send_notification
    send_notification >> job_completed
