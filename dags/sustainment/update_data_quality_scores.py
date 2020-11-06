from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow import DAG
import ckanapi
import sys

sys.path.append(Variable.get("repo_dir"))
import dags.utils as airflow_utils  # noqa: E402
import jobs.utils.common as common_utils  # noqa: E402

job_settings = {
    "description": "Gets datasets behind published refresh rate and by how long",
    "schedule": "0 16 * * 2,4",
    "start_date": days_ago(1),
}

job_file = Path(os.path.abspath(__file__))
job_name = job_file.name[:-3]

active_env = Variable.get("active_env")
ckan_creds = Variable.get("ckan_credentials", deserialize_json=True)
ckan = ckanapi.RemoteCKAN(**ckan_creds[active_env])



def send_success_msg(**kwargs):
    msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
    airflow_utils.message_slack(name=job_name, ckan_url=ckan.address, **msg)


def send_failure_msg(**kwargs):
    airflow_utils.message_slack(
        name=job_name,
        ckan_url=ckan.address,
        message_type="error",
        msg="",
    )


def create_dag(name, schedule, description, dag_number, default_args):
    def send_success_msg(**kwargs):
        airflow_utils.message_slack(
            name=name,
            ckan_url=ckan.address,
            **kwargs["ti"].xcom_pull(task_ids="run_job"),
        )

    def send_failure_msg():
        airflow_utils.message_slack(
            name=name,
            ckan_url=ckan.address,
            message_type="error",
            msg="",
        )

    with DAG(
        name,
        default_args={**default_args, "on_failure_callback": send_failure_msg},
        description=description,
        schedule_interval=schedule,
    ) as dag:
        job = (
            getattr(datasets, name)
            if hasattr(datasets, name)
            else getattr(sustainment, name)
        )

        run_job = PythonOperator(
            task_id="run_job",
            python_callable=job.run,
            dag_number=dag_number,
            op_kwargs={"ckan": ckan},
        )

        send_notification = PythonOperator(
            task_id="send_notification",
            provide_context=True,
            python_callable=send_success_msg,
            dag_number=dag_number,
        )

        run_job >> send_notification

    return dag


for number, job in enumerate(jobs):
    name, settings = job

    globals()[name] = create_dag(
        name=name,
        dag_number=number,
        description=settings["description"],
        schedule=settings["schedule"],
        default_args=airflow_utils.get_default_args(
            {"start_date": settings["start_date"]}
        ),
    )
