import json
import logging
from datetime import datetime
from pathlib import Path

import ckanapi
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from ckan_operators.package_operator import GetAllPackagesOperator
from utils import airflow_utils
from utils_operators.directory_operator import CreateLocalDirectoryOperator

job_name = "get_empty_datastore_resources"
empties_file_name = "empties.json"

with DAG(
    job_name,
    default_args=airflow_utils.get_default_args(
        {
            "on_failure_callback": airflow_utils.message_slack(
                name=job_name,
                message_type="error",
                msg="Job not finished",
                active_env=Variable.get("active_env"),
                prod_webhook=Variable.get("active_env") == "prod",
            ),
            "start_date": datetime(2020, 11, 9, 0, 30, 0),
        }
    ),
    description="Identifies empty datastore resources and send to Slack",
    schedule_interval="5 15,18,21,0,3 * * *",
    tags=["sustainment"],
    catchup=False,
) as dag:

    ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
    active_env = Variable.get("active_env")
    ckan_address = ckan_creds[active_env]["address"]
    ckan_apikey = ckan_creds[active_env]["apikey"]

    def send_success_msg(**kwargs):
        msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")

        airflow_utils.message_slack(
            name=dag.dag_id,
            **msg,
            active_env=active_env,
            prod_webhook=active_env == "prod",
        )

    def get_record_counts(**kwargs):
        ckan = ckanapi.RemoteCKAN(address=kwargs["address"], apikey=kwargs["apikey"])

        packages = kwargs.pop("ti").xcom_pull(task_ids="get_all_packages")
        datastore_resources = []

        for p in packages:
            for r in p["resources"]:
                if r["url_type"] != "datastore":
                    continue
                res = ckan.action.datastore_search(id=r["id"], limit=0)

                datastore_resources.append(
                    {
                        "package_id": p["title"],
                        "resource_id": r["id"],
                        "resource_name": r["name"],
                        "extract_job": r["extract_job"],
                        "row_count": res["total"],
                        "fields": res["fields"],
                    }
                )

                logging.info(f'{p["name"]}: {r["name"]} - {res["total"]} records')

        logging.info(f"Identified {len(datastore_resources)} datastore resources")

        return datastore_resources

    def filter_empty_resources(**kwargs):
        datastore_resources = kwargs.pop("ti").xcom_pull(task_ids="get_record_counts")

        empties = [r for r in datastore_resources if r["row_count"] == 0]

        logging.info(f"Identified {len(empties)} empty datastore resources")

        return empties

    def build_message(**kwargs):
        def pprint_2d_list(matrix):
            s = [[str(e) for e in row] for row in matrix]
            lens = [max(map(len, col)) for col in zip(*s)]
            fmt = "\t".join("{{:{}}}".format(x) for x in lens)
            table = [fmt.format(*row) for row in s]
            return "\n".join(table)

        empties = kwargs.pop("ti").xcom_pull(task_ids="filter_empty_resources")

        if not len(empties):
            logging.info("No empty resources found")
            return {"message_type": "success", "msg": "No empties"}

        empties = sorted(empties, key=lambda i: i["package_id"])

        matrix = [["#", "PACKAGE", "EXTRACT_JOB", "ROW_COUNT"]]

        for i, r in enumerate(empties):
            string = [f"{i+1}."]
            string.extend(
                [r[f] for f in ["package_id", "extract_job", "row_count"] if r[f]]
            )
            matrix.append(string)

        empties = pprint_2d_list(matrix)

        logging.warning(f"Empty resources found:\n```\n{empties}\n```")

        return {
            "message_type": "warning",
            "msg": f"""```{empties}```""",
        }

    def are_there_empties(**kwargs):
        empties = kwargs.pop("ti").xcom_pull(task_ids="filter_empty_resources")

        if len(empties) == 0:
            return "there_are_no_empties"

        return "there_are_empties"

    def were_there_empties_prior(**kwargs):
        ti = kwargs.pop("ti")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))

        fpath = tmp_dir / empties_file_name

        if fpath.exists():
            return "there_were_empties_prior"

        return "there_were_no_empties_prior"

    def save_empties_file(**kwargs):
        ti = kwargs.pop("ti")
        empties = ti.xcom_pull(task_ids="filter_empty_resources")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))

        fpath = tmp_dir / empties_file_name

        with open(fpath, "w") as f:
            json.dump(empties, f)

        return fpath

    create_tmp_dir = CreateLocalDirectoryOperator(
        task_id="create_tmp_dir", path=Path(Variable.get("tmp_dir")) / dag.dag_id,
    )

    packages = GetAllPackagesOperator(
        task_id="get_all_packages", address=ckan_address, apikey=ckan_apikey,
    )

    record_counts = PythonOperator(
        task_id="get_record_counts",
        provide_context=True,
        python_callable=get_record_counts,
        op_kwargs={"address": ckan_address, "apikey": ckan_apikey},
    )

    empties = PythonOperator(
        task_id="filter_empty_resources",
        provide_context=True,
        python_callable=filter_empty_resources,
    )

    empties_branch = BranchPythonOperator(
        task_id="are_there_empties", python_callable=are_there_empties,
    )

    no_notification = DummyOperator(task_id="no_notification",)

    message = PythonOperator(python_callable=build_message, task_id="build_message",)

    prior_branch = BranchPythonOperator(
        task_id="were_there_empties_prior", python_callable=were_there_empties_prior,
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=send_success_msg,
        trigger_rule="one_success",
    )

    save_file = PythonOperator(
        task_id="save_empties_file", python_callable=save_empties_file,
    )

    there_are_empties = DummyOperator(task_id="there_are_empties",)

    there_are_no_empties = DummyOperator(task_id="there_are_no_empties",)

    there_were_empties_prior = DummyOperator(task_id="there_were_empties_prior",)

    there_were_no_empties_prior = DummyOperator(task_id="there_were_no_empties_prior",)

    delete_tmp_dir = PythonOperator(
        task_id="delete_tmp_dir",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": dag.dag_id, "recursively": True},
        trigger_rule="none_failed",
    )

    create_tmp_dir >> empties_branch

    packages >> record_counts >> empties >> message >> empties_branch

    empties_branch >> there_are_empties >> save_file >> send_notification

    empties_branch >> there_are_no_empties >> prior_branch

    prior_branch >> there_were_no_empties_prior >> no_notification >> delete_tmp_dir

    prior_branch >> there_were_empties_prior >> send_notification

    there_were_empties_prior >> delete_tmp_dir
