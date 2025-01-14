import pytest
import os
import csv
import filecmp
from pathlib import Path
from datetime import datetime

from airflow.models import DagBag, DagRun, Variable
from sustainment.portal_usage.portal_usage_utils import (
    determine_latest_period_loaded,
    calculate_periods_to_load,
    generate_usage_report,
    extract_new_report,
)


@pytest.fixture()
def report_params():
    account_id = Variable.get("oracle_infinity_account_id")
    user = Variable.get("oracle_infinity_user")
    password = Variable.get("oracle_infinity_password")
    report_name = "Page Views and Time Based Metrics"
    report_id = "acd19558f6734dfc187e1add2680e287"

    return {
        "account_id": account_id,
        "user": user,
        "password": password,
        "report_name": report_name,
        "report_id": report_id,
    }


def test_determine_latest_period_loaded():
    dir_path = Path("/data/operations/dags/sustainment/portal_usage/tests/test_data")
    latest_loaded = determine_latest_period_loaded(dir_path)

    assert latest_loaded == datetime.strptime(
        "2024-11-01 00:00:00", "%Y-%m-%d %H:%M:%S"
    )


def test_calculate_periods_to_load():
    latest_loaded_date = datetime(2024, 5, 2)
    periods_to_load = calculate_periods_to_load(latest_loaded_date)

    assert periods_to_load[0] == {"begin": "2024/06/1/0", "end": "2024/06/30/23"}


def test_generate_usage_report(report_params):
    latest_loaded_date = datetime(2024, 10, 17)

    # select the most recent period based on the running date
    # make sure that period's data is always available on oracle infinity
    last_periods_to_load = calculate_periods_to_load(latest_loaded_date)[-1]

    response = generate_usage_report(
        report_name=report_params["report_name"],
        report_id=report_params["report_id"],
        begin=last_periods_to_load["begin"],
        end=last_periods_to_load["end"],
        account_id=report_params["account_id"],
        user=report_params["user"],
        password=report_params["password"],
    )

    assert response.status_code == 200


def test_extract_new_report(report_params):
    import shutil

    # test only the most recent
    current_folder = "/data/operations/dags/sustainment/portal_usage/tests/test_data/"
    latest_loaded_date = datetime(2024, 10, 17)
    last_periods_to_load = [calculate_periods_to_load(latest_loaded_date)[-1]]

    extract_new_report(
        report_name=report_params["report_name"],
        periods_to_load=last_periods_to_load,
        dest_path=current_folder,
    )

    ym = datetime.strptime(last_periods_to_load[0]["end"], "%Y/%m/%d/%H").strftime(
        "%Y%m"
    )
    fpath = current_folder + ym + "/" + report_params["report_name"] + "_" + ym + ".csv"

    assert os.path.exists(fpath) == True

    # remove the generated files after testing to main clean directory
    folder_path = current_folder + ym
    shutil.rmtree(folder_path, ignore_errors=True)


@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="/data/operations/dags/sustainment/portal_usage")


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="open_data_portal_usage")
    assert dagbag.import_errors == {}, "Import error occurs, broken DAG"
    assert dag is not None
    assert len(dag.tasks) == 17


def test_task_flow_valid(dagbag):
    dag = dagbag.get_dag(dag_id="open_data_portal_usage")

    expected_task_flow = {
        "create_tmp_dir": ["determine_latest_period_loaded"],
        "determine_latest_period_loaded": ["calculate_periods_to_load"],
        "calculate_periods_to_load": ["does_report_need_update", "build_message"],
        "does_report_need_update": ["get_or_create_package", "no_new_reports_to_load"],
        "get_or_create_package": [
            "extract_report_PageViewsandTimeBasedMetrics",
            "extract_report_FileURLClicks",
        ],
        "extract_report_PageViewsandTimeBasedMetrics": [
            "get_or_create_resource_PageViewsandTimeBasedMetrics"
        ],
        "get_or_create_resource_PageViewsandTimeBasedMetrics": [
            "insert_records_to_datastore_PageViewsandTimeBasedMetrics"
        ],
        "insert_records_to_datastore_PageViewsandTimeBasedMetrics": [
            "datastore_cache_PageViewsandTimeBasedMetrics"
        ],
        "datastore_cache_PageViewsandTimeBasedMetrics": ["all_reports_inserted"],
        "all_reports_inserted": ["send_to_slack"],
    }
    for task_id, downstream_list in expected_task_flow.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list), "Invalid task flow!!"


def test_run_dag(dagbag):
    dag_id = "open_data_portal_usage"
    dag = dagbag.get_dag(dag_id=dag_id)
    dag.test()

    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

    # wait for most recent dagrun to finish
    while dag_runs[0].state in ["running", "queued"]:
        time.sleep(10)
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

    dag_run = dag_runs[0]
    failed_tasks = []
    for ti in dag_run.get_task_instances():
        if ti.state not in ["success", "skipped"]:
            failed_tasks.append(ti.task_id)

    assert len(failed_tasks) == 0, "Failed tasks detected!"
