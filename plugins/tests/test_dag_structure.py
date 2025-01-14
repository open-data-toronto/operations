import pytest
from airflow.models import DagBag


def test_dag_has_all_tasks_single_resource(dagbag):
    dag = dagbag.get_dag(dag_id="daily-shelter-overnight-service-occupancy-capacity")
    expected_tasks_pool = [
        "create_tmp_dir",
        "get_or_create_package",
        "done_inserting_into_datastore",
        "download_data_Dailyshelterovernightoccupancy",
        "get_or_create_resource_Dailyshelterovernightoccupancy",
        "new_or_existing_Dailyshelterovernightoccupancy",
        "brand_new_Dailyshelterovernightoccupancy",
        "does_Dailyshelterovernightoccupancy_need_update",
        "dont_update_resource_Dailyshelterovernightoccupancy",
        "check_Dailyshelterovernightoccupancy_insert_method",
        "delete_resource_Dailyshelterovernightoccupancy",
        "insert_records_Dailyshelterovernightoccupancy",
        "datastore_cache_Dailyshelterovernightoccupancy",
        "delete_failed_resource_Dailyshelterovernightoccupancy",
        "restore_backup_records_Dailyshelterovernightoccupancy",
        "clean_backups_Dailyshelterovernightoccupancy",
        "message_factory",
        "slack_writer",
    ]
    for task_id in expected_tasks_pool:
        assert dag.has_task(task_id) == True, "Invalid task detected."


def test_task_flow_valid_single_resource(dagbag):
    dag = dagbag.get_dag(dag_id="daily-shelter-overnight-service-occupancy-capacity")

    expected_task_flow = {
        "create_tmp_dir": ["get_or_create_package"],
        "get_or_create_package": ["download_data_Dailyshelterovernightoccupancy"],
        "download_data_Dailyshelterovernightoccupancy": [
            "get_or_create_resource_Dailyshelterovernightoccupancy"
        ],
        "get_or_create_resource_Dailyshelterovernightoccupancy": [
            "new_or_existing_Dailyshelterovernightoccupancy"
        ],
        "new_or_existing_Dailyshelterovernightoccupancy": [
            "brand_new_Dailyshelterovernightoccupancy",
            "does_Dailyshelterovernightoccupancy_need_update",
        ],
        "brand_new_Dailyshelterovernightoccupancy": [
            "insert_records_Dailyshelterovernightoccupancy"
        ],
        "does_Dailyshelterovernightoccupancy_need_update": [
            "check_Dailyshelterovernightoccupancy_insert_method",
            "dont_update_resource_Dailyshelterovernightoccupancy",
        ],
        "check_Dailyshelterovernightoccupancy_insert_method": [
            "delete_resource_Dailyshelterovernightoccupancy",
            "insert_records_Dailyshelterovernightoccupancy",
        ],
        "delete_resource_Dailyshelterovernightoccupancy": [
            "insert_records_Dailyshelterovernightoccupancy"
        ],
        "insert_records_Dailyshelterovernightoccupancy": [
            "datastore_cache_Dailyshelterovernightoccupancy",
            "delete_failed_resource_Dailyshelterovernightoccupancy",
        ],
        "delete_failed_resource_Dailyshelterovernightoccupancy": [
            "restore_backup_records_Dailyshelterovernightoccupancy"
        ],
        "restore_backup_records_Dailyshelterovernightoccupancy": [
            "clean_backups_Dailyshelterovernightoccupancy"
        ],
        "datastore_cache_Dailyshelterovernightoccupancy": [
            "clean_backups_Dailyshelterovernightoccupancy"
        ],
        "dont_update_resource_Dailyshelterovernightoccupancy": [
            "clean_backups_Dailyshelterovernightoccupancy"
        ],
        "clean_backups_Dailyshelterovernightoccupancy": [
            "done_inserting_into_datastore"
        ],
        "done_inserting_into_datastore": ["message_factory"],
        "slack_writer": [],
    }
    for task_id, downstream_list in expected_task_flow.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert set(downstream_list) <= set(
            task.downstream_task_ids
        ), "Invalid task flow!!"


def test_dag_has_all_tasks_multiple_resources(dagbag):
    dag = dagbag.get_dag(dag_id="registered-programs-and-drop-in-courses-offering")
    expected_tasks_pool = [
        "create_tmp_dir",
        "get_or_create_package",
        "done_inserting_into_datastore",
        "download_data_RegisteredPrograms",
        "get_or_create_resource_RegisteredPrograms",
        "new_or_existing_RegisteredPrograms",
        "brand_new_RegisteredPrograms",
        "does_RegisteredPrograms_need_update",
        "dont_update_resource_RegisteredPrograms",
        "check_RegisteredPrograms_insert_method",
        "delete_resource_RegisteredPrograms",
        "insert_records_RegisteredPrograms",
        "datastore_cache_RegisteredPrograms",
        "delete_failed_resource_RegisteredPrograms",
        "restore_backup_records_RegisteredPrograms",
        "clean_backups_RegisteredPrograms",
        "download_data_Drop-in",
        "get_or_create_resource_Drop-in",
        "new_or_existing_Drop-in",
        "brand_new_Drop-in",
        "does_Drop-in_need_update",
        "dont_update_resource_Drop-in",
        "check_Drop-in_insert_method",
        "delete_resource_Drop-in",
        "insert_records_Drop-in",
        "datastore_cache_Drop-in",
        "delete_failed_resource_Drop-in",
        "restore_backup_records_Drop-in",
        "clean_backups_Drop-in",
        "download_data_Locations",
        "get_or_create_resource_Locations",
        "new_or_existing_Locations",
        "brand_new_Locations",
        "does_Locations_need_update",
        "dont_update_resource_Locations",
        "check_Locations_insert_method",
        "delete_resource_Locations",
        "insert_records_Locations",
        "datastore_cache_Locations",
        "delete_failed_resource_Locations",
        "restore_backup_records_Locations",
        "clean_backups_Locations",
        "download_data_Facilities",
        "get_or_create_resource_Facilities",
        "new_or_existing_Facilities",
        "brand_new_Facilities",
        "does_Facilities_need_update",
        "dont_update_resource_Facilities",
        "check_Facilities_insert_method",
        "delete_resource_Facilities",
        "insert_records_Facilities",
        "datastore_cache_Facilities",
        "delete_failed_resource_Facilities",
        "restore_backup_records_Facilities",
        "clean_backups_Facilities",
        "message_factory",
        "slack_writer",
    ]
    for task_id in expected_tasks_pool:
        assert dag.has_task(task_id) == True, "Invalid task detected."


def test_task_flow_valid_multiple_resources(dagbag):
    dag = dagbag.get_dag(dag_id="registered-programs-and-drop-in-courses-offering")

    expected_task_flow = {
        "create_tmp_dir": ["get_or_create_package"],
        "get_or_create_package": [
            "download_data_RegisteredPrograms",
            "download_data_Drop-in",
            "download_data_Facilities",
            "download_data_Locations",
        ],
        "new_or_existing_RegisteredPrograms": [
            "brand_new_RegisteredPrograms",
            "does_RegisteredPrograms_need_update",
        ],
        "does_RegisteredPrograms_need_update": [
            "check_RegisteredPrograms_insert_method",
            "dont_update_resource_RegisteredPrograms",
        ],
        "insert_records_RegisteredPrograms": [
            "datastore_cache_RegisteredPrograms",
            "delete_failed_resource_RegisteredPrograms",
        ],
        "clean_backups_RegisteredPrograms": ["done_inserting_into_datastore"],
        "clean_backups_Facilities": ["done_inserting_into_datastore"],
        "clean_backups_Locations": ["done_inserting_into_datastore"],
        "clean_backups_RegisteredPrograms": ["done_inserting_into_datastore"],
    }

    for task_id, downstream_list in expected_task_flow.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert set(downstream_list) <= set(
            task.downstream_task_ids
        ), "Invalid task flow!!"
