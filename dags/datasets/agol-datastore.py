# agol-datastore.py - this file makes multiple DAGs; each is an ETL for a police annual report from AGOL into CKAN

import json

from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator

from utils_operators.directory_operator import CreateLocalDirectoryOperator, DeleteLocalDirectoryOperator
from utils_operators.agol_operators import AGOLDownloadFileOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator

from ckan_operators.package_operator import GetPackageOperator

from ckan_operators.datastore_operator import (
    BackupDatastoreResourceOperator,
    DeleteDatastoreResourceRecordsOperator,
    InsertDatastoreResourceRecordsOperator,
    RestoreDatastoreResourceBackupOperator,
    DeleteDatastoreResourceOperator,
    InsertDatastoreResourceRecordsFromJSONOperator
)
from ckan_operators.resource_operator import GetOrCreateResourceOperator, EditResourceMetadataOperator
from utils import airflow_utils

# init hardcoded vars for these dags
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

TMP_DIR = Path(Variable.get("tmp_dir"))

base_url = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/"

datasets = [
    {"schema_change": True, "name": "TPS Budget 2020", "tps_table_code": "Budget_2020", "package_id": "toronto-police-budget", "agol_dataset": "Budget_2020"},
    {"schema_change": True, "name": "TPS Budget 2021", "tps_table_code": "Budget_2021", "package_id": "toronto-police-budget", "agol_dataset": "Budget_2021"},
    {"schema_change": True, "name": "TPS Budget by command", "tps_table_code": "2020", "package_id": "toronto-police-budget-by-command", "agol_dataset": "Budget_by_Command"},
    {"schema_change": True, "name": "TPS Staffing by Command", "tps_table_code": "2020", "package_id": "toronto-police-staffing-by-command", "agol_dataset": "Staffing_by_Command"},
]

fields =  {"Budget_2020":
[
    { "id": "Fiscal_Year", "type": "text", "info": { "notes": "Fiscal Year" } }, 
    { "id": "Budget_Type", "type": "text", "info": { "notes": "Approved Budget or Proposed Budget" } },
    { "id": "Organization_Entity", "type": "text", "info": { "notes": "Organization in TPS" } },
    { "id": "Command_Name", "type": "text", "info": { "notes": "Name of command" } },
    { "id": "Pillar_Name", "type": "text", "info": { "notes": "Name of Pillar" } },
    { "id": "District_Name", "type": "text", "info": { "notes": "District  Name" } },
    { "id": "Unit_Name", "type": "text", "info": { "notes": "Unit name: Central Paid Duty" } },
    { "id": "Feature_Category", "type": "text", "info": { "notes": "Category: Revenues" } },
    { "id": "Cost_Element", "type": "text", "info": { "notes": "Number of cost element" } },
    { "id": "Cost_Element_Long_Name", "type": "text", "info": { "notes": "Name of cost element: PAID DUTY" } },
    { "id": "Amount", "type": "float8", "info": { "notes": "Budget amount" } }
],
"Budget_2021":[
    { "id": "Fiscal_Year", "type": "text", "info": { "notes": "Fiscal Year" } }, 
    { "id": "Budget_Type", "type": "text", "info": { "notes": "Approved Budget or Proposed Budget" } },
    { "id": "Organization_Entity", "type": "text", "info": { "notes": "Organization in TPS" } },
    { "id": "Command_Name", "type": "text", "info": { "notes": "Name of command" } },
    { "id": "Pillar_Name", "type": "text", "info": { "notes": "Name of Pillar" } },
    { "id": "District_Name", "type": "text", "info": { "notes": "District  Name" } },
    { "id": "Unit_Name", "type": "text", "info": { "notes": "Unit name: Central Paid Duty" } },
    { "id": "Feature_Category", "type": "text", "info": { "notes": "Category: Revenues" } },
    { "id": "Cost_Element", "type": "text", "info": { "notes": "Number of cost element" } },
    { "id": "Cost_Element_Long_Name", "type": "text", "info": { "notes": "Name of cost element: PAID DUTY" } },
    { "id": "Amount", "type": "float8", "info": { "notes": "Budget amount" } }
],
"Budget_by_Command":
[
    { "id": "Year", "type": "float8", "info": { "notes": "Fiscal Year" } },
    { "id": "Type_of_Metric", "type": "text", "info": { "notes": "Type of Metric" } },
    { "id": "Category", "type": "text", "info": { "notes": "Category" } },
    { "id": "Command", "type": "text", "info": { "notes": "Command" } },
    { "id": "Amount", "type": "float8", "info": { "notes": "Budget amount" } },
],
"Staffing_by_Command":
[
    { "id": "Year", "type": "float8", "info": { "notes": "Fiscal Year" } },
    { "id": "Type_of_Metric", "type": "text", "info": { "notes": "Type of Metric" } },
    { "id": "Command", "type": "text", "info": { "notes": "Command" } },
    { "id": "Category", "type": "text", "info": { "notes": "Category" } },
    { "id": "Count_", "type": "float8", "info": { "notes": "Budget amount" } },
],
}
common_job_settings = {
    "description": "Toronto Police Service - budget",
    "start_date": datetime(2021, 9, 30, 0, 0, 0),
    "schedule": "@once",
}

# custom function to create multiple custom dags
def create_dag(dag_id,
               dataset,
               schedule,
               default_args):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              tags=['police', 'gary']
              )

    with dag:
        # init vars
        data_filename = "data.json"
        agol_dataset = dataset["agol_dataset"]
        package_name=dataset['package_id']
        resource_name = dataset["name"]

        tmp_dir = CreateLocalDirectoryOperator(
            task_id = "tmp_dir", 
            path = TMP_DIR / dag_id
        )   

        fields_filepath = TMP_DIR / dag_id / "fields.json"

        get_package = GetPackageOperator(
            task_id="get_package",
            address=CKAN,
            apikey=CKAN_APIKEY,
            package_name_or_id=package_name,
        )

        def is_resource_new(**kwargs):
            resource = kwargs["ti"].xcom_pull(task_ids="get_or_create_resource")

            if resource["is_new"]:
                return "new_resource"

            return "existing_resource"

        new_or_existing = BranchPythonOperator(
            task_id="new_or_existing", python_callable=is_resource_new,
        )

        new_resource = DummyOperator(task_id="new_resource", dag=dag)
        existing_resource = DummyOperator(task_id="existing_resource", dag=dag)

        get_agol_data = AGOLDownloadFileOperator(
            task_id = "get_agol_data",
            request_url = base_url + agol_dataset + "/FeatureServer/0/",
            dir = TMP_DIR / dag_id,
            filename = data_filename,
            delete_col = ["geometry", "objectid"]
            #on_success_callback=task_success_slack_alert,
        )

        get_or_create_resource = GetOrCreateResourceOperator(
            task_id="get_or_create_resource",
            address=CKAN,
            apikey=CKAN_APIKEY,
            package_name_or_id=package_name,
            resource_name=resource_name,
            resource_attributes=dict(
                format="csv",
                package_id=package_name,
                is_preview=True,
                url_type="datastore",
                extract_job=f"Airflow: {package_name}",
            ),
        )

        def set_fields(**kwargs):
            ti = kwargs["ti"]
            tmp_dir = Path(ti.xcom_pull(task_ids="tmp_dir"))
            with open(fields_filepath, 'w') as fields_json_file:
                json.dump(fields[agol_dataset], fields_json_file)

            return fields_filepath

        set_fields = PythonOperator(
            task_id="set_fields", python_callable=set_fields, trigger_rule="none_failed"
        )

        backup_resource = BackupDatastoreResourceOperator(
            task_id = "backup_resource",
            address=CKAN,
            apikey=CKAN_APIKEY,
            resource_task_id="get_or_create_resource",
            dir_task_id="tmp_dir"
        )

        # delete_records = DeleteDatastoreResourceOperator(
        #     task_id="delete_records",
        #     address = CKAN,
        #     apikey = CKAN_APIKEY,
        #     resource_id_task_id = "get_or_create_resource",
        #     resource_id_task_key = "id",
        #     trigger_rule="none_failed",
        # )

        delete_records = DeleteDatastoreResourceRecordsOperator(
            task_id="delete_records",
            address = CKAN,
            apikey = CKAN_APIKEY,
            backup_task_id="backup_resource",
        )

        insert_records = InsertDatastoreResourceRecordsFromJSONOperator(
            task_id = "insert_records",
            address = CKAN,
            apikey = CKAN_APIKEY,
            resource_id_task_id = "get_or_create_resource",
            resource_id_task_key = "id",
            data_path = TMP_DIR / dag_id / data_filename,
            fields_path = fields_filepath
        )

        modify_metadata = EditResourceMetadataOperator(
            task_id = "modify_metadata",
            address = CKAN,
            apikey = CKAN_APIKEY,
            resource_id_task_id = "get_or_create_resource",
            resource_id_task_key = "id",
            new_resource_name = resource_name,
            last_modified_task_id = "get_agol_data",
            last_modified_task_key = "last_modified"           
        )

        restore_backup = RestoreDatastoreResourceBackupOperator(
            task_id = "restore_backup",
            address = CKAN,
            apikey = CKAN_APIKEY,
            backup_task_id = "backup_resource",
        )

        delete_tmp_dir = DeleteLocalDirectoryOperator(
            task_id = "delete_tmp_dir",
            path = TMP_DIR / dag_id,
            #on_success_callback=task_success_slack_alert,
        )

        job_success = DummyOperator(
            task_id = "job_success",
            trigger_rule="all_success"
        )

        job_failed = DummyOperator(
            task_id = "job_failed",
            trigger_rule="one_failed"
        )

        message_slack_success = GenericSlackOperator(
            task_id = "message_slack_success",
            message_header = dag_id + " run successfully.",
            message_content_task_id = "insert_records",
            message_content_task_key = "data_inserted"
        )

        message_slack_abort = GenericSlackOperator(
            task_id = "message_slack_abort",
            message_header = dag_id + " failed, data records not changed",
        )

        message_slack_recover = GenericSlackOperator(
            task_id = "message_slack_recover",
            message_header = dag_id + " failed to update, data records restored",
        )

        join_or = DummyOperator(task_id="join_or", dag=dag, trigger_rule="one_success")
        join_and = DummyOperator(task_id="join_and", dag=dag, trigger_rule="all_success")

        ## DAG EXECUTION LOGIC
        tmp_dir >> get_agol_data >>  join_and
        get_package >> get_or_create_resource >> new_or_existing >> [new_resource,  existing_resource ]
        new_resource >> set_fields >> join_or >> join_and
        existing_resource >> backup_resource >> delete_records >> join_or >> join_and
        join_and >> insert_records >> modify_metadata >> job_success >> delete_tmp_dir>> message_slack_success  
        [get_agol_data, get_or_create_resource] >> job_failed  >> message_slack_abort
        [insert_records] >> job_failed >> restore_backup >> message_slack_recover

    return dag


# build a dag for each number in range(10)
for dataset in datasets:
    dag_id = dataset['package_id']+'-'+dataset['tps_table_code']

    schedule = '@once'
    default_args = airflow_utils.get_default_args(
        {
            "owner": "Gary Test",
            "depends_on_past": False,
            "email": ["gary.qi@toronto.ca"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "on_failure_callback": task_failure_slack_alert,
            "retries": 0,
            "start_date": common_job_settings["start_date"],
        }
    )
    globals()[dag_id] = create_dag(dag_id,
                                  dataset,
                                  schedule,
                                  default_args)
