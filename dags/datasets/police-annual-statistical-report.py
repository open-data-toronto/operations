# police-annual-statistical-report.py - this file makes multiple DAGs; each is an ETL for a police annual report from AGOL into CKAN

import ckanapi
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

BASE_URL = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/"

DATASETS = [
    {"schema_change": False, "name": "Reported Crimes", "tps_table_code": "ASR-RC-TBL-001", "package_id": "police-annual-statistical-report-reported-crimes", "agol_dataset": "Reported_Crimes_ASR_RC_TBL_001"},
    {"schema_change": False, "name": "Victims of Crime", "tps_table_code": "ASR-VC-TBL-001", "package_id": "police-annual-statistical-report-victims-of-crime", "agol_dataset": "Victims_of_Crime_ASR_VC_TBL_001"},
    {"schema_change": False, "name": "Search of Persons", "tps_table_code": "ASR-SP-TBL-001", "package_id": "police-annual-statistical-report-search-of-persons", "agol_dataset": "Search_of_Persons_ASR_SP_TBL_001"},
    {"schema_change": True, "name": "Traffic Collisions", "tps_table_code": "ASR-T-TBL-001", "package_id": "police-annual-statistical-report-traffic-collisions", "agol_dataset": "Traffic_Collisions_ASR_T_TBL_001"},
    {"schema_change": False, "name": "Firearms Top 5 Calibres", "tps_table_code": "ASR-F-TBL-001", "package_id": "police-annual-statistical-report-firearms-top-5-calibres", "agol_dataset": "Firearms_Top_5_Calibres_ASR_F_TBL_001"},
    {"schema_change": False, "name": "Top 20 Offences of Firearms Seizures", "tps_table_code": "ASR-F-TBL-002", "package_id": "police-annual-statistical-report-top-20-offences-of-firearms-seizures", "agol_dataset": "Top_20_Offences_of_Firearm_Seizures_ASR_F_TBL_002"},
    {"schema_change": False, "name": "Miscellaneous Firearms", "tps_table_code": "ASR-F-TBL-003", "package_id": "police-annual-statistical-report-miscellaneous-firearms", "agol_dataset": "Miscellaneous_Firearms_ASR_F_TBL_003"},
    {"schema_change": False, "name": "Gross Expenditures by Division", "tps_table_code": "ASR-PB-TBL-001", "package_id": "police-annual-statistical-report-gross-expenditures-by-division", "agol_dataset": "Gross_Expenditures_by_Division_ASR_PB_TBL_001"},
    {"schema_change": False, "name": "Personnel by Rank", "tps_table_code": "ASR-PB-TBL-002", "package_id": "police-annual-statistical-report-personnel-by-rank", "agol_dataset": "Personnel_by_Rank__ASR_PB_TBL_002"},
    {"schema_change": False, "name": "Personnel by Rank by Division", "tps_table_code": "ASR-PB-TBL-003", "package_id": "police-annual-statistical-report-personnel-by-rank-by-division", "agol_dataset": "Personnel_by_Rank_by_Division_ASR_PB_TBL_003"},
    {"schema_change": False, "name": "Personnel by Command", "tps_table_code": "ASR-PB-TBL-004", "package_id": "police-annual-statistical-report-personnel-by-command", "agol_dataset": "Personnel_by_Command_ASR_PB_TBL_004"},
    {"schema_change": False, "name": "Gross Operating Budget", "tps_table_code": "ASR-PB-TBL-005", "package_id": "police-annual-statistical-report-gross-operating-budget", "agol_dataset": "Gross_Operating_Budget_ASR_PB_TBL_005"},
    {"schema_change": False, "name": "Dispatched Calls by Division", "tps_table_code": "ASR-CS-TBL-001", "package_id": "police-annual-statistical-report-dispatched-calls-by-division", "agol_dataset": "Dispatched_Calls_by_Division_ASR_CS_TBL_001"},
    {"schema_change": False, "name": "Miscellaneous Calls for Service", "tps_table_code": "ASR-CS-TBL-002", "package_id": "police-annual-statistical-report-miscellaneous-calls-for-service", "agol_dataset": "Miscellaneous_Calls_for_Service_ASR_CS_TBL_002"},
    {"schema_change": False, "name": "Total Public Complaints", "tps_table_code": "ASR-PCF-TBL-001", "package_id": "police-annual-statistical-report-total-public-complaints", "agol_dataset": "Total_Public_Complaints_(ASR_PCF_TBL_001)"},
    {"schema_change": False, "name": "Investigated Alleged Misconduct", "tps_table_code": "ASR-PCF-TBL-002", "package_id": "police-annual-statistical-report-investigated-alleged-misconduct", "agol_dataset": "Investigated_Alleged_Misconduct__ASR_PCF_TBL_002"},
    {"schema_change": False, "name": "Complaint Dispositions", "tps_table_code": "ASR-PCF-TBL-003", "package_id": "police-annual-statistical-report-complaint-dispositions", "agol_dataset": "Complaint_Dispositions__ASR_PCF_TBL_003"},
    {"schema_change": False, "name": "Administrative", "tps_table_code": "ASR-AD-TBL-001", "package_id": "police-annual-statistical-report-administrative", "agol_dataset": "Administrative_ASR_AD_TBL_001"},
    {"schema_change": False, "name": "Miscellaneous Data", "tps_table_code": "ASR-MISC-TBL-001", "package_id": "police-annual-statistical-report-miscellaneous-data", "agol_dataset": "Miscellaneous_Data_ASR_MISC_TBL_001"}
]

COMMON_JOB_SETTINGS = {
    "description": "Toronto Police Service - Annual Services Report for dataset",
    "start_date": datetime(2021, 6, 30, 0, 0, 0),
    "schedule": "@once",
}

# custom function to create multiple custom dags
def create_dag(dag_id,
               dataset,
               schedule,
               default_args):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args
              )

    with dag:
        # init vars
        data_filename = dag_id + ".json"
        agol_dataset = dataset["agol_dataset"]
        name = dataset["name"]

        if dataset["schema_change"]:
            fields_path_task_id = "get_agol_data"
            fields_path_task_key = "fields_path"
        
        else:
            fields_path_task_id = "backup_resource"
            fields_path_task_key = "fields_file_path"

        tmp_dir = CreateLocalDirectoryOperator(
            task_id = "tmp_dir", 
            path = TMP_DIR / dag_id
        )   

        get_agol_data = AGOLDownloadFileOperator(
            task_id = "get_agol_data",
            file_url = base_url + agol_dataset + "/FeatureServer/0/",
            dir = TMP_DIR / dag_id,
            filename = dag_id + ".json",
            #on_success_callback=task_success_slack_alert,
        )

        get_resource_id = GetOrCreateResourceOperator(
            task_id="get_resource_id",
            address=CKAN,
            apikey=CKAN_APIKEY,
            package_name_or_id=dag_id,
            resource_name=name,
            #resource_id_filepath = TMP_DIR / dag_id / "resource_id.txt",
            resource_attributes=dict(
                format="csv",
                is_preview=True,
                url_type="datastore",
                extract_job=f"Airflow: {dag_id}",
            ),
        )

        backup_resource = BackupDatastoreResourceOperator(
            task_id = "backup_resource",
            address=CKAN,
            apikey=CKAN_APIKEY,
            resource_task_id="get_resource_id",
            dir_task_id="tmp_dir"

        )

        delete_resource = DeleteDatastoreResourceOperator(
            task_id="delete_resource",
            address = CKAN,
            apikey = CKAN_APIKEY,
            resource_id_task_id = "get_resource_id",
            resource_id_task_key = "id"
        )

        insert_records = InsertDatastoreResourceRecordsFromJSONOperator(
            task_id = "insert_records",
            address = CKAN,
            apikey = CKAN_APIKEY,
            resource_id_task_id = "get_resource_id",
            resource_id_task_key = "id",
            data_path = TMP_DIR / dag_id / data_filename,
            fields_path_task_id = fields_path_task_id,
            fields_path_task_key = fields_path_task_key
        )

        modify_metadata = EditResourceMetadataOperator(
            task_id = "modify_metadata",
            address = CKAN,
            apikey = CKAN_APIKEY,
            resource_id_task_id = "get_resource_id",
            resource_id_task_key = "id",
            new_resource_name = name,
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

        message_slack = GenericSlackOperator(
            task_id = "message_slack",
            message_header = "Police Annual Statistical Report Job Succeeded",
            message_content_task_id = "insert_records",
            message_content_task_key = "data_inserted"
        )

        ## DAG EXECUTION LOGIC
        tmp_dir >> get_agol_data >> get_resource_id >> backup_resource >> delete_resource >> insert_records >> modify_metadata >>  delete_tmp_dir >> job_success >> message_slack 
        [modify_metadata, insert_records] >> job_failed >> restore_backup
    return dag


# build a dag for each number in range(10)
for dataset in datasets:
    dag_id = dataset['package_id']
    #agol_dataset = dataset['agol_dataset']

    schedule = '@once'
    default_args = airflow_utils.get_default_args(
        {
            "owner": "Mackenzie",
            "depends_on_past": False,
            "email": ["mackenzie.nichols4@toronto.ca"],
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