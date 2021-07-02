# police-annual-statistical-report.py - this file makes multiple DAGs; each is an ETL for a police annual report from AGOL into CKAN

import ckanapi

from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from utils_operators.directory_operator import CreateLocalDirectoryOperator
from utils_operators.agol_operators import AGOLDownloadFileOperator
from ckan_operators.datastore_operator import (
    BackupDatastoreResourceOperator,
    DeleteDatastoreResourceRecordsOperator,
    InsertDatastoreResourceRecordsOperator,
    RestoreDatastoreResourceBackupOperator,
    DeleteDatastoreResourceOperator
)
from ckan_operators.resource_operator import GetOrCreateResourceOperator
from utils import airflow_utils



# init hardcoded vars for these dags
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]

base_url = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/"

datasets = [
    {"tps_table_code": "ASR-RC-TBL-001", "package_id": "police-annual-statistical-report-reported-crimes", "agol_dataset": "Reported_Crimes_ASR_VC_TBL_001"},
    {"tps_table_code": "ASR-VC-TBL-001", "package_id": "police-annual-statistical-report-victims-of-crime", "agol_dataset": "Victims_of_Crime_ASR_RC_TBL_001"},
    {"tps_table_code": "ASR-SP-TBL-001", "package_id": "police-annual-statistical-report-search-of-persons", "agol_dataset": "Search_of_Persons_ASR_SP_TBL_001"},
    {"tps_table_code": "ASR-T-TBL-001", "package_id": "police-annual-statistical-report-traffic-collisions", "agol_dataset": "Traffic_Collisions_ASR_T_TBL_001"},
    {"tps_table_code": "ASR-F-TBL-001", "package_id": "police-annual-statistical-report-firearms-top-5-calibres", "agol_dataset": "Firearms_Top_5_Calibres_ASR_F_TBL_001"},
    {"tps_table_code": "ASR-F-TBL-002", "package_id": "police-annual-statistical-report-top-20-offences-of-firearms-seizures", "agol_dataset": "Top_20_Offences_of_Firearms_Seizures_ASR_F_TBL_002"},
    {"tps_table_code": "ASR-F-TBL-003", "package_id": "police-annual-statistical-report-miscellaneous-firearms", "agol_dataset": "Miscellaneous_Firearms_ASR_F_TBL_003"},
    {"tps_table_code": "ASR-PB-TBL-001", "package_id": "police-annual-statistical-report-gross-expenditures-by-division", "agol_dataset": "Gross_Expenditures_by_Division_ASR_PB_TBL_001"},
    {"tps_table_code": "ASR-PB-TBL-002", "package_id": "police-annual-statistical-report-personnel-by-rank", "agol_dataset": "Personnel_by_Rank_ASR_PB_TBL_002"},
    {"tps_table_code": "ASR-PB-TBL-003", "package_id": "police-annual-statistical-report-personnel-by-rank-by-division", "agol_dataset": "Personnel_by_Rank_by_Division_ASR_PB_TBL_003"},
    {"tps_table_code": "ASR-PB-TBL-004", "package_id": "police-annual-statistical-report-personnel-by-command", "agol_dataset": "Personnel_by_Command_ASR_PB_TBL_004"},
    {"tps_table_code": "ASR-PB-TBL-005", "package_id": "police-annual-statistical-report-gross-operating-budget", "agol_dataset": "Gross_Operating_Budget_ASR_PB_TBL_005"},
    {"tps_table_code": "ASR-CS-TBL-001", "package_id": "police-annual-statistical-report-dispatched-calls-by-division", "agol_dataset": "Dispatched_Calls_by_Division_ASR_CS_TBL_001"},
    {"tps_table_code": "ASR-CS-TBL-002", "package_id": "police-annual-statistical-report-miscellaneous-calls-for-service", "agol_dataset": "Miscellaneous_Calls_for_Service_ASR_CS_TBL_002"},
    {"tps_table_code": "ASR-PCF-TBL-001", "package_id": "police-annual-statistical-report-total-public-complaints", "agol_dataset": "Total_Public_Complaints_ASR_PCF_TBL_001"},
    {"tps_table_code": "ASR-PCF-TBL-002", "package_id": "police-annual-statistical-report-investigated-alleged-misconduct", "agol_dataset": "Investigated_Alleged_Misconduct_ASR_PCF_TBL_002"},
    {"tps_table_code": "ASR-PCF-TBL-003", "package_id": "police-annual-statistical-report-complaint-dispositions", "agol_dataset": "Complaint_Dispositions_ASR_PCF_TBL_003"},
    {"tps_table_code": "ASR-RI-TBL-001", "package_id": "police-annual-statistical-report-regulated-interactions", "agol_dataset": "Regulated_Interactions_ASR_RI_TBL_001"},
    {"tps_table_code": "ASR-RI-TBL-001", "package_id": "police-annual-statistical-report-regulated-interactions-demographics", "agol_dataset": "Regulated_Interactions_Demographics_ASR_RI_TBL_001"},
    {"tps_table_code": "ASR-AD-TBL-001", "package_id": "police-annual-statistical-report-administrative", "agol_dataset": "Administrative_ASR_AD_TBL_001"},
    {"tps_table_code": "ASR-MISC-TBL-001", "package_id": "police-annual-statistical-report-miscellaneous-data", "agol_dataset": "Miscellaneous_Data_ASR_MISC_TBL_001"}
]

common_job_settings = {
    "description": "Toronto Police Service - Annual Services Report for dataset",
    "start_date": datetime(2021, 6, 30, 0, 0, 0),
    "schedule": "@once",
}

# init slack failure message function
def send_failure_message():
    airflow_utils.message_slack(
        name=common_job_settings["description"],
        message_type="error",
        msg="Job not finished",
        active_env=Variable.get("active_env"),
        prod_webhook=Variable.get("active_env") == "prod",
    )

# custom function to create multiple custom dags
def create_dag(dag_id,
               agol_dataset,
               schedule,
               default_args):

    dag = DAG(dag_id,
              agol_dataset,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:

        tmp_dir = CreateLocalDirectoryOperator(
            task_id = "tmp_dir", 
            path = Path(Variable.get("tmp_dir")) / dag_id
        )   

        get_agol_data = AGOLDownloadFileOperator(
            task_id = "get_data_from_agol",
            file_url = base_url + agol_dataset + "/FeatureServer/0/",
            dir = Path(Variable.get("tmp_dir")) / dag_id,
            filename = dag_id + ".json"
        )

        delete_resource = DeleteDatastoreResourceOperator(
            task_id="delete_resource",
            address = CKAN,
            apikey = CKAN_APIKEY,
            resource_id_filepath = Path(Variable.get("tmp_dir")) / dag_id / "resource_id.txt"

        )

        get_or_create_resource = GetOrCreateResourceOperator(
            task_id="get_or_create_resource",
            address=CKAN,
            apikey=CKAN_APIKEY,
            package_name_or_id=dag_id,
            resource_name=dag_id,
            resource_id_filepath = Path(Variable.get("tmp_dir")) / dag_id / "resource_id.txt",
            resource_attributes=dict(
                format="csv",
                is_preview=True,
                url_type="datastore",
                extract_job=f"Airflow: {dag_id}",
            ),
        )

        ## DAG EXECUTION LOGIC
        tmp_dir >> get_agol_data >> get_or_create_resource >> delete_resource
        


        

    return dag


# build a dag for each number in range(10)
for dataset in datasets:
    dag_id = dataset['package_id']
    agol_dataset = dataset['agol_dataset']

    schedule = '@once'
    default_args = airflow_utils.get_default_args(
        {
            "owner": "Mackenzie",
            "depends_on_past": False,
            "email": ["mackenzie.nichols4@toronto.ca"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "on_failure_callback": send_failure_message,
            "retries": 0,
            "start_date": common_job_settings["start_date"],
        }
    )
    globals()[dag_id] = create_dag(dag_id,
                                  agol_dataset,
                                  schedule,
                                  default_args)