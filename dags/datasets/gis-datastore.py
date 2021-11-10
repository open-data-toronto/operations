# this dag generator is to create dags loading data from arcgis service to CKAN datastore
# source can be agol or toronto services
# may include geometry column to make it geojson, or tabular (CSV) data without geometry
# configuration is loaded from a master gis-config.json file hosted on opendata.toronto.ca, if missing, it would use a default local gis-config.json
# each dag may have its own schedule, or otherwise default to once and can be triigered manually

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
    RestoreDatastoreResourceBackupOperator,
    DeleteDatastoreResourceOperator,
    InsertDatastoreResourceRecordsFromJSONOperator
)
from ckan_operators.resource_operator import GetOrCreateResourceOperator, EditResourceMetadataOperator
# , DeleteResourceOperator
from utils import airflow_utils

# init setting from airflow env
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]
TMP_DIR = Path(Variable.get("tmp_dir"))

datasets = [            # this will be move to external
    {
    "package_id": "zoning-by-law", 
    "res_name":"Zoning Area",
    "schema_change": True,  # will delete re-create datastore schema 
    # "url": "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services/COTGEO_ZBL_ZONE/FeatureServer/0/", 

    # also need to use https instead of http for gis.toronto.ca end points
    "url": "https://gis.toronto.ca/arcgis/rest/services/cot_geospatial11/FeatureServer/3",
    "schedule":"@once",
    "del_columns": ["objectid"],
    "format": 'geojson',
    "fields": [
            { "id": "ZN_LU_CATEGORY", "type": "int", "info": { "notes": "ZN_LU_CATEGORY" } }, 
            { "id": "ZN_ZONE", "type": "text", "info": { "notes": "ZN_ZONE" } }, 
            { "id": "ZN_HOLDING", "type": "text", "info": { "notes": "ZN_HOLDING" } }, 
            { "id": "ZN_HOLDING_NO", "type": "int", "info": { "notes": "ZN_HOLDING_NO" } }, 
            { "id": "ZN_FRONTAGE", "type": "int", "info": { "notes": "ZN_FRONTAGE" } }, 
            { "id": "ZN_AREA", "type": "int", "info": { "notes": "ZN_AREA" } }, 
            { "id": "ZN_UNIT_COUNT", "type": "int", "info": { "notes": "ZN_UNIT_COUNT" } }, 
            { "id": "ZN_FSI_DENSITY", "type": "int", "info": { "notes": "ZN_FSI_DENSITY" } }, 
            { "id": "ZN_COVERAGE", "type": "text", "info": { "notes": "ZN_COVERAGE" } }, 
            { "id": "FSI_TOTAL", "type": "float8", "info": { "notes": "FSI_TOTAL" } }, 
            { "id": "FSI_COMMERCIAL_USE", "type": "int", "info": { "notes": "" } }, 
            { "id": "FSI_RESIDENTIAL_USE", "type": "int", "info": { "notes": "" } }, 
            { "id": "FSI_EMPLOYMENT_USE", "type": "int", "info": { "notes": "" } }, 
            { "id": "FSI_OFFICE_USE", "type": "int", "info": { "notes": "" } }, 
            { "id": "ZN_EXCPTN", "type": "text", "info": { "notes": "" } }, 
            { "id": "ZN_EXCPTN_NO", "type": "int", "info": { "notes": "" } }, 
            { "id": "STANDARDS_SET", "type": "int", "info": { "notes": "" } }, 
            { "id": "ZN_STATUS", "type": "int", "info": { "notes": "" } }, 
            { "id": "ZN_STRING", "type": "text", "info": { "notes": "" } }, 
            { "id": "ZN_AREA_UNIT", "type": "int", "info": { "notes": "" } }, 
            { "id": "BYLAW_CHAPTERLINK", "type": "text", "info": { "notes": "" } }, 
            { "id": "BYLAW_SECTIONLINK", "type": "text", "info": { "notes": "" } }, 
            { "id": "BYLAW_EXCPTNLINK", "type": "text", "info": { "notes": "" } }, 
            { "id": "BYLAW_HOLDINGLINK", "type": "text", "info": { "notes": "" } }, 
            { "id": "ZBL_CHAPTER", "type": "text", "info": { "notes": "" } }, 
            { "id": "ZBL_SECTION", "type": "text", "info": { "notes": "" } }, 
            { "id": "ZBL_EXCPTN", "type": "text", "info": { "notes": "" } }, 
        ],
    },    
    {
    "package_id": "zoning-by-law", 
    "res_name":"Zoning Height Overlay",
    "schema_change": True,  # will delete re-create datastore schema 
    "url": "https://gis.toronto.ca/arcgis/rest/services/cot_geospatial11/FeatureServer/9", 
    "schedule":"@once",
    "format": 'geojson',
    "del_columns": ["objectid"],
    "fields":None
    },    
    {
    "package_id": "zoning-by-law", 
    "res_name":" Zoning Lot Coverage Overlay",
    "schema_change": True,  # will delete re-create datastore schema 
    "url": "https://gis.toronto.ca/arcgis/rest/services/cot_geospatial11/FeatureServer/10", 
    "schedule":"@once",
    "format": 'geojson',
    "del_columns": ["objectid"],
    "fields":None
    },    
    {
    "package_id": "zoning-by-law", 
    "res_name":"Zoning By-law Policy Area Overlay",
    "schema_change": True,  # will delete re-create datastore schema 
    "url": "https://gis.toronto.ca/arcgis/rest/services/cot_geospatial11/FeatureServer/13", 
    "schedule":"@once",
    "format": 'geojson',
    "del_columns": ["objectid"],
    "fields":[
            { "id": "POLICY_AREA", "type": "text", "info": { "notes": "POLICY_AREA" } }, 
            { "id": "ZN_EXCPTN", "type": "text", "info": { "notes": "" } }, 
            { "id": "ZN_EXCPTN_NO", "type": "int", "info": { "notes": "" } }, 
            { "id": "LINK_TO_PARKINGRATE", "type": "text", "info": { "notes": "" } }, 
            { "id": "LINK_TO_EXCPTN", "type": "text", "info": { "notes": "" } }, 
            { "id": "geometry", "type": "text", "info": { "notes": "geometry of the policy area" } }, 
        ]

    },    
    {
    "package_id": "zoning-by-law", 
    "res_name":"Zoning Policy Road Overlay",
    "schema_change": True,  # will delete re-create datastore schema 
    "url": "https://gis.toronto.ca/arcgis/rest/services/cot_geospatial11/FeatureServer/14", 
    "schedule":"@once",
    "format": 'geojson',
    "del_columns": ["objectid"],
    "fields":None
    },    
    {
    "package_id": "zoning-by-law", 
    "res_name":"Zoning Zooming House Overlay",
    "schema_change": True,  # will delete re-create datastore schema 
    "url": "https://gis.toronto.ca/arcgis/rest/services/cot_geospatial11/MapServer/19", 
    "schedule":"@once",
    "format": 'geojson',
    "del_columns": ["objectid"],
    "fields":None
    },    
]

# custom function to create multiple custom dags
def create_dag(dag_id, dataset, schedule, default_args): 
    dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args, tags=['gis', 'gary'] )

    with dag:
        data_filename = "data.json"
        field_def = "fields.json"
        package_name=dataset['package_id']
        resource_name = dataset["res_name"]
        tmp_path = TMP_DIR / dag_id
        fields_filepath = tmp_path / field_def

        tmp_dir = CreateLocalDirectoryOperator(
            task_id = "tmp_dir", 
            path = tmp_path
        )   

        get_package = GetPackageOperator(
            task_id="get_package",
            address=CKAN,
            apikey=CKAN_APIKEY,
            package_name_or_id=package_name,
        )

        def delete_res_or_not():  # delete records or delete datastore schema
            if dataset["schema_change"]:
                return "delete_datastore"

            return "delete_records"

        delete_res_or_not = BranchPythonOperator(
            task_id="delete_res_or_not", python_callable=delete_res_or_not,
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
            request_url = dataset['url'],
            dir = tmp_path,
            filename = data_filename,
            delete_col = dataset['del_columns'],
            fields_json_file = fields_filepath,
        )

        get_or_create_resource = GetOrCreateResourceOperator(
            task_id="get_or_create_resource",
            address=CKAN,
            apikey=CKAN_APIKEY,
            package_name_or_id=package_name,
            resource_name=resource_name,
            resource_attributes=dict(
                format=dataset['format'],
                package_id=package_name,
                is_preview=True,
                url_type="datastore",
                extract_job=f"Airflow: {package_name}-{resource_name}",
            ),
        )

        def set_fields():
            if dataset["fields"]:
                with open(fields_filepath, 'w') as fields_json_file:
                    json.dump(dataset["fields"], fields_json_file)

            return fields_filepath

        set_fields = PythonOperator(
            task_id="set_fields", python_callable=set_fields
        )

        backup_resource = BackupDatastoreResourceOperator(
            task_id = "backup_resource",
            address=CKAN,
            apikey=CKAN_APIKEY,
            resource_task_id="get_or_create_resource",
            dir_task_id="tmp_dir"
        )

        delete_datastore = DeleteDatastoreResourceOperator(   # delete datastore schema for the resource
            task_id="delete_datastore",
            address = CKAN,
            apikey = CKAN_APIKEY,
            resource_id_task_id = "get_or_create_resource",
            resource_id_task_key = "id",
        )

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
            data_path = tmp_path / data_filename,
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
            path = tmp_path,
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
        # join_and = DummyOperator(task_id="join_and", dag=dag, trigger_rule="all_success")

        ## DAG EXECUTION LOGIC
        
        
        tmp_dir >> get_agol_data >> set_fields >> get_package >>  get_or_create_resource >> new_or_existing >> [new_resource,  existing_resource ]
        new_resource >>  join_or
        existing_resource >> backup_resource >> delete_res_or_not >> [delete_datastore, delete_records] >> join_or >> insert_records >> modify_metadata >> job_success >> delete_tmp_dir>> message_slack_success  
        [get_agol_data, get_or_create_resource] >> job_failed  >> message_slack_abort
        [insert_records] >> job_failed >> restore_backup >> message_slack_recover

    return dag


# build a dag for each number in range(10)
for dataset in datasets:
    dag_id = dataset['package_id']+'_'+dataset["res_name"].replace(' ', '-')
    default_args = airflow_utils.get_default_args(
        {
            "owner": "Gary Qi",
            "depends_on_past": False,
            "email": ["gary.qi@toronto.ca"],
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": 1,
            "on_failure_callback": task_failure_slack_alert,
            "start_date": datetime(2021, 9, 30, 0, 0, 0),
        }
    )
    globals()[dag_id] = create_dag(dag_id,
                                  dataset,
                                  dataset["schedule"],
                                  default_args)
