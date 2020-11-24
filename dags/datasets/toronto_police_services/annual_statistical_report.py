from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.models import Variable
import ckanapi

# import logging
from pathlib import Path
from airflow import DAG
import sys
import json

sys.path.append(Variable.get("repo_dir"))
from utils import airflow as airflow_utils  # noqa: E402
from utils import agol as agol_utils  # noqa: E402
from utils import ckan as ckan_utils  # noqa: E402

common_job_settings = {
    "description": "Toronto Police Service - Annual Services Report for dataset",
    "start_date": datetime(2020, 11, 15, 0, 0, 0),
    "schedule": "@once",
}

extracts = [
    {
        "package_id": "police-annual-statistical-report-reported-crimes",
        "tps_table_code": "ASR-RC-TBL-001",
        "agol_dataset": "Reported_Crimes_ASR_RC_TBL_001",
        "fields": [
            "Index_",
            "ReportedYear",
            "GeoDivision",
            "Category",
            "Subtype",
            "Count_",
            "CountCleared",
        ],
    },
    {
        "package_id": "police-annual-statistical-report-traffic-collisions",
        "tps_table_code": "ASR-T-TBL-001",
        "agol_dataset": "Traffic_Collisions_ASR_T_TBL_001",
        "fields": [
            "Index_",
            "OccurredYear",
            "GeoDivision",
            "Category",
            "Subtype",
            "Count_",
        ],
    },
    {
        "package_id": "police-annual-statistical-report-victims-of-crime",
        "tps_table_code": "ASR-VC-TBL-001",
        "agol_dataset": "Victims_of_Crime_ASR_VC_TBL_001",
        "fields": [
            "Index_",
            "ReportedYear",
            "Category",
            "Subtype",
            "AssaultSubtype",
            "Sex",
            "AgeGroup",
            "AgeCohort",
            "Count_",
        ],
    },
    {
        "package_id": "police-annual-statistical-report-search-of-persons",
        "tps_table_code": "ASR-SP-TBL-001",
        "agol_dataset": "Search_of_Persons_ASR_SP_TBL_001",
        "fields": [
            "Index_",
            "SearchYear",
            "SearchLevel",
            "SelfIdentifyTrans",
            "Evidence",
            "Escape_",
            "Injury",
            "Other",
            "Count_",
        ],
    },
    {
        "package_id": "police-annual-statistical-report-firearms-top-5-calibres",
        "tps_table_code": "ASR-F-TBL-001",
        "agol_dataset": "Firearms_Top_5_Calibres_ASR_F_TBL_001",
        "fields": ["Index_", "Year", "Firearm_Type", "Calibre"],
    },
    {
        "package_id": "police-annual-statistical-report-top-20-offences-of-firearms-seizures",  # noqa: E501
        "tps_table_code": "ASR-F-TBL-002",
        "agol_dataset": "Top_20_Offences_of_Firearm_Seizures_ASR_F_TBL_002",
        "fields": ["SeizedYear", "Rank", "offence"],
    },
    {
        "package_id": "police-annual-statistical-report-miscellaneous-firearms",
        "tps_table_code": "ASR-F-TBL-003",
        "agol_dataset": "Miscellaneous_Firearms_ASR_F_TBL_003",
        "fields": ["Index_", "Year", "Category", "Type", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-gross-expenditures-by-division",
        "tps_table_code": "ASR-PB-TBL-001",
        "api_endpoint": "&outSR=4326&f=json",
        "agol_dataset": "Gross_Expenditures_by_Division_ASR_PB_TBL_001",
        "fields": [
            "Index_",
            "Year",
            "Category",
            "Division",
            "Command",
            "Gross_Expenditure__final_",
        ],
    },
    {
        "package_id": "police-annual-statistical-report-personnel-by-rank",
        "tps_table_code": "ASR-PB-TBL-002",
        "agol_dataset": "Personnel_by_Rank_ASR_PB_TBL_002",
        "fields": ["Index_", "Year", "Rank", "Classification", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-personnel-by-rank-by-division",
        "tps_table_code": "ASR-PB-TBL-003",
        "api_endpoint": "&outSR=4326&f=json",
        "agol_dataset": "Personnel_by_Rank_by_Division_ASR_PB_TBL_003",
        "fields": [
            "Index_",
            "Year",
            "Command_Group",
            "Command_Subunit",
            "Unit",
            "Classification",
            "Count_",
        ],
    },
    {
        "package_id": "police-annual-statistical-report-personnel-by-command",
        "tps_table_code": "ASR-PB-TBL-004",
        "agol_dataset": "Personnel_by_Command_ASR_PB_TBL_004",
        "fields": ["Index_", "Year", "Command", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-gross-operating-budget",
        "tps_table_code": "ASR-PB-TBL-005",
        "agol_dataset": "Gross_Operating_Budget_ASR_PB_TBL_005",
        "fields": ["Index_", "Year", "Section", "Category", "Subtype", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-dispatched-calls-by-division",
        "tps_table_code": "ASR-CS-TBL-001",
        "agol_dataset": "Dispatched_Calls_by_Division_ASR_CS_TBL_001",
        "fields": ["Index_", "Year", "Category", "Unit", "Command", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-miscellaneous-calls-for-service",  # noqa: E501
        "tps_table_code": "ASR-CS-TBL-002",
        "agol_dataset": "Miscellaneous_Calls_for_Service_ASR_CS_TBL_002",
        "fields": ["Index_", "Year", "Category", "Type", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-total-public-complaints",
        "tps_table_code": "ASR-PCF-TBL-001",
        "agol_dataset": "ASR_PCF_TBL_001",
        "fields": ["Index_", "Year", "Type", "Subtype", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-investigated-alleged-complaints",  # noqa: E501
        "tps_table_code": "ASR-PCF-TBL-002",
        "agol_dataset": "Investigated_Alleged_Misconduct_ASR_PCF_TBL_002",
        "fields": ["Index_", "Year", "Type", "Subtype", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-dispositions",
        "tps_table_code": "ASR-PCF-TBL-003",
        "agol_dataset": "Complaint_Dispositions_ASR_PCF_TBL_003",
        "fields": ["Index_", "Year", "Type", "Subtype", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-regulated-interactions",
        "tps_table_code": "ASR-RI-TBL-001",
        "agol_dataset": "Regulated_Interactions_ASR_RI_TBL_001",
        "fields": ["Index_", "Year", "Category", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-regulated-interactions-demographics",  # noqa: E501
        "tps_table_code": "ASR-RI-TBL-002",
        "agol_dataset": "Regulated_Interactions_Demographics_ASR_RI_TBL_002",
        "fields": ["Index_", "Year", "Category", "Subtype", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-administrative",
        "tps_table_code": "ASR-AD-TBL-001",
        "agol_dataset": "Administrative_ASR_AD_TBL_001",
        "fields": ["Index_", "Year", "Section", "Category", "Subtype", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-miscellaneous-data",
        "tps_table_code": "ASR-MISC-TBL-001",
        "agol_dataset": "Miscellaneous_Data_ASR_MISC_TBL_001",
        "fields": ["Index_", "Year", "Section", "Category", "Subtype", "Count_"],
    },
    {
        "package_id": "police-annual-statistical-report-shooting-occurrences",
        "tps_table_code": "ASR-SH-TBL-001",
        "agol_dataset": "Shooting_Occurrence_ASR_SH_TBL_001",
        "fields": ["Index_", "OccurredYear", "GeoDivision", "Category", "Count_"],
    },
]

# ACTIVE_ENV = Variable.get("active_env")
ACTIVE_ENV = "qa"
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])
APIKEY = Variable.get("flowworks_apikey")


def create_dag(dag_id, entry):
    description = f"{common_job_settings['description']}: {entry['package_id']}"
    agol_dataset = entry.pop("agol_dataset")
    fields_to_capture = entry.pop("fields")
    resource_name = f"{entry['package_id']}-data"

    # def send_success_msg(**kwargs):
    #     msg = kwargs.pop("ti").xcom_pull(task_ids="build_message")
    #     airflow_utils.message_slack(
    #         name=dag_id,
    #         message_type="success",
    #         msg=msg,
    #     )

    def send_failure_msg(self):
        airflow_utils.message_slack(
            name=dag_id,
            message_type="error",
            msg="Job not finished",
        )

    def build_api_endpoint():
        api_path = "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services"

        filters = [
            "where=1%3D1",
            "outSR=4326",
            "f=json",
            "resultType=standard",
            f"outFields={','.join(fields_to_capture)}",
        ]

        return f"{api_path}/{agol_dataset}/FeatureServer/0/query?{'&'.join(filters)}"

    def get_data(**kwargs):
        ti = kwargs.pop("ti")
        api_endpoint = ti.xcom_pull(task_ids="build_api_endpoint")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))
        filepath = tmp_dir / "data.json"

        data = agol_utils.get_data(api_endpoint)

        with open(filepath, "w") as f:
            json.dump(data, f)

        return filepath

    def get_agol_fields(**kwargs):
        ti = kwargs.pop("ti")
        api_endpoint = ti.xcom_pull(task_ids="build_api_endpoint")
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))
        filepath = tmp_dir / "agol_fields.json"

        data = agol_utils.get_fields(api_endpoint)

        with open(filepath, "w") as f:
            json.dump(data, f)

        return filepath

    def create_ckan_data_dict(**kwargs):
        ti = kwargs.pop("ti")
        agol_fields_fp = Path(ti.xcom_pull(task_ids="get_agol_fields"))
        tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_dir"))
        ckan_fields_fp = tmp_dir / "ckan_fields.json"

        with open(agol_fields_fp, "r") as f:
            agol_fields = json.load(f)

        ckan_fields = agol_utils.convert_dtypes_to_ckan(agol_fields)
        with open(ckan_fields_fp, "w") as f:
            json.dump(ckan_fields, f)

        return ckan_fields_fp

    # def build_message(**kwargs):
    #     ti = kwargs.pop("ti")
    #     datapoints_fp = ti.xcom_pull(task_ids="get_site_datapoints")
    #     datapoints = pd.read_csv(datapoints_fp)
    #     start_date = ti.xcom_pull(task_ids="get_from_timestamp")
    #     time_lastest_loaded = datetime.strptime(start_date, "%Y%m%d%H%M%S").strftime(
    #         "%Y-%m-%d %H:%M:%S"
    #     )

    #     return f"{datapoints.shape[0]} new records found since {time_lastest_loaded}"

    def check_if_new_resource(**kwargs):
        ti = kwargs.pop("ti")
        package = ti.xcom_pull(task_ids="get_package")

        is_new = resource_name not in [r["name"] for r in package["resources"]]

        if is_new:
            return "resource_is_new"

        return "resource_is_not_new"

    def create_new_resource(**kwargs):
        ti = kwargs.pop("ti")
        data_dict_fp = Path(ti.xcom_pull(task_ids="create_ckan_data_dict"))

        with open(data_dict_fp, "r") as f:
            fields = json.load(f)

        resource = {
            "package_id": entry["package_id"],
            "name": resource_name,
            "format": "csv",
            "is_preview": True,
        }

        CKAN.action.datastore_create(resource=resource, records=[], fields=fields)

    def get_resource_id():
        package = ckan_utils.get_package(ckan=CKAN, package_id=entry["package_id"])
        resources = package["resources"]

        resource = [r for r in resources if r["name"] == resource_name][0]

        assert (
            resource["datastore_active"] is True
        ), f"Resource {resource_name} found but not in datastore"

        return resource["id"]

    def insert_records(**kwargs):
        ti = kwargs.pop("ti")
        resource_id = ti.xcom_pull(task_ids="get_resource_id")
        data_fp = ti.xcom_pull(task_ids="get_data")

        with open(data_fp, "r") as f:
            records = json.load(f)

        chunk_size = int(Variable.get("ckan_insert_chunk_size"))

        ckan_utils.insert_datastore_records(
            ckan=CKAN, resource_id=resource_id, records=records, chunk_size=chunk_size
        )

    dag = DAG(
        dag_id,
        default_args=airflow_utils.get_default_args(
            {
                # "on_failure_callback": send_failure_msg,
                "start_date": common_job_settings["start_date"],
                "retries": 0,
            }
        ),
        description=description,
        schedule_interval=common_job_settings["schedule"],
    )

    with dag:
        api_endpoint = PythonOperator(
            task_id="build_api_endpoint",
            python_callable=build_api_endpoint,
        )

        create_tmp_dir = PythonOperator(
            task_id="create_tmp_dir",
            python_callable=airflow_utils.create_dir_with_dag_name,
            op_kwargs={"dag_id": dag_id, "dir_variable_name": "tmp_dir"},
        )

        data = PythonOperator(
            task_id="get_data",
            python_callable=get_data,
            provide_context=True,
        )

        agol_fields = PythonOperator(
            task_id="get_agol_fields",
            python_callable=get_agol_fields,
            provide_context=True,
        )

        ckan_data_dict = PythonOperator(
            task_id="create_ckan_data_dict",
            python_callable=create_ckan_data_dict,
            provide_context=True,
        )

        package = PythonOperator(
            task_id="get_package",
            op_kwargs={"ckan": CKAN, "package_id": entry["package_id"]},
            python_callable=ckan_utils.get_package,
        )

        new_resource = PythonOperator(
            task_id="create_new_resource",
            python_callable=create_new_resource,
            provide_context=True,
        )

        resource_id = PythonOperator(
            task_id="get_resource_id",
            python_callable=get_resource_id,
            trigger_rule="one_success",
        )

        is_resource_new = BranchPythonOperator(
            task_id="is_resource_new",
            python_callable=check_if_new_resource,
            provide_context=True,
        )

        resource_is_not_new = DummyOperator(
            task_id="resource_is_not_new",
        )

        resource_is_new = DummyOperator(
            task_id="resource_is_new",
        )

        insert = PythonOperator(
            task_id="insert_records",
            python_callable=insert_records,
            provide_context=True,
        )

        api_endpoint >> [data, agol_fields]
        create_tmp_dir >> [data, agol_fields]

        package >> is_resource_new
        is_resource_new >> resource_is_new
        is_resource_new >> resource_is_not_new

        resource_is_new >> agol_fields >> ckan_data_dict >> new_resource
        resource_is_not_new >> resource_id

        new_resource >> resource_id
        [resource_id, data] >> insert

    return dag


for entry in extracts:
    dag_id = entry["package_id"].replace("-", "_")

    globals()[dag_id] = create_dag(dag_id, entry)
