import pandas as pd
import requests
import ckanapi
import math
import re
from datetime import timedelta

import hashlib
import json
import logging
import os
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from ckan_operators.datastore_operator import (
    BackupDatastoreResourceOperator,
    DeleteDatastoreResourceRecordsOperator,
    InsertDatastoreResourceRecordsOperator,
    RestoreDatastoreResourceBackupOperator,
)
from ckan_operators.package_operator import GetPackageOperator
from ckan_operators.resource_operator import (
    GetOrCreateResourceOperator,
    ResourceAndFileOperator,
)
from dateutil import parser
from utils import agol_utils, airflow_utils, ckan_utils
from utils_operators.directory_operator import CreateLocalDirectoryOperator
from utils_operators.file_operators import DownloadFileOperator
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator

RESOURCE_NAME = "Toronto's Dashboard - Key metrics"
tpp_measure_url = "https://www.toronto.ca/app_content/tpp_measures"
tpp_narratives_url = "https://www.toronto.ca/app_content/tpp_narratives/"
PACKAGE_NAME = "toronto-s-dashboard-key-indicators"
EXPECTED_COLUMNS = [
    "measure_id",		
    "measure_name",		
    "interval_type",		
    "value_type",		
    "measure_value",		
    "target",
    "year_to_date_variance",		
    "budget_variance",		
    "decimal_accuracy",		
    "desired_direction",		
    "category",		
    "data_source_notes",		
    "city_perspective_note",		
    "year",		
    "period_number_in_year",		
    "keywords",	
    "notes"		
    ]

mapping = {
    "id": "measure_id",
    "m": "measure_name",
    "it": "interval_type",
    "vt": "value_type",
    "v": "variance",
    "yv": "year_to_date_variance",
    "bv": "budget_variance",
    "da": "decimal_accuracy", 
    "dd": "desired_direction",
    "c": "category",
    "ds":"data_source_notes",
    "cp": "city_perspective_note",
    "y": "year",
    "p": "period_number_in_year",
    "v": "measure_value",
    "target":"target",
    "note":"note",
    "c": "category",
}

def get_category_measures(measures, category):
    subset = []
    for m in measures:
        assert len(m["c"]) == 1, f"Measure has more than 1 category: {m['c']}"
        if m["c"][0].lower() == category.lower():
            subset.append(m)
            
    return subset


def make_measures_records(measures):
    records = []
    
    for i in measures:
        item = { **i }
        data_points = item.pop("vs")
        
        assert len(i["c"]) == 1, f"Item '{i['m']}' ({i['id']}) belongs to more than 1 category: {item['c']}"
        
        item["c"] = item["c"][0].replace("&amp;", "&")
        
        for dp in data_points:
            r = { k: v for k, v in {**item, **dp}.items() if v == v }
            r["m"] = r["m"].replace("\n", " ").replace("&amp;", "&")
            r["ds"] = r["ds"].replace("&amp;", "&")
            r.pop("ytd")
            r.pop("ht")
            r.pop("kw")
            if "da" in r:
                try:
                    r["da"] = int(r["da"])
                except:
                    r.pop("da")
            if "yv" in r:
                try:
                    r["yv"] = float(r["yv"])
                except:
                    r.pop("yv")
            if "bv" in r:
                try:
                    r["bv"] = float(r["bv"])
                except:
                    r.pop("bv")
            
            for original,updated in mapping.items():
                if original in r:
                    r[updated] = r.pop(original)

            records.append(r)
            
    return records

def ds_fields():
    fields=[{'info': {'notes': 'ID Number assigned to uniquely identify each Measure'},  'type': 'float8',  'id': 'measure_id'},
            {'info': {'notes': 'Measure Name'},  'type': 'text',  'id': 'measure_name'},
            {'info': {'notes': 'Interval Type for measure result collection'},  'type': 'text',  'id': 'interval_type'},
            {'info': {'notes': 'Value Type of measure result'},  'type': 'text',  'id': 'value_type'},
            {'info': {'notes': 'Actual value of the measure'},  'type': 'float8',  'id': 'measure_value'},
            {'info': {'notes': 'Year To Date Variance to compare % Changed for Current Year-To-Date vs. Previous Year to determine Analysis in Trend Analysis'},  'type': 'float8',  'id': 'year_to_date_variance'},
            {'info': {'notes': 'Budget Variance to compare % Changed for Current Period vs. Budget/Target to determine Analysis in Trend Analysis'},  'type': 'float8',  'id': 'budget_variance'},
            {'info': {'notes': 'Decimal Accuracy - number of decimals to display in the measure result'},  'type': 'int4',  'id': 'decimal_accuracy'},
            {'info': {'notes': 'Desired Direction - determines colour and direction of trend arrows'},  'type': 'text',  'id': 'desired_direction'},
            {'info': {'notes': 'Category to which measure is assigned'},  'type': 'text',  'id': 'category'},
            {'info': {'notes': 'DataSource Notes to display under the Chart'},  'type': 'text',  'id': 'data_source_notes'},
            {'info': {'notes': 'CityPerspective Note to display under Trend Analysis'},  'type': 'text',  'id': 'city_perspective_note'},
            {'info': {'notes': 'Year', 'label': ''}, 'type': 'int4', 'id': 'year'}, 
            {'info': {'notes': 'Period number - Month'},  'type': 'int4',  'id': 'period_number_in_year'},
            {'info': {'notes': 'Target value of the measure'}, 'type': 'float8',  'id': 'target'},
            {'info': {'notes': 'Note'}, 'type': 'text',  'id': 'note'}]
    
    return fields

def string_to_dict(string, pattern):
    regex = re.sub(r'{(.+?)}', r'(?P<_\1>.+)', pattern)
    values = list(re.search(regex, string).groups())
    keys = re.findall(r'{(.+?)}', pattern)
    _dict = dict(zip(keys, values))
    return _dict

def build_narratives_df(notes):
    p_map = {
        "January": 1,   
        "February":2,
        "March":3,
        "April":4,
        "May":5,
        "June":6,
        "July":7,
        "August":8,
        "September":9,
        "October":10,
        "November":11,
        "December":12,
        "Spring":2,
        "Summer":3,
        "Fall":4,
        "Winter":1,
    }

    pattern1 = {"a":"^\[Quarter {period_number_in_year} {year}\]{note}$", "b":"\[Quarter \d \d{4}].*"}
    pattern2 = {"a":"^\[Annual {year}\]{note}$","b":"\[Annual \d{4}].*"}
    pattern3 = {"a":"^\[{period_number_in_year} {year}\]{note}$","b":"\[\w{3,15} \d{4}].*"}

    narratives=[]
    for k,v in notes.items():
        if len(v) > 10:
            for n in v.split('<br /><br />'):
                note = None
                nn = n.replace("<br />", "").strip().replace("&amp;", "&")
                if re.fullmatch(pattern1["b"], nn, flags=0):
                    note = string_to_dict(nn,pattern1["a"])
                elif re.fullmatch(pattern2["b"], nn, flags=0):
                    note = string_to_dict(nn,pattern2["a"])
                    note["period_number_in_year"] = note["year"]
                elif re.fullmatch(pattern3["b"], nn, flags=0):
                    note = string_to_dict(nn,pattern3["a"])
                    note['period_number_in_year'] = p_map[note['period_number_in_year']]
                else:
                    None
                    # print("note does not match pattern:", n)

                if note:
                    note["year"] = int(note["year"])
                    note["period_number_in_year"] = int(note["period_number_in_year"])
                    note["measure_id"] = float(k)
                    narratives.append(note)

    return pd.DataFrame(narratives)

def send_failure_message():
    airflow_utils.message_slack(
        name=PACKAGE_NAME,
        message_type="error",
        msg="Job not finished",
        active_env=Variable.get("active_env"),
        prod_webhook=Variable.get("active_env") == "prod",
    )


with DAG(
    PACKAGE_NAME,
    default_args=airflow_utils.get_default_args(
        {
            "owner": "Gary",
            "depends_on_past": False,
            "email": ["gqi@toronto.ca"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(seconds=600),
            "on_failure_callback": task_failure_slack_alert,
            "start_date": days_ago(1),
            "retries": 0,
            "etl_mapping":[{
                "source": ["https://contrib.wp.intra.prod-toronto.ca/app_content/tpp_measures", "https://contrib.wp.intra.prod-toronto.ca/app_content/tpp_narratives/"],
                "target_package_name": PACKAGE_NAME,
                "target_resource_name": RESOURCE_NAME
            }]
        }
    ),
    description="Take tpp json and narratives from progress portal",
    schedule_interval="0 22 * * 1-5",
    catchup=False,
    tags=["dataset"],
) as dag:

    def is_resource_new(**kwargs):
        package = kwargs["ti"].xcom_pull(task_ids="get_package")
        logging.info(f"resources found: {[r['name'] for r in package['resources']]}")
        is_new = RESOURCE_NAME not in [r["name"] for r in package["resources"]]

        if is_new:
            return "resource_is_new"

        return "resource_is_not_new"

    def transform_data(**kwargs):
        ti = kwargs["ti"]
        data_file_measure = ti.xcom_pull(task_ids="get_measure")
        data_file_narrative = ti.xcom_pull(task_ids="get_narrative")
        tmp_dir = ti.xcom_pull(task_ids="tmp_dir")

        with open(data_file_measure["data_path"]) as f:
            measure = json.load(f)
        logging.info(f"tmp_dir: {tmp_dir} | data_file_measure: {data_file_measure}")

        with open(data_file_narrative["data_path"]) as f:
            narrative = json.load(f)
        logging.info(f"tmp_dir: {tmp_dir} | data_file_narrative: {data_file_narrative}")

        df_measure = pd.DataFrame(make_measures_records(measure["measures"]))       # measure without target
        df_narrative = build_narratives_df(narrative)                               # narrative with measure id, year, period decoded

        # build target df
        targets=measure["targets"][0]
        df_target = pd.DataFrame()
        for k, v in targets.items():
            df = pd.DataFrame(v)
            df["measure_id"] = float(k)
            df_target = df_target.append (df.rename(columns={"v":"target", "p":"period_number_in_year", "y":"year"}))
        
        # join measure with target
        df_measure_target = pd.merge(df_measure,df_target, how='left', on=['measure_id', 'year', 'period_number_in_year'])
        df_measure_with_target = df_measure_target[df_measure_target['target'] == df_measure_target['target']][['measure_id', 'year', 'period_number_in_year','target']]
        df_measure_with_target['matched']=True
        # logging.info('target number:', len(df_target), '\nmacthed:', len(df_measure_with_target))

        # find targets without measures
        compare_df = pd.merge(df_target[['measure_id', 'year', 'period_number_in_year','target']], df_measure_with_target, how='left', on=['measure_id', 'year', 'period_number_in_year'])
        df_target_wo_measure = compare_df[compare_df['matched'] != True][['measure_id','year','period_number_in_year','target_x']].rename(columns={"target_x":"target"})
        df_measure_wo_vs = df_measure_target.drop(columns=['year','period_number_in_year','measure_value','target']).drop_duplicates(keep='last')
        df_measure_wo_vs['measure_value']=None
        df_target_wo_vs = pd.merge(df_target_wo_measure,df_measure_wo_vs, how='left', on=['measure_id'])

        # df with both measure and target, plus period with target but no measure is published yet. this is complete list
        df_m_t = pd.concat([df_measure_target, df_target_wo_vs[df_measure_target.columns]])

        # measure/target join with narrative
        df = pd.merge(df_m_t,df_narrative, how='left', on=['measure_id', 'year', 'period_number_in_year'])

        filepath = Path(tmp_dir) / "measure_target_narrative.parquet"

        df.to_parquet(path=filepath, engine="fastparquet", compression=None)

        return str(filepath)

    def get_fields(**kwargs):
        ti = kwargs["ti"]
        tmp_dir = ti.xcom_pull(task_ids="tmp_dir")
        filepath = Path(tmp_dir) / "fields.json"
        with open(filepath, 'w') as fields_json_file:
            json.dump(ds_fields(), fields_json_file)

        return str(filepath)

    def were_records_loaded(**kwargs):
        inserted_records_count = kwargs["ti"].xcom_pull(task_ids="insert_records")

        if inserted_records_count is not None and inserted_records_count > 0:
            return "new_records_notification"

        return "no_new_data_notification"

    def send_update_failed_notification(**kwargs):
        airflow_utils.message_slack(
            PACKAGE_NAME,
            "Update failed, data restored from backup",
            "error",
            Variable.get("active_env") == "prod",
            Variable.get("active_env"),
        )

    def send_new_records_notification(**kwargs):
        count = kwargs["ti"].xcom_pull("insert_records")

        airflow_utils.message_slack(
            PACKAGE_NAME,
            f"Refreshed {count} records",
            "success",
            Variable.get("active_env") == "prod",
            Variable.get("active_env"),
        )

    ckan_creds = Variable.get("ckan_credentials_secret", deserialize_json=True)
    active_env = Variable.get("active_env")
    ckan_address = ckan_creds[active_env]["address"]
    ckan_apikey = ckan_creds[active_env]["apikey"]

    tmp_dir = CreateLocalDirectoryOperator(
        task_id="tmp_dir", path=Variable.get("tmp_dir") + "/" + PACKAGE_NAME,
    )

    backups_dir = CreateLocalDirectoryOperator(
        task_id="backups_dir", path=Variable.get("backups_dir") + "/" + PACKAGE_NAME,
    )

    src1 = DownloadFileOperator(
        task_id="get_measure",
        file_url=tpp_measure_url,
        dir=Variable.get("tmp_dir") + "/" + PACKAGE_NAME,
        filename="measure.json",
    )
    src2 = DownloadFileOperator(
        task_id="get_narrative",
        file_url=tpp_narratives_url,
        dir=Variable.get("tmp_dir") + "/" + PACKAGE_NAME,
        filename="narrative.json",
    )

    package = GetPackageOperator(
        task_id="get_package",
        package_name_or_id=PACKAGE_NAME,
    )

    new_resource_branch = BranchPythonOperator(
        task_id="new_resource_branch", python_callable=is_resource_new,
    )

    transformed_data = PythonOperator(
        task_id="transform_data", python_callable=transform_data,
    )

    get_or_create_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_resource",
        package_name_or_id=PACKAGE_NAME,
        resource_name=RESOURCE_NAME,
        resource_attributes=dict(
            format="csv",
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {PACKAGE_NAME}",
            package_id=PACKAGE_NAME,
            url="placeholder",
        ),
    )

    backup_data = BackupDatastoreResourceOperator(
        task_id="backup_data",
        resource_task_id="get_or_create_resource",
        dir_task_id="backups_dir",
    )

    fields = PythonOperator(
        task_id="get_fields", python_callable=get_fields, trigger_rule="none_failed"
    )

    delete_tmp_data = PythonOperator(
        task_id="delete_tmp_data",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": PACKAGE_NAME, "recursively": True},
        trigger_rule="one_success",
    )

    sync_timestamp = ResourceAndFileOperator(
        task_id="sync_timestamp",
        download_file_task_id="get_measure",
        resource_task_id="get_or_create_resource",
        upload_to_ckan=False,
        sync_timestamp=True,
        trigger_rule="one_success",
    )

    delete_records = DeleteDatastoreResourceRecordsOperator(
        task_id="delete_records",
        backup_task_id="backup_data",
    )

    insert_records = InsertDatastoreResourceRecordsOperator(
        task_id="insert_records",
        fields_json_path_task_id="get_fields",
        parquet_filepath_task_id="transform_data",
        resource_task_id="get_or_create_resource",
    )

    new_records_notification = PythonOperator(
        task_id="new_records_notification",
        python_callable=send_new_records_notification,
    )

    update_failed_notification = PythonOperator(
        task_id="update_failed_notification",
        python_callable=send_update_failed_notification,
    )

    no_new_data_notification = PythonOperator(
        task_id="no_new_data_notification",
        python_callable=airflow_utils.message_slack,
        op_args=(
            PACKAGE_NAME,
            "Updated resource last_modified time only: new file but no new data",
            "success",
            active_env == "prod",
            active_env,
        ),
    )

    records_loaded_branch = BranchPythonOperator(
        task_id="were_records_loaded", python_callable=were_records_loaded,
    )

    restore_backup = RestoreDatastoreResourceBackupOperator(
        task_id="restore_backup",
        backup_task_id="backup_data",
        trigger_rule="all_failed",
    )

    tmp_dir >> src1 >> src2 >> transformed_data >> fields 

    package >> get_or_create_resource >>  new_resource_branch 

    new_resource_branch >> DummyOperator(
        task_id="resource_is_new"
    ) >>  fields >> insert_records >> sync_timestamp

    new_resource_branch >> DummyOperator(
        task_id="resource_is_not_new"
    ) >> backups_dir >> backup_data 
    
    [backup_data, transformed_data] >> delete_records >> fields >> insert_records >> sync_timestamp

    sync_timestamp >> records_loaded_branch

    records_loaded_branch >> new_records_notification

    records_loaded_branch >> no_new_data_notification

    [
        update_failed_notification,
        no_new_data_notification,
        new_records_notification,
    ] >> DummyOperator(task_id="Notification_sent", trigger_rule="one_success",) >>delete_tmp_data

    insert_records >> restore_backup >> update_failed_notification
