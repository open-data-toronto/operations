import ckanapi
import requests
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from ckan_operators.package_operator import GetCkanPackageListOperator
from utils import airflow_utils, ckan_utils
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator

import requests
import pandas as pd

# init hardcoded vars for these dags
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]


address = "https://ckanadmin0.intra.prod-toronto.ca/api/3/action/"
call = "https://api.oracleinfinity.io/v1/account/97j62divdr/dataexport/c589ej49l7/data?begin=2022/11/1/0&end=2022/11/30/23&format=csv&timezone=America/New_York&suppressErrorCodes=true&autoDownload=true&download=false&totals=true&limit=250"
user = ""
password = ""


with DAG(
    'portal-analytics',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
                "owner": "Yanan",
                "depends_on_past": False,
                "email": "yanan.zhang@toronto.ca",
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": 3,
                "on_failure_callback": task_failure_slack_alert,
                "tags": ["sustainment"]
            },
    description='Report top 15 popular datasets on Open Data Portal',
    schedule_interval="@once",
    start_date=datetime(2023, 1, 6, 0, 0, 0),
    catchup = False,
) as dag:
    
    # create package and resource link mapping
    def create_mapping(**kwargs):
        url = address + "package_list"
        response = requests.get(url)
        package_list = response.json()["result"]
        
        #package_list = kwargs.pop("ti").xcom_pull(task_ids="get_packages")
        logging.info(f"--------{package_list}-----")
        mapping = dict()
        for pkg_name in package_list:
            url = address + "package_show?id=" + pkg_name
            response = requests.get(url).json()["result"]
            package_name = response["name"]
            package_id = response["id"]
            mapping[package_id] = package_name
        
        logging.info("---------Mapping Created--------")
        return mapping

    # Generate summarization report
    def generate_summary_report(**kwargs):
        
        mapping = kwargs.pop("ti").xcom_pull(task_ids="get_mapping")
        
        # grab statistics from oracle infinity througth API
        response = requests.get(call, auth=(user, password))
        status = response.status_code
        content = response.text
        
        # parse and construct report content
        names = [name.strip('\"') for name in content.split("\n")[0].split(",")]
        logging.info(f"----GENERATING REPORT--COLUMNS----{names}------")
        
        result = []
        for item in content.split("\n")[2:]:
            row = [i.strip('\"') for i in item.split('\",')]
            logging.info(f"--{row}--")
            
            # only calculate downloads hosted on ckan0.cf.opendata
            if row[0].startswith("ckan0.cf.opendata"):
                result.append(row)

        df = pd.DataFrame(result, columns=names)
        df["Package_Id"] = df["Link Tracking -URL Clicked"].apply(lambda x: x.split("/dataset/")[1].split("/resource/")[0])
        df["Dataset_Name"] = df["Package_Id"].apply(lambda x: mapping[x] if x in mapping.keys() else x)
        df["Download_Clicks"] = df["Clicks"].astype(int)
        
        stats_df = df.groupby('Dataset_Name', as_index = False)['Download_Clicks'].sum().sort_values(by="Download_Clicks", ascending=False)
        stats_df = stats_df.reset_index(drop=True).head(30)
        
        result = dict(zip(stats_df["Dataset_Name"], stats_df["Download_Clicks"].astype(str)))
        logging.info(result)
        
        return {"report": result}
        
    
    # get_packages = GetCkanPackageListOperator(
    #     task_id = "get_packages", 
    #     address=CKAN, 
    #     apikey=CKAN_APIKEY,
    # )

    get_mapping = PythonOperator(
        task_id = "get_mapping", 
        provide_context=True,
        python_callable=create_mapping,
        op_kwargs={"address": CKAN, "apikey": CKAN_APIKEY},
    )
 
    portal_summary_report = PythonOperator(
        task_id="portal_summary_report", 
        provide_context=True,
        python_callable=generate_summary_report,
        op_kwargs={"call": call, "user": user, "password": password},
    )

    slack_notificaiton = GenericSlackOperator(
            task_id = "slack_notificaiton",
            message_header = "Portal Analytics - Monthy Popular Datasets",
            message_content_task_id = "portal_summary_report",
            message_content_task_key = "report",
            message_body = "\n TOP Datasets this month!"
        )
   
    #get_packages >> get_mapping >> portal_summary_report >> slack_notificaiton
    get_mapping >> portal_summary_report >> slack_notificaiton
