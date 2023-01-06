import ckanapi
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from utils import airflow_utils, ckan_utils
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator
from ckan_operators.package_operator import GetOrCreatePackageOperator
from ckan_operators.resource_operator import GetOrCreateResourceOperator

import requests
import pandas as pd



base_url = "https://ckanadmin0.intra.prod-toronto.ca/api/3/action/"

# create package and resource link mapping
def create_mapping(base_url):
    url = base_url + "package_list"
    response = requests.get(url)
    package_list = response.json()["result"]
    mapping = dict()
    for pkg_name in package_list:
        print("processing package: " + pkg_name)
        url = base_url + "package_show?id=" + pkg_name
        response = requests.get(url).json()["result"]
        package_name = response["name"]
        package_id = response["id"]
        mapping[package_id] = package_name

# Generate summarization report
def generate_summary_report(url):
    
    # grab statistics from oracle infinity througth API
    call = "https://api.oracleinfinity.io/v1/account/97j62divdr/dataexport/c589ej49l7/data?begin=2022/12/1/0&end=2022/12/31/23&format=csv&timezone=America/New_York&suppressErrorCodes=true&autoDownload=true&download=false&totals=true&limit=250"
    user = 
    password = 

    response = requests.get(call, auth=(user, password))
    status = response.status_code
    content = response.text

    result = []
    for item in content.split("\n")[2:]:
        print(item)
        row = [i.strip('\"') for i in item.split('\",')]
        result.append(row)

    names = [name.strip('\"') for name in content.split("\n")[0].split(",")]

    df = pd.DataFrame(result, columns=names)
    df["Package_Id"] = df["Link Tracking -URL Clicked"].apply(lambda x: x.split("/dataset/")[1].split("/resource/")[0])
    df["Package_Name"] = df["Package_Id"].apply(lambda x: mapping[x])
    df["Clicks"] = df["Clicks"].astype(int)
    df.to_csv("Raw_Stats.csv", index=False)
    
    