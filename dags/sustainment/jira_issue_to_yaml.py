'''Creates a yaml config for CoT OD Airflow ETL from an OD Jira Ticket
'''

import requests
import json
import logging
import yaml
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain

from utils import airflow_utils
from utils_operators.slack_operators import task_success_slack_alert 
from utils_operators.slack_operators import task_failure_slack_alert 
from utils_operators.slack_operators import GenericSlackOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


JIRA_APIKEY = Variable.get("jira_apikey")
JIRA_URL = "https://toronto.atlassian.net/rest/api/3/search?jql=type=11468"
DIR_PATH = os.path.dirname(os.path.realpath(__file__))

def get_jira_issues():
    '''Returns all "Publish Dataset" Jira issues' important metadata'''
    headers = {'Authorization': 'Basic ' + JIRA_APIKEY}
    issues = json.loads(requests.get(JIRA_URL, headers=headers).text)["issues"]

    # init output of this operator
    output = []

    # grab relevant metadata from Jira issue
    for issue in issues:
        fields = issue["fields"]
        output.append({
            "jira_issue_id": issue["key"],
            "jira_issue_url": issue["self"],
            "title": fields["summary"],
            #"date_published":,
            #"dataset_category":,
            "refresh_rate": fields["customfield_12166"][-1]["value"],
            "owner_division": fields["customfield_11827"][-1]["value"],
            "owner_section": fields["customfield_11958"],
            "owner_unit": fields["customfield_11959"],
            "owner_email": fields["customfield_12243"],
            "civic_issues": "",
            "topics": [topic["value"] 
                    for topic in fields["customfield_12246"]],
            "tags": [{"name": item, "vocabulary_id": None} 
                    for item in fields["customfield_12249"]]
                    if fields["customfield_12249"] else None,
            "information_url": fields["customfield_11861"],
            "excerpt": fields["customfield_12244"]["content"][-1]["content"][-1]["text"],
            "limitations": fields["customfield_12245"],
            "notes": fields["description"]["content"][-1]["content"][-1]["text"],
        })

    return output

def write_to_yaml(**kwargs):
    '''Receives a json input and writes it to a YAML file'''
    issues = kwargs["ti"].xcom_pull(task_ids=["get_jira_issues"])

    for issue in issues:
        package_id = issue["title"].lower().replace(" ", "")
        with open(DIR_PATH + "/" + package_id + ".yaml", "w") as file:
            yaml.dump({package_id: issue}, file)



# init DAG
default_args = airflow_utils.get_default_args({
    "owner": "Mackenzie",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": 3,
    "on_failure_callback": task_failure_slack_alert,
    "start_date": datetime(2022, 8, 8, 0, 0, 0),
    "tags": ["sustainment"],
})

with DAG(
    "jira_issues_to_yaml",
    default_args = default_args,
    schedule_interval = "@once",
    catchup = False, 
) as dag:

    get_jira_issues = PythonOperator(
        task_id = "get_jira_issues", 
        python_callable = get_jira_issues,
    )

    write_to_yaml = PythonOperator(
        task_id = "write_to_yaml", 
        python_callable = write_to_yaml,
    )

    get_jira_issues >> write_to_yaml

