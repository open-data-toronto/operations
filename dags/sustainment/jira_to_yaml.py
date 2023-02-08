'''Creates a yaml config for CoT OD Airflow ETL from an OD Jira Ticket
   Assign Jira ticket Id using Configuration JSON before triggering DAG,
   e.g. {"jira_ticket_id": "DTSD-816"}
'''

import requests
import json
import logging
import yaml
import os
import re
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


JIRA_URL = "https://toronto.atlassian.net/rest/api/3/search?jql=type=11468"
DIR_PATH = Path(os.path.dirname(os.path.realpath(__file__))).parent
YAML_DIR_PATH = DIR_PATH / "datasets" / "files_to_datastore"


# init DAG
default_args = airflow_utils.get_default_args({
    "owner": "Yanan",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": 3,
    "on_failure_callback": task_failure_slack_alert,
    "start_date": datetime(2023, 2, 6, 0, 0, 0),
    "tags": ["sustainment"],
})

with DAG(
    "jira_to_yaml",
    default_args = default_args,
    schedule_interval = "@once",
    catchup = False, 
) as dag:
    
    def get_jira_ticket_id(**kwargs):
    
    if "jira_ticket_id" in kwargs['dag_run'].conf.keys():
        jira_ticket_id = kwargs['dag_run'].conf["jira_ticket_id"]
        
        assert isinstance(jira_ticket_id, str), "Input 'jira_ticket_id' object needs to be a string of the jira ticket you want to run"

    return {"jira_ticket_id": jira_ticket_id}

    def get_jira_issues(**kwargs):
        '''Returns all "Publish Dataset" Jira issues' important metadata'''
        
        jira_ticket_id = kwargs.pop("ti").xcom_pull(task_ids="get_jira_ticket_id")["jira_ticket_id"]
        logging.info(f"---Jira Ticket Id----{jira_ticket_id}-----------")
        
        headers = {
            }

        # get all jira issues
        issues = json.loads(requests.get(JIRA_URL, headers=headers).content)["issues"]
        
        # get current jira issue
        current_issue = [issue for issue in issues if issue["key"] == jira_ticket_id][0]
        logging.info(current_issue)
        
        fields = current_issue["fields"]
        output = []

        # assign relevant metadata from jira to a dict
        issue_metadata = {
            "jira_issue_id": current_issue["key"],
            "jira_issue_url": current_issue["self"],
            "schedule": "@once",
            "dag_owner_name": fields["assignee"]["displayName"],
            "dag_owner_email": fields["assignee"]["emailAddress"],
            "title": fields["summary"],
            "date_published": None,#datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "dataset_category": None,
            "refresh_rate": fields["customfield_12166"][-1]["value"],
            "dataset_category": "Table",
            "owner_division": fields["customfield_11827"][-1]["value"],
            "owner_section": fields["customfield_11958"],
            "owner_unit": fields["customfield_11959"],
            "owner_email": fields["customfield_12243"],
            "civic_issues": None,
            "topics": [topic["value"] 
                    for topic in fields["customfield_12246"]],
            "tags": [{"name": item, "vocabulary_id": None} 
                    for item in fields["customfield_12249"]]
                    if fields["customfield_12249"] else None,
            "information_url": fields["customfield_11861"],
            "excerpt": fields["customfield_12244"]["content"][-1]["content"][-1]["text"],
            "limitations": fields["customfield_12245"],
            "notes": fields["description"]["content"][-1]["content"][-1]["text"],
            "resources": {},
        }
        print(issue_metadata)
        output.append(issue_metadata)
        
        return output

    def write_to_yaml(**kwargs):
        '''Receives a json input and writes it to a YAML file'''
        issues = kwargs["ti"].xcom_pull(task_ids=["get_jira_issues"])[0]
        print(issues)
        for issue in issues:
            print(issue)
            package_id = issue["title"].lower().replace(" ", "-")
            filename = package_id + ".yaml"
            with open(YAML_DIR_PATH / filename, "w") as file:
                yaml.dump({package_id: issue}, file, sort_keys=False)
    
    get_jira_ticket_id = PythonOperator(
        task_id = "get_jira_ticket_id",
        python_callable = get_jira_ticket_id,
        provide_context = True
    )

    get_jira_issues = PythonOperator(
        task_id = "get_jira_issues", 
        python_callable = get_jira_issues,
    )

    # write_to_yaml = PythonOperator(
    #     task_id = "write_to_yaml", 
    #     python_callable = write_to_yaml,
    # )

    get_jira_ticket_id >> get_jira_issues #>> write_to_yaml

