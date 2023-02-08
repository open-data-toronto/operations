'''Creates a yaml config for CoT OD Airflow ETL from an OD Jira Ticket
   Assign Jira ticket Id using Configuration JSON before triggering DAG,
   e.g. {"jira_ticket_id": "DTSD-876"}
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

# Create a mapping between yaml fields and jira fields
mapping = {
    
    "refresh_rate": "customfield_12251",
    "owner_division": "customfield_11827",
    "owner_section": "customfield_11958",
    "owner_unit": "customfield_11959",
    "owner_email": "customfield_12243",
    "civic_issues": "customfield_12253",
    "topics": "customfield_12246",
    "tags": "customfield_12249",
    "information_url": "customfield_11861",
    "excerpt": "customfield_12244",
    "limitations": "customfield_12252"
}

# init DAG
default_args = airflow_utils.get_default_args({
    "owner": "Yanan",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
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

        # get all jira issues is and request type
        issues = json.loads(requests.get(JIRA_URL, headers=headers).content)["issues"]
        issue_ids = []
        for issue in issues:
            request_type = issue["fields"]["customfield_10502"]["requestType"]["name"]
            issue_ids.append({issue["key"] : request_type})
        logging.info(f"All Jira Ticket Id and Request Type: {issue_ids}")
        
        # get current jira issue
        current_issue = [issue for issue in issues if issue["key"] == jira_ticket_id][0]
        logging.info(current_issue)
        
        fields = current_issue["fields"]
        output = []
        
        # check request type
        request_type = fields["customfield_10502"]["requestType"]["name"]
        if request_type == "Publish Dataset":
            logging.info("This is a request for publishing dataset, generating yaml file!")
            
            # assign relevant metadata from jira to a dict
            issue_metadata = {
                "jira_issue_id": current_issue["key"],
                "jira_issue_url": current_issue["self"],
                
                "schedule": "@once",
                "dag_owner_name": fields["assignee"]["displayName"] if fields["assignee"] else None,
                "dag_owner_email": fields["assignee"]["emailAddress"] if fields["assignee"] else None,
                "title": fields["summary"],
                "date_published": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "dataset_category": None,
                
                "refresh_rate": fields[mapping["refresh_rate"]]["value"] if fields[mapping["refresh_rate"]] else None,
                "owner_division": fields[mapping["owner_division"]][0]["value"] if fields[mapping["owner_division"]] else None,
                "owner_section": fields[mapping["owner_section"]] if fields[mapping["owner_section"]] else None,
                "owner_unit": fields[mapping["owner_unit"]] if fields[mapping["owner_unit"]] else None,
                "owner_email": fields[mapping["owner_email"]] if fields[mapping["owner_email"]] else None,
                "civic_issues": fields[mapping["civic_issues"]][0]["value"] if fields[mapping["civic_issues"]] else None,
                "topics": [topic["value"] 
                        for topic in fields[mapping["topics"]]] if fields[mapping["topics"]] else None,
                "tags": [{"name": item, "vocabulary_id": None} 
                        for item in fields[mapping["tags"]]]
                        if fields[mapping["tags"]] else None,
                "information_url": fields[mapping["information_url"]] if fields[mapping["information_url"]] else None,
                "excerpt": fields[mapping["excerpt"]]["content"][0]["content"][0]["text"] if fields[mapping["excerpt"]] else None,
                "limitations": fields[mapping["limitations"]]["content"][0]["content"][0]["text"] if fields[mapping["limitations"]] else None,
                "notes": fields["description"]["content"][0]["content"][0]["text"] if fields["description"] else None,
                "resources": {},
            }
            print(issue_metadata)
            output.append(issue_metadata)
        else:
            logging.info("This is a request for updating exisitng dataset.")
        
        return output

    def write_to_yaml(**kwargs):
        '''Receives a json input and writes it to a YAML file'''
        issues = kwargs["ti"].xcom_pull(task_ids=["get_jira_issues"])[0]
        for issue in issues:
            package_id = issue["title"].lower().replace(" ", "-")
            filename = package_id + "-test.yaml"
            logging.info(f"Generating yaml file: {filename}")
            with open(YAML_DIR_PATH / filename, "w") as file:
                yaml.dump({package_id: issue}, file, sort_keys=False)
        return filename
    
    get_jira_ticket_id = PythonOperator(
        task_id = "get_jira_ticket_id",
        python_callable = get_jira_ticket_id,
        provide_context = True
    )

    get_jira_issues = PythonOperator(
        task_id = "get_jira_issues", 
        python_callable = get_jira_issues,
    )

    write_to_yaml = PythonOperator(
        task_id = "write_to_yaml", 
        python_callable = write_to_yaml,
    )

    get_jira_ticket_id >> get_jira_issues >> write_to_yaml

