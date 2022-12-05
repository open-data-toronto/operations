'''Creates a yaml config for CoT OD Airflow ETL from an OD Jira Ticket
'''

import requests
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain

JIRA_APIKEY = Variable.get("jira_apikey")
JIRA_URL = "https://toronto.atlassian.net/rest/api/3/search?jql=type=11468"



def get_jira_issues():
    '''Returns all "Publish Dataset" Jira issues' important metadata'''
    headers = {'Authorization': 'Basic ' + JIRA_APIKEY}
    issues = json.loads(requests.get(JIRA_URL, headers=headers).text)

    output = []

    for issue in issues:
        output.append({
            "jira_issue_id": issue["key"],
            "jira_issue_url": issue["self"],
            "title":,
            #"date_published":,
            #"dataset_category":,
            "refresh_rate":,
            "owner_division":,
            "owner_section":,
            "owner_unit":,
            # TODO - mapping from jira api to CKAN package metadata
        })



