'''Creates a yaml config for CoT OD Airflow ETL from an OD Jira Ticket
'''

import requests
import json
import logging
import yaml
import os
import re
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

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
DIR_PATH = Path(os.path.dirname(os.path.realpath(__file__))).parent
YAML_DIR_PATH = DIR_PATH / "datasets" / "files_to_datastore"

def scrape_resource_content_from_agol(url):
    AGOL_CKAN_TYPE_MAP = {
        "sqlTypeFloat": "float",
        "sqlTypeNVarchar": "text",
        "sqlTypeInteger": "int",
        "sqlTypeOther": "text",
        "sqlTypeTimestamp2": "timestamp",
        "sqlTypeDouble": "float",
        "esriFieldTypeString": "text",
        "esriFieldTypeDate": "timestamp",
        "esriFieldTypeInteger": "int",
        "esriFieldTypeOID": "int",
        "esriFieldTypeDouble": "float",
        "esriFieldTypeSmallInteger": "int",
        "esriFieldTypeSingle": "text",
        "esriFieldTypeGlobalID": "int",
    }

    DELETE_FIELDS = [
        "longitude",
        "latitude",
        "shape__length",
        "shape__area",
        "lat",
        "long",
        "lon",
        "x",
        "y",
        "index_",
    ]

    page = requests.get(url)

    # parse html content
    soup = BeautifulSoup(page.content, "html.parser")
    fields = list(soup.find_all("ul")[4])

    # collect attributes
    attributes = []
    for field in fields:
        if field != "\n":
            field_content = field.get_text()
            id = field_content.split(" ")[0]
            type = field_content.split("type: ")[1].split(",")[0]
            # remove redudent geometry fields
            if id.lower() not in DELETE_FIELDS:
                attributes.append(
                    {
                        "id": id, 
                        "type": AGOL_CKAN_TYPE_MAP[type], 
                        "info": {"notes": ""}
                    }
                )

    attributes.append({"id": "geometry", "type": "text", "info": {"notes": ""}})
    return attributes

def get_jira_issues():
    '''Returns all "Publish Dataset" Jira issues' important metadata'''
    headers = {'Authorization': 'Basic ' + JIRA_APIKEY}
    issues = json.loads(requests.get(JIRA_URL, headers=headers).text)["issues"]

    # init output of this operator
    output = []

    # grab relevant metadata from Jira issue
    for issue in issues:
        fields = issue["fields"]

        # Ignore closed tickets
        if fields["status"]["name"] == "Closed":
            continue

        # assign relevant metadata from jira to a dict
        issue_metadata = {
            "jira_issue_id": issue["key"],
            "jira_issue_url": issue["self"],
            "schedule": "@once",
            "dag_owner_name": fields["assignee"]["displayName"],
            "dag_owner_email": fields["assignee"]["emailAddress"],
            "title": fields["summary"],
            "date_published": None,#datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "dataset_category": None,
            "refresh_rate": fields["customfield_12166"][-1]["value"],
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

        # If the issue has an AGOL link in it, extract its attributes
        if ("https://services3.arcgis.com/b9WvedVPoizGfvfD/arcgis/rest/services/" 
        in fields["customfield_12234"]):
            pattern = r"(https://services3.arcgis.com/b9WvedVPoizGfvfD/arcgis/rest/services/.*/FeatureServer/0)"
            agol_url = re.search(pattern, fields["customfield_12234"])[0]
            print(agol_url)
            attributes = scrape_resource_content_from_agol(agol_url)
            print(attributes)
            # also add relevant configs for the yaml jobs to know what to parse
            issue_metadata["resources"][fields["summary"]] = {
                "format": "geojson",
                "agol": True,
                "url": agol_url,
                "attributes": attributes,
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

