"""
    Creates a yaml config for CoT OD Airflow ETL from an OD Jira Ticket
    Assign Jira ticket Id using Configuration JSON before triggering DAG,

    e.g. {"jira_ticket_id": ["DTSD-876", "DTSD-391"]},

    please note string need to be double quoted.

    Please make sure AUTHENTICATED before sending Jira API calls.
"""

import requests
import json
import logging
import yaml
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Variable

from utils import airflow_utils
from utils_operators.slack_operators import (
    GenericSlackOperator,
    task_failure_slack_alert,
)
from airflow.operators.python import PythonOperator

CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)

# we need ckan prod address to get a full list of packages
CKAN = CKAN_CREDS["prod"]["address"]

JIRA_URL = "https://toronto.atlassian.net/rest/api/3/search?jql=type=11468"
JIRA_API_KEY = Variable.get("jira_apikey")
DIR_PATH = Path(os.path.dirname(os.path.realpath(__file__))).parent
YAML_DIR_PATH = DIR_PATH / "datasets" / "files_to_datastore"

# headers for authenticate jira api calls
headers = {"Authorization": JIRA_API_KEY}

# Create a mapping between jira issue transition name and id
jira_issue_transitions_mapping = {
    "In Progress": "11",
    "Waiting on Division": "21",
    "Closed": "31",
    "Waiting on Open Data": "51",
}

# Create a mapping between yaml fields and jira fields
mapping = {
    "refresh_rate": "customfield_12251",
    "dataset_category": "customfield_12248",
    "owner_division": "customfield_11827",
    "owner_section": "customfield_11958",
    "owner_unit": "customfield_11959",
    "owner_email": "customfield_12243",
    "civic_issues": "customfield_12253",
    "topics": "customfield_12246",
    "tags": "customfield_12249",
    "information_url": "customfield_11861",
    "excerpt": "customfield_12244",
    "limitations": "customfield_12252",
}

YAML_METADATA = {  # DAG info
    "schedule": "@once",
    "dag_owner_name": "",  # dag owner name
    "dag_owner_email": "",  # dag owner email
}

PACKAGE_METADATA = [  # mandatory package attributes
    "title",
    "date_published",
    "refresh_rate",
    "dataset_category",
    # optional package attributes
    "owner_division",
    "owner_section",
    "owner_unit",
    "owner_email",
    "civic_issues",
    "topics",
    "tags",
    "information_url",
    "excerpt",
    "limitations",
    "notes",
]

# init DAG
default_args = airflow_utils.get_default_args(
    {
        "owner": "Yanan",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": 3,
        "on_failure_callback": task_failure_slack_alert,
        "start_date": datetime(2023, 2, 6, 0, 0, 0),
        "tags": ["sustainment"],
    }
)

with DAG(
    "jira_to_yaml",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:

    def get_jira_ticket_id(**kwargs):
        # get jira ticket ids from airflow dag configuration
        if "jira_ticket_id" in kwargs["dag_run"].conf.keys():
            jira_ticket_id = kwargs["dag_run"].conf["jira_ticket_id"]

            assert isinstance(jira_ticket_id, list), (
                "Input 'jira_ticket_ids' object needs to be a list of "
                + "jira tickets you want to run"
            )

        return {"jira_ticket_id": jira_ticket_id}

    def validate_jira_ticket(**kwargs):
        # make sure input jira ticket id exists
        input_ticket_ids = kwargs.pop("ti").xcom_pull(task_ids="get_jira_ticket_id")[
            "jira_ticket_id"
        ]

        issues = json.loads(requests.get(JIRA_URL, headers=headers).content)["issues"]
        ticket_pool = [issue["key"] for issue in issues]
        logging.info(f"Jira Ticket Pool: {json.dumps(ticket_pool)}")

        for ticket in input_ticket_ids:
            if ticket not in ticket_pool:
                message = f"Jira Ticket {ticket} is not valid. Please double check."
                raise Exception(message)

    def grab_issue_content(issue):

        fields = issue["fields"]

        # check request type
        request_type = fields["customfield_10502"]["requestType"]["name"]
        if request_type == "Publish Dataset":
            logging.info(
                "This is a request for publishing dataset, generating yaml file!"
            )

            # assign relevant metadata from jira to a dict
            issue_metadata = {
                "jira_issue_id": issue["key"],
                "jira_issue_url": issue["self"],
                "schedule": "@once",
                "dag_owner_name": fields["assignee"]["displayName"]
                if fields["assignee"]
                else None,
                "dag_owner_email": fields["assignee"]["emailAddress"]
                if fields["assignee"]
                else None,
                "title": fields["summary"],
                "date_published": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "dataset_category": "Map"
                if fields[mapping["dataset_category"]]
                and fields[mapping["dataset_category"]]["value"] == "Yes"
                else None,
                "refresh_rate": fields[mapping["refresh_rate"]]["value"]
                if fields[mapping["refresh_rate"]]
                else None,
                "owner_division": fields[mapping["owner_division"]][0]["value"]
                if fields[mapping["owner_division"]]
                else None,
                "owner_section": fields[mapping["owner_section"]]
                if fields[mapping["owner_section"]]
                else None,
                "owner_unit": fields[mapping["owner_unit"]]
                if fields[mapping["owner_unit"]]
                else None,
                "owner_email": fields[mapping["owner_email"]]
                if fields[mapping["owner_email"]]
                else None,
                "civic_issues": fields[mapping["civic_issues"]][0]["value"]
                if fields[mapping["civic_issues"]]
                else None,
                "topics": [topic["value"] for topic in fields[mapping["topics"]]]
                if fields[mapping["topics"]]
                else None,
                "tags": [
                    {"name": item, "vocabulary_id": None}
                    for item in fields[mapping["tags"]]
                ]
                if fields[mapping["tags"]]
                else None,
                "information_url": fields[mapping["information_url"]]
                if fields[mapping["information_url"]]
                else None,
                "excerpt": fields[mapping["excerpt"]]["content"][0]["content"][0][
                    "text"
                ]
                if fields[mapping["excerpt"]]
                else None,
                "limitations": fields[mapping["limitations"]]["content"][0]["content"][
                    0
                ]["text"]
                if fields[mapping["limitations"]]
                and (fields[mapping["limitations"]]["content"])
                else None,
                "notes": fields["description"]["content"][0]["content"][0]["text"]
                if fields["description"]
                else None,
                "resources": {},
            }
            print(issue_metadata)
            package_id = fields["summary"].lower().replace("/", " ").replace(" ", "-")

            return {package_id: issue_metadata}

    def write_to_yaml(content, filename):
        """Receives a json input and writes it to a YAML file"""

        logging.info(f"Generating yaml file: {filename}")
        try:
            with open(YAML_DIR_PATH / filename, "w") as file:
                yaml.dump(content, file, sort_keys=False)
        except PermissionError:
            message = "Note: yaml file already exist, please double check!"
            raise Exception(message)
        return {"filename": filename}

    def process_ticket(jira_issue_id):
        url = (
            "https://toronto.atlassian.net/rest/api/3/issue/"
            + jira_issue_id
            + "/transitions"
        )
        payload = json.dumps(
            {"transition": {"id": jira_issue_transitions_mapping["In Progress"]}}
        )

        headers = {"Authorization": JIRA_API_KEY, "Content-Type": "application/json"}

        response = requests.request("POST", url, headers=headers, data=payload)
        logging.info(f"Status: {response.status_code}, Ticket has been processed.")

    # grab metadata content
    def metadata_generator(ckan_url, yaml_metadata, package_metadata):
        response = requests.get(ckan_url).json()
        ckan_metadata_content = response["result"]

        ckan_metadata_fields = ckan_metadata_content.keys()

        for field in package_metadata:
            if field in ckan_metadata_fields:
                if field == "tags":
                    tags_content = ckan_metadata_content["tags"]
                    tags_yaml = []
                    for i in range(len(tags_content)):
                        tag = {}
                        tag["name"] = tags_content[i]["display_name"]
                        tag["vocabulary_id"] = None
                        tags_yaml.append(tag)
                    yaml_metadata[field] = tags_yaml
                else:
                    yaml_metadata[field] = ckan_metadata_content[field]
            else:
                yaml_metadata[field] = None
        metadata = {ckan_metadata_content["name"]: yaml_metadata}
        return metadata

    def get_all_jira_issues(**kwargs):
        """Generate all yamls for "Publish Dataset" Jira issues"""

        input_ticket_ids = kwargs.pop("ti").xcom_pull(task_ids="get_jira_ticket_id")[
            "jira_ticket_id"
        ]
        logging.info(f"Jira Ticket Ids: {input_ticket_ids}")

        # get all jira issues is and request type
        issues = json.loads(requests.get(JIRA_URL, headers=headers).content)["issues"]
        output_list = {}

        for issue in issues:
            if issue["key"] in input_ticket_ids:
                request_type = issue["fields"]["customfield_10502"]["requestType"][
                    "name"
                ]
                logging.info(
                    f"Jira Ticket Id: {issue['key']} and Request Type: {request_type}"
                )

                if request_type == "Publish Dataset":
                    issue_content = grab_issue_content(issue)
                    filename = "".join(list(issue_content.keys())) + ".yaml"
                    # write data to yaml file
                    write_to_yaml(issue_content, filename)
                    # process current ticket
                    process_ticket(issue["key"])
                    output_list[issue["key"]] = (
                        "Publish Dataset :done_green:" + filename
                    )
                if request_type == "Update Dataset":
                    package_name = (
                        issue["fields"]["summary"].split("/dataset/")[1].strip("//")
                    )
                    ckan_url = CKAN + "api/3/action/package_show?id=" + package_name
                    logging.info(ckan_url)

                    # grab metadata
                    metadata = metadata_generator(
                        ckan_url, YAML_METADATA, PACKAGE_METADATA
                    )
                    filename = package_name + ".yaml"
                    write_to_yaml(metadata, filename)
                    output_list[issue["key"]] = "Update Dataset :done_green:" + filename
            else:
                continue

        return {"generated-yaml-list": output_list}

    get_jira_ticket_id = PythonOperator(
        task_id="get_jira_ticket_id",
        python_callable=get_jira_ticket_id,
        provide_context=True,
    )

    validate_jira_ticket = PythonOperator(
        task_id="validate_jira_ticket",
        python_callable=validate_jira_ticket,
        provide_context=True,
    )

    get_all_jira_issues = PythonOperator(
        task_id="get_all_jira_issues",
        python_callable=get_all_jira_issues,
        provide_context=True,
    )

    slack_notificaiton = GenericSlackOperator(
        task_id="slack_notificaiton",
        message_header=(" Task Succeeded, Succefully Generated :yaml: !"),
        message_content_task_id="get_all_jira_issues",
        message_content_task_key="generated-yaml-list",
        message_body="",
    )

    (
        get_jira_ticket_id
        >> validate_jira_ticket
        >> get_all_jira_issues
        >> slack_notificaiton
    )
