"""
    This DAG creates a yaml config file for CoT OD Airflow ETL from an OD Jira Ticket,
    enables automation from data owner requests intake to create data pipelines, finally
    publish dataset to open data portal (https://open.toronto.ca), more info can be found
    at airflow UI DAG Docs.

"""

import requests
import json
import logging
import yaml
import os
import pandas as pd
import io
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

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

# jira api call default return 50 results,
# manually expand to 100 to expand active jira ticket pool
JIRA_URL = (
    "https://toronto.atlassian.net/rest/api/3/search?jql=type=11468&maxResults=100"
)
JIRA_API_KEY = Variable.get("jira_apikey")

# headers for authenticate jira api calls
headers = {"Authorization": JIRA_API_KEY}

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
    "data_url": "customfield_12279",
    "collection_method": "customfield_12647",
}

YAML_METADATA = {  # DAG info
    "schedule": "@once",
    "dag_owner_name": "",  # dag owner name
    "dag_owner_email": "",  # dag owner email
}

PACKAGE_METADATA = [
    # mandatory package attributes
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
    "collection_method",
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
    # write some DAG-level documentation to be visible on the Airflow UI
    dag.doc_md = """
    ### Summary
    This DAG creates a yaml config file for CoT OD Airflow ETL from an OD Jira Ticket,
    enables automation from client requests intake to create data pipelines, finally
    publish dataset to open data portal (https://open.toronto.ca)

    ### Notes
    - Assign Jira ticket Id using Configuration JSON before triggering DAG
     e.g. {"jira_ticket_id": ["DTSD-3056"]}
     e.g. {"jira_ticket_id": ["DTSD-876", "DTSD-391"]}

    - Please note string need to be double quoted.

    - Please make sure AUTHENTICATED before sending Jira API calls.

    - For "Publish New Open Dataset Page" request, generate a yaml file based on
    Jira Ticket Input, if agol endpoint or data dictionary attachment provided,
    attributes section will also be generated.

    - For "Update Existing Open Dataset Page" request, generate a yaml based on
    existing package's metadata.

    - Please note currently dag can only generate attributes based on Data Dictionary
    Attachment, e.g."ATPF-v4.xlsx". And the sheet name has to be "Content"

    ### Contact
    jira-to-yaml automation inquiries should go to Yanan.Zhang@toronto.ca
    """

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
        return ticket_pool

    # Scrape attributes from GCC managed ArcGIS Online Endpoint, city arcgis only
    def scrape_content_from_arcgis(url):
        # AGOL and CKAN data type mapping
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
                            "info": {"notes": ""},
                        }
                    )

        attributes.append({"id": "geometry", "type": "text", "info": {"notes": ""}})
        return attributes

    def get_attributes_from_attachment(attachment_url):
        # Authentication needed to get attachment's content
        logging.info("Start pulling the attachements...")
        try:
            response = requests.get(attachment_url, headers=headers, timeout = 15)
            with io.BytesIO(response.content) as file:
                df = pd.io.excel.read_excel(file, sheet_name="Content")
        except requests.exceptions.Timeout:
            logging.error("Having trouble reaching the attachments")

        # Grab columns which will be used in yaml attributes section
        columns = [
            "Column Name in Source Data",
            "Desired Column Name on Open Data",
            "Data Type",
            "Business Description",
        ]

        # Read from attachment ATPF, user input start from row index 8
        df = df.loc[8:, columns].reset_index(drop=True)
        value_list = df.values.tolist()
        attributes = []

        # If source data column name and desired column name are both provided
        # We assign source_name and target_name in yaml
        if (str(value_list[0][0]) != "nan") and (str(value_list[0][1]) != "nan"):
            for row in value_list:
                attributes.append(
                    {
                        "source_name": row[0],
                        "target_name": row[1],
                        "type": row[2],
                        "info": {"notes": row[3]},
                    }
                )

        # If only source data column name or desired column name provided,
        # We assign id in yaml
        else:
            for row in value_list:
                attributes.append(
                    {
                        "id": row[0] if str(row[0]) != "nan" else row[1],
                        "type": row[2],
                        "info": {"notes": row[3]},
                    }
                )

        return attributes

    def grab_issue_content(issue):
        fields = issue["fields"]

        # location where data stored, could be an AGOL or NAS endpoint
        data_url = None
        if fields[mapping["data_url"]]:
            if len(fields[mapping["data_url"]]["content"]) > 0:
                data_url = fields[mapping["data_url"]]["content"][0]["content"][0][
                    "text"
                ]

        # check if dataset contains geographic coordinates
        geospatial = (
            True
            if fields[mapping["dataset_category"]]
            and fields[mapping["dataset_category"]]["value"] == "Yes"
            else False
        )

        attributes = []
        # if agol endpoint provided
        if data_url and geospatial:
            logging.info(data_url)
            attributes = scrape_content_from_arcgis(data_url)
            resource_content = {
                "format": "geojson",
                "agol": True,
                "url": data_url,
                "attributes": attributes,
            }
            resource = {fields["summary"]: resource_content}

        # if data column description (data dictionary) provided
        elif fields["attachment"]:
            attachment_url = fields["attachment"][0]["content"]
            attachment_name = fields["attachment"][0]["filename"]
            logging.info(f"Attachment name: {attachment_name}, url: {attachment_url}")

            try:
                logging.info("Start getting attributes from attachement.")
                attributes = get_attributes_from_attachment(attachment_url)
            except Exception as e:
                logging.error(e)
                logging.warning("Unexpected Attachment File Structure!")

            resource_content = {
                "format": "",
                "url": data_url,
                "attributes": attributes,
            }
            resource = {fields["summary"]: resource_content}

        else:
            resource = {}

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
            "dataset_category": "Map" if geospatial else "Table",
            "refresh_rate": fields[mapping["refresh_rate"]]["value"]
            if fields[mapping["refresh_rate"]]
            else None,
            "owner_division": fields[mapping["owner_division"]][0]["value"].strip()
            if fields[mapping["owner_division"]]
            else None,
            "owner_section": fields[mapping["owner_section"]].strip()
            if fields[mapping["owner_section"]]
            else None,
            "owner_unit": fields[mapping["owner_unit"]].strip()
            if fields[mapping["owner_unit"]]
            else None,
            "owner_email": fields[mapping["owner_email"]].strip()
            if fields[mapping["owner_email"]]
            else None,
            "civic_issues": [
                civic_issue["value"] for civic_issue in fields[mapping["civic_issues"]]
            ]
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
            "excerpt": fields[mapping["excerpt"]]["content"][0]["content"][0]["text"]
            if fields[mapping["excerpt"]]
            else None,
            "limitations": fields[mapping["limitations"]]["content"][0]["content"][0][
                "text"
            ]
            if fields[mapping["limitations"]]
            else None,
            "notes": fields["description"]["content"][0]["content"][0]["text"]
            if fields["description"]
            else None,
            "collection_method": fields[mapping["collection_method"]]["content"][0]["content"][0]["text"]
            if fields[mapping["collection_method"]]
            else None,
            "resources": resource,
        }
        package_id = fields["summary"].lower().replace("/", " ").replace(" ", "-")

        return {package_id: issue_metadata}

    def write_to_yaml(content, filename):
        """Receives a json input and writes it to a YAML file"""
        dir_path = Path(os.path.dirname(os.path.realpath(__file__))).parent
        yaml_dir_path = dir_path / "datasets" / "files_to_datastore"

        logging.info(f"Generating yaml file: {filename}")
        try:
            with open(yaml_dir_path / filename, "w") as file:
                yaml.dump(content, file, sort_keys=False)
        except PermissionError:
            message = "Note: yaml file already exist, please double check!"
            raise Exception(message)
        return {"filename": filename}

    def process_ticket(jira_issue_id):
        # Create a mapping between jira issue transition name and id
        jira_issue_transitions_mapping = {
            "In Progress": "11",
            "Waiting on Division": "21",
            "Closed": "31",
            "Waiting on Open Data": "51",
            "Published": "61",
            "Retired": "71",
            "Staged for Review": "81",
        }

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
        """Generate all yamls for "Publish New Open Dataset Page" Jira issues"""
        ti = kwargs.pop("ti")
        input_ticket_ids = ti.xcom_pull(task_ids="get_jira_ticket_id")["jira_ticket_id"]
        logging.info(f"Input Jira Ticket Ids: {input_ticket_ids}")

        # get jira issues and request type
        jira_issue_pool = ti.xcom_pull(task_ids="validate_jira_ticket")
        output_list = {}

        for jira_issue_id in jira_issue_pool:
            if jira_issue_id in input_ticket_ids:
                # get issue content
                url = "https://toronto.atlassian.net/rest/api/3/issue/" + jira_issue_id

                issue = json.loads(requests.get(url, headers=headers).content)
                request_type = issue["fields"]["customfield_10502"]["requestType"][
                    "name"
                ]
                logging.info(
                    f"Jira Ticket Id: {jira_issue_id} and Request Type: {request_type}"
                )

                if request_type == "Publish New Open Dataset Page":
                    issue_content = grab_issue_content(issue)
                    filename = "".join(list(issue_content.keys())) + ".yaml"
                    # write data to yaml file
                    write_to_yaml(issue_content, filename)
                    # process current ticket
                    process_ticket(issue["key"])
                    output_list[issue["key"]] = (
                        "Publish Dataset :done_green:" + filename
                    )
                if request_type == "Update Existing Open Dataset Page":
                    package_name = (
                        issue["fields"]["summary"].split("/dataset/")[1].strip("//")
                    )
                    logging.info(package_name)
                    ckan_url = CKAN + "api/3/action/package_show?id=" + package_name

                    # grab metadata
                    metadata = metadata_generator(
                        ckan_url, YAML_METADATA, PACKAGE_METADATA
                    )
                    filename = package_name + ".yaml"
                    # write data to yaml file
                    write_to_yaml(metadata, filename)
                    # process current ticket
                    process_ticket(issue["key"])
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

    slack_notification = GenericSlackOperator(
        task_id="slack_notification",
        message_header=(" Task Succeeded, Successfully Generated :yaml: !"),
        message_content_task_id="get_all_jira_issues",
        message_content_task_key="generated-yaml-list",
        message_body="",
    )

    (
        get_jira_ticket_id
        >> validate_jira_ticket
        >> get_all_jira_issues
        >> slack_notification
    )
