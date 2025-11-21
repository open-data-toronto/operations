"""
    This DAG creates a yaml config file for CoT OD Airflow ETL from an OD Jira Ticket,
    enables automation from data owner requests intake to create data pipelines, finally
    publish dataset to open data portal (https://open.toronto.ca), more info can be found
    at airflow UI DAG Docs.

"""

import requests
import urllib.parse
import json
import logging
import yaml
import os
import io
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

from airflow.decorators import dag, task
from airflow.models import Variable

CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)

# we need ckan prod address to get a full list of packages
CKAN = CKAN_CREDS["prod"]["address"]

# jira api call default return 50 results,
# manually expand to 100 to expand active jira ticket pool
JIRA_URL = "https://toronto.atlassian.net/rest/api/3/issue/"

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
    "limitations": "customfield_12252", # TODO - FORMAT TEXT HERE
    "data_url": "customfield_12279",    # TODO - ERROR HANDLING
    "collection_method": "customfield_12647", #TODO - FORMAT TEXT HERE
}

DIR_PATH = Path(os.path.dirname(os.path.realpath(__file__))).parent

YAML_DIR_PATH = DIR_PATH / "datasets" / "files_to_datastore"

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


@dag(
    schedule="35 20 * * 2",
    catchup=False,
    start_date=datetime(2025, 2, 22, 0, 0, 0),

)
def jira_to_yaml():
    # write some DAG-level documentation to be visible on the Airflow UI
    # TODO - fix this summary of how the DAG works
    """
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

    - For "Publish New" requests, generate a yaml file based on
    Jira Ticket Input, if agol endpoint or data dictionary attachment provided,
    attributes section will also be generated.

    - For "Update" requesta, generate a yaml based on
    existing package's metadata.

    - Please note currently dag can only generate attributes based on Data Dictionary
    Attachment, e.g."ATPF-v4.xlsx". And the sheet name has to be "Content"

    """
    @task
    def get_jira_issues():
        # get jira ticket ids from active jira tickets
        output = []
        
        # customer request types taken from airflow variables
        raw_request_types = Variable.get("jira_intake_customer_request_types")
        jira_url = f'https://toronto.atlassian.net/rest/api/3/search/jql?jql=%22Customer%20Request%20Type%22%20in%20({urllib.parse.quote(raw_request_types)})%20AND%20status%20%3D%20"In%20Development"%20&fields=*all&expand=names'

        resp = requests.get(jira_url, headers=headers)

        assert resp.status_code == 200, f"Failed to get {ticket_id}: {json.loads(resp.text)['errorMessages']}"
        tickets = json.loads(resp.text)["issues"]
        for ticket in tickets:
            output.append({
                "jira_ticket_id": ticket["key"],
                "jira_ticket_content": ticket,
            })

        return output

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
        # TODO: actually read schema from this link (if its just a link) into resources section of YAML
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
            "notes": fields["description"]["content"][0]["content"][0]["text"] # TODO: FORMAT TEXT HERE
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
        

        logging.info(f"Generating yaml file: {filename}")
        try:
            with open(YAML_DIR_PATH / filename, "w") as file:
                yaml.dump(content, file, sort_keys=False)
        except PermissionError:
            message = "Note: yaml file already exist, please double check!"
            raise Exception(message)
        return {"filename": filename}

    def process_ticket(jira_issue_id): 
        # TODO
        # consider removing status transition
        # create YAML, run DAG, comment on jira issue w success or failure

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

    @task
    def make_yaml_configs(tickets):
        """Generate all yamls for "Publish New Open Dataset Page" Jira issues"""

        # prep output of new dag ids
        items = []

        # get jira issues and request type
        for ticket in tickets:
            # get issue content
            jira_ticket_id = ticket["jira_ticket_id"]
            issue = ticket["jira_ticket_content"]
            
            request_type = issue["fields"]["customfield_10502"]["requestType"][
                "name"
            ]
            logging.info(
                f"Jira Ticket Id: {jira_ticket_id} has Request Type: '{request_type}'"
            )

            if request_type == "I’m ready to publish a new dataset":
                issue_content = grab_issue_content(issue)
                items.append({
                    "dag_id": list(issue_content.keys())[0],
                    "jira_ticket_id": jira_ticket_id,
                    })
                filename = "".join(list(issue_content.keys())) + ".yaml"
                # write data to yaml file
                write_to_yaml(issue_content, filename)

            if request_type == "I’d like to update an existing dataset or page":
                package_name = (
                    issue["fields"]["summary"].split("/dataset/")[1].strip("//")
                )
                items.append({
                    "dag_id":package_name,
                    "jira_ticket_id": jira_ticket_id
                })
                ckan_url = CKAN + "api/3/action/package_show?id=" + package_name

                # grab metadata
                metadata = metadata_generator(
                    ckan_url, YAML_METADATA, PACKAGE_METADATA
                )
                filename = package_name + ".yaml"
                # write data to yaml file
                write_to_yaml(metadata, filename)

        return items

    
    @task
    def validate_dags(items):
        # basic validation of new dag(s) (including running the new dag(s)), return the results
        output = {}

        for item in items:
            dag_id = item["dag_id"]
            jira_ticket_id = item["jira_ticket_id"]
            these_issues = []
            # read new yaml
            filename = item["dag_id"] + ".yaml"

            with open(YAML_DIR_PATH / filename, "r") as f:
                config = yaml.load(f, yaml.SafeLoader)[dag_id]

                # does yaml have a populated 'resources' section?
                if len(config.get("resources", "")) > 0:
                    for k,v in config["resources"].items():
                        # format is populated
                        if v.get("format", "") == "":
                           these_issues.append(f"{k} has no associated format")

                        # url is an accessible url
                        try:
                            if requests.get(v.get("url", "")).status_code != 200:
                                these_issues.append(f"{k} has an invalid data source")
                        except requests.exceptions.MissingSchema:
                            these_issues.append(f"{k} has an invalid data source")

                        # attributes is populated
                        if len(v.get("attributes", [])) == 0:
                            these_issues.append(f"{k} has an no attributes populated")

                else:
                   these_issues.append("No resources identified on this dataset")
            
            if len(these_issues) == 0:
                these_issues = ["No issues found"]
            output[jira_ticket_id] = these_issues

        return output

    @task
    def message_jira(ticket_issues):
        # summarize issues associated with each YAML in associated JIRA ticket comments
        
        for ticket_id, issues in ticket_issues.items():
            message = []

            for issue in issues:
                message.append({
                    "type": "listItem",
                    "content": [
                        {
                        "type": "paragraph",
                        "content": [
                            {
                            "type": "text",
                            "text": issue
                            }
                        ]
                        }
                    ]
                })
            body = {
                "version": 1,
                "type": "doc",
                "content": [
                    {
                    "type": "paragraph",
                    "content": [
                        {
                            "type": "emoji",
                            "attrs": {
                                "shortName": ":robot:",
                                "id": "1f916",
                                "text": "🤖"
                            },
                        },
                        {
                            "type": "text",
                            "text": " Airflow Config Checker Report",
                        },
                        {
                        "type": "text",
                        "text": " "
                        }
                    ]
                    },
                    {
                    "type": "bulletList",
                    "content": message
                    }
                ]
                }
            
            payload = json.dumps({
                "body": body,
                "visibility": {
                    "type": "role",
                    "value": "Administrators"
                    }
                
            })

            url = (
                "https://toronto.atlassian.net/rest/api/3/issue/"
                + ticket_id
                + "/comment"
            )
            headers = {"Authorization": JIRA_API_KEY, "Content-Type": "application/json"}

            response = requests.request("POST", url, headers=headers, data=payload)
            
            logging.info(f"Status: {response.status_code} - {response.text}")

    
    dag_ids = make_yaml_configs(get_jira_issues())
    results = validate_dags(dag_ids)
    message_jira(results)

jira_to_yaml()