import json
import ast
import requests
import csv
import logging

from typing import List, Dict
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from pathlib import Path

from utils import misc_utils, airflow_utils
from utils_operators.slack_operators import (
    task_failure_slack_alert,
    GenericSlackOperator,
)
from ckan_operators.package_operator import GetOrCreatePackageOperator
from ckan_operators.resource_operator import GetOrCreateResourceOperator


default_args = {
    "owner": "Yanan",
    "email": ["yanan.zhang@toronto.ca"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": task_failure_slack_alert,
    "retry_delay": timedelta(seconds=3),
    "pool": "ckan_pool",
    "retries": 1,
}


@dag(
    default_args=default_args,
    schedule="0 13 * * *",
    start_date=datetime(2023, 11, 1),
    catchup=False,
)
def open_data_intake():
    dir_path = "/data/tmp/"
    package_name = "open-data-intake"
    resource_name = "Toronto Open Data Intake"
    package_metadata = {
        "title": "Toronto Open Data Intake",
        "date_published": "2024-05-17",
        "refresh_rate": "Daily",
        "dataset_category": "Table",
        "owner_division": "Information & Technology",
        "owner_section": None,
        "owner_unit": None,
        "owner_email": "opendata@toronto.ca",
        "civic_issues": None,
        "topics": "City government",
        "tags": [{"name": "queue", "vocabulary_id": None}],
        "information_url": None,
        "excerpt": "This dataset displays the Open Data intake queue.",
        "limitations": None,
        "notes": """This dataset displays the Open Data intake queue in its raw form. Data here contains attributes relating to how the Toronto Open Data team manages its various requests for updating old and publishing new datasets the Toronto Open Data Portal. \n
Typically, an "Open Data Inquiry" ticket is first opened, prompting Open Data to investigate the possibility of updating or adding new data to the portal. Once the investigation is finished, another ticket will be opened, directing Open Data staff to "Publish a New Open Dataset Page" or "Update an Existing Open Dataset Page". These tickets will be related to the initial inquiry so to track the history of a change from beginning (Inquiry) to end (Publication or Update).\n
Records here are direct from an internal ticket management system, so they match exactly what Open Data staff are working with. Each record is a period of time in which a ticket was in a particular status. \n
The creation of this data is to support council motion [2023.EX10.18](https://secure.toronto.ca/council/agenda-item.do?item=2023.EX10.18) and shared with the hopes that it will inform the public what Open Data is working on, and what datasets Open Data is updating or publishing.\n   
        """,
    }

    @task
    def create_tmp_dir(dag_id):
        dir_with_dag_name = Path(dir_path) / dag_id
        dir_with_dag_name.mkdir(parents=True, exist_ok=True)

        return str(dir_with_dag_name)

    @task(
        params={
            # "jira_url": "https://toronto.atlassian.net/rest/api/3/search?jql=type=11468&expand=changelog"
            "jira_url": """https://toronto.atlassian.net/rest/api/3/search?jql="Customer%20Request%20Type"%20in%20("Publish%20New%20Open%20Dataset%20Page"%2C%20"Update%20Existing%20Open%20Dataset%20Page"%2C%20"Make%20Open%20Data%20Inquiry")&expand=changelog"""
        }
    )
    def get_jira_queue(tmp_dir: str, **context) -> str:
        # headers for authenticate jira api calls
        jira_api_key = Variable.get("jira_apikey")
        headers = {"Authorization": jira_api_key}

        # Since jira api calls have maxResults, we need use pagination and get results in chunks
        has_more = True
        jira_queue = []
        offset = 0
        while has_more:
            jira_call = context["params"]["jira_url"] + "&startAt=" + str(offset)
            result = json.loads(requests.get(jira_call, headers=headers).content)

            jira_queue += result["issues"]
            has_more = len(jira_queue) < result["total"]

            logging.info(f"Processing in batch, start from {offset}")
            offset += 50

        ticket_pool = [ticket["key"] for ticket in jira_queue]
        logging.info(f"Getting tickets from {jira_call}")
        logging.info(f"A total of {len(ticket_pool)} tickets in the queue.")
        logging.info(f"Jira Ticket Pool: {json.dumps(ticket_pool)}")

        filename = "jira_queue.csv"
        filepath = tmp_dir + "/" + filename

        with open(filepath, "w") as f:
            dict_writer = csv.DictWriter(f, fieldnames=jira_queue[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(jira_queue)

        return str(filepath)

    @task
    def grab_public_jira_fields(jira_queue_path: str) -> List:
        records = []

        with open(jira_queue_path, newline="") as f:
            jira_queue_reader = csv.DictReader(f)

            for ticket in jira_queue_reader:
                ticket_id = ticket["key"]
                fields = ast.literal_eval(ticket["fields"])

                # Inquiry Source (city council, public, etc)
                if fields["customfield_12376"]:
                    inquiry_source = fields["customfield_12376"].get("value", None)

                # ticket name
                ticket_name = fields["summary"]

                # get ticket request type
                # some tickets have this value empty
                try:
                    request_type = fields["customfield_10502"]["requestType"]["name"]
                except:
                    request_type = "Unknown"

                # time to first response
                completedCycles = fields["customfield_10602"].get(
                    "completedCycles", None
                )

                first_response_time = (
                    completedCycles[0]["stopTime"]["jira"] if completedCycles else None
                )

                # ticket created time
                created_time = fields["created"]

                # linked ticket id
                issue_links = fields["issuelinks"]
                if issue_links:
                    inward_issue_lst = [
                        issue_link["inwardIssue"]["key"]
                        for issue_link in issue_links
                        if "inwardIssue" in issue_link.keys()
                    ]
                else:
                    inward_issue_lst = []

                inward_issue = ",".join(inward_issue_lst)

                logging.info(f"Generate Status Log for Ticket {ticket_id}")

                # get change histories only for status change
                changelog_histories = [
                    changelog
                    for changelog in ast.literal_eval(ticket["changelog"])["histories"]
                    if changelog["items"][0]["field"] == "status"
                ]
                status_log = []
                if changelog_histories:
                    for changelog in reversed(changelog_histories):
                        from_status = changelog["items"][0]["fromString"]
                        to_status = changelog["items"][0]["toString"]
                        timestamp = changelog["created"][:-9]
                        status_log.append(
                            {
                                "Ticket Id": ticket_id,
                                "Ticket Name": ticket_name,
                                "Inquiry Source": inquiry_source,
                                "Request Type": request_type,
                                "Created": created_time[:-9],
                                "First Response": (
                                    first_response_time[:-9]
                                    if first_response_time
                                    else None
                                ),
                                "From Status": from_status,
                                "To Status": to_status,
                                "Status Timestamp": timestamp,
                                "Linked Ticket Id": inward_issue,
                            }
                        )

                # if no status change history, record as None
                else:
                    status_log = [
                        {
                            "Ticket Id": ticket_id,
                            "Ticket Name": ticket_name,
                            "Inquiry Source": inquiry_source,
                            "Request Type": request_type,
                            "Created": created_time[:-9],
                            "First Response": (
                                first_response_time[:-9]
                                if first_response_time
                                else None
                            ),
                            "From Status": None,
                            "To Status": None,
                            "Status Timestamp": None,
                            "Linked Ticket Id": inward_issue,
                        }
                    ]

                # Check if last changelog status match final ticket status; if not append final status.
                final_ticket_status = fields["customfield_10502"]["currentStatus"][
                    "status"
                ]
                final_status_timestamp = fields["customfield_10502"]["currentStatus"][
                    "statusDate"
                ]["jira"]

                if (
                    final_ticket_status != status_log[-1]["To Status"]
                    and changelog_histories
                ):
                    logging.info(
                        f"Updating Final Ticket Status to {final_ticket_status}"
                    )
                    status_log.append(
                        {
                            "Ticket Id": ticket_id,
                            "Ticket Name": ticket_name,
                            "Inquiry Source": inquiry_source,
                            "Request Type": request_type,
                            "Created": created_time[:-9],
                            "First Response": (
                                first_response_time[:-9]
                                if first_response_time
                                else None
                            ),
                            "From Status": status_log[-1]["To Status"],
                            "To Status": final_ticket_status,
                            "Status Timestamp": final_status_timestamp[:-9],
                            "Linked Ticket Id": inward_issue,
                        }
                    )

                records.extend(status_log)

        logging.info(records[5])

        # sort records by key, status date, and created date
        sorted_records = sorted(
            records,
            key=lambda d: (d["Created"], d["Ticket Id"], d["Status Timestamp"]),
            reverse=True,
        )

        return sorted_records

    @task
    def insert_records_into_datastore(records: List, datastore_resource: None) -> Dict:
        ckan = misc_utils.connect_to_ckan()

        data_fields_desc = {
            "Ticket Id": "Unique identifier for a ticket in the city's internal system",
            "Ticket Name": "Working, plain-English name given to the ticket. These are not unique IDs and can change at the discretion of the Open Data team",
            "Inquiry Source": "Where the request/inquiry originated (Public, Private Business, Academia, City Division, City Council, etc)",
            "Request Type": "**Open Data Inquiry** is where most intake starts; it's where Open Data staff investigate the existance of a requested dataset and feasibility of publishing it. From there, it can become a **Publish New Open Dataset Page**, which is the process of connecting to city systems containing data to be published, and reviewing the content and context on an open dataset page before it's published. **Update Existing Open Dataset Page** tickets are the process of making and validating changes to the schema, source system or metadata of an existing page. Some of these values are 'Unknown' because these statuses were introduced after these tickets were made.",
            "Created": "The ticket's original creation time",
            "First Response": "When open data team first replied to this ticket",
            "From Status": "Ticket's status before its current status, if any",
            "To Status": "Ticket's current status",
            "Status Timestamp": "When the ticket changed to this record's status",
            "Linked Ticket Id": "Tickets are often linked to other tickets. When an Open Data Inquiry ticket is completed, it can become a 'Publish New Open Dataset Page' ticket or an 'Update Existing Open Dataset Page' ticket",
        }

        # collecting datastore fields
        fields = []
        for x in records[0].keys():
            if x in ["First Response", "Created", "Timestamp"]:
                datatype = "timestamp"
            else:
                datatype = "text"
            fields.append(
                {"id": x, "type": datatype, "info": {"notes": data_fields_desc[x]}}
            )
        logging.info(fields)

        # drop existing old resource
        try:
            logging.info("Dropping old datastore resource...")
            ckan.action.datastore_delete(id=datastore_resource["id"], force=True)

        except Exception as e:
            # Create datastore resource if no existing one.
            logging.warning(e)
            logging.info("Old datastore doesn't exist, no need to drop.")

        logging.info(f"Inserting to datastore_resource: {resource_name}")
        ckan.action.datastore_create(
            resource_id=datastore_resource["id"],
            records=records,
            fields=fields,
            force=True,
        )

        return {"total_records": len(records)}

    @task
    def build_message(total_records: Dict) -> Dict:
        message = "*Package*: {} \n\t\t   *Resources*: {}: {} records".format(
            package_name, resource_name, total_records["total_records"]
        )

        return {"message": message}

    get_or_create_package = GetOrCreatePackageOperator(
        task_id="get_or_create_package",
        package_name_or_id=package_name,
        package_metadata=package_metadata,
    )

    get_or_create_public_jira_queue_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_public_jira_queue_resource",
        package_name_or_id=package_name,
        resource_name="Open Data Intake Records",
        resource_attributes=dict(
            format="csv",
            is_preview=True,
            url_type="datastore",
            extract_job=f"Airflow: {package_name}",
            package_id=package_name,
            url="placeholder",
        ),
    )

    success_message_slack = GenericSlackOperator(
        task_id="success_message_slack",
        message_header="Public Jira Queue :jira:",
        message_content_task_id="build_message",
        message_content_task_key="message",
        message_body="",
    )

    delete_jira_queue_tmp_file = PythonOperator(
        task_id="delete_jira_queue_tmp_file",
        python_callable=airflow_utils.delete_file,
        op_kwargs={"task_ids": ["get_jira_queue"]},
        provide_context=True,
    )

    delete_tmp_dir = PythonOperator(
        task_id="delete_tmp_data_dir",
        python_callable=airflow_utils.delete_tmp_data_dir,
        op_kwargs={"dag_id": package_name},
    )

    get_or_create_package >> get_or_create_public_jira_queue_resource

    inserted_records = insert_records_into_datastore(
        grab_public_jira_fields(get_jira_queue(create_tmp_dir(package_name))),
        get_or_create_public_jira_queue_resource.output,
    )

    build_message(inserted_records) >> success_message_slack

    inserted_records >> delete_jira_queue_tmp_file >> delete_tmp_dir


open_data_intake()
