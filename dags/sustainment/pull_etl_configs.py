""" This DAG goes to FME, NiFi, and Airflow to pull information about their
ETLs"""

import json
import os
import requests
import yaml
import ckanapi
import csv

import datetime

from utils import airflow_utils
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from utils_operators.slack_operators import task_failure_slack_alert
from ckan_operators.package_operator import GetOrCreatePackageOperator
from ckan_operators.resource_operator import GetOrCreateResourceOperator

from airflow.models import DagBag

# init DAG variables

DEFAULT_ARGS = airflow_utils.get_default_args(
    {
        "owner": "Mackenzie",
        "depends_on_past": False,
        "email": ["mackenzie.nichols4@toronto.ca"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "on_failure_callback": task_failure_slack_alert,
        "start_date": datetime.datetime(2022, 7, 7, 0, 0, 0),
        "catchup": False,
    }
)

DESCRIPTION = "Pulls Open Data ETL info from FME, Airflow, and NiFi"
SCHEDULE = "0 6 * * *"
TAGS = ["sustainment"]

# Init CKAN
ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN_APIKEY = CKAN_CREDS[ACTIVE_ENV]["apikey"]  #
CKAN_ADDRESS = CKAN_CREDS[ACTIVE_ENV]["address"]
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])

# Init CKAN package metadata
package_name = "od-etl-configs"
package_metadata = {
    "private": True,
    "title": "OD ETL List",
    "date_published": "2022-05-06 00:00:00.000000",
    "refresh_rate": "Daily",
    "dataset_category": "Table",
    "owner_unit": "",
    "owner_section": "",
    "owner_division": "Information & Technology",
    "owner_email": "",
    "civic_issues": "",
    "topics": "",
    "tags": [],
    "excerpt": "",
    "limitations": "",
    "notes": "",
}

# FME
def get_fme_configs(**kwargs):
    """Grab FME ETL metadata from file in NAS"""
    fme_configs_url = ("http://opendata.toronto.ca/"
                       "technology.services/_OpenData_fmeserver_jobs_list/"
                       "FMEServer_publishing_jobs.json")

    fme_response = json.loads(requests.get(fme_configs_url).text)

    # return a list of FME job names
    return {"output": [item["name"] for item in fme_response["items"]]}

# NIFI
def get_nifi_configs(**kwargs):
    """Grab NIFI ETL metadata from file in NAS"""
    nifi_configs_url = ("http://opendata.toronto.ca/"
                        "config/nifi_od_etl_inventory.json")
    try:
        nifi_response = json.loads(requests.get(nifi_configs_url).text)
        # flatten the structure of the nifi configs somewhat,
        # to make combining this with airflow configs easier
        for i in range(len(nifi_response)):
            if "addtParams" in nifi_response[i].keys():
                nifi_response[i]["engine"] = "NiFi"
                nifi_response[i]["target_package_name"] = nifi_response[i][
                    "addtParams"
                ]["PACKAGE_NAME"]
                nifi_response[i]["target_resource_name"] = nifi_response[i][
                    "addtParams"
                ]["RESOURCE_NAME"]
                nifi_response[i]["source_data"] = (
                    nifi_response[i]["connection"]["type"]
                    + " - "
                    + nifi_response[i]["connection"]["hostName"]
                )
                if (
                    "https://services3.arcgis.com"
                    in nifi_response[i]["connection"]["hostName"]
                ):
                    nifi_response[i]["source_data"] = nifi_response[i][
                        "connection"]["hostName"].split(
                            "/query?where=1=1&outFields=*")[0]
                nifi_response[i]["schedule"] = nifi_response[i][
                    "addtParams"]["SCHEDULING_PERIOD"]

                nifi_response[i]["config_location"] = (
                    nifi_response[i]["sourceCategory"]
                    + " > "
                    + nifi_response[i]["group"]
                    + " > "
                    + nifi_response[i]["name"]
                )
                nifi_response[i]["od_owner"] = nifi_response[i]["addtParams"][
                    "EMAIL"
                    ]
                nifi_response[i]["etl_description"] = nifi_response[i][
                    "description"
                    ]
            else:
                nifi_response[i]["engine"] = None
                nifi_response[i]["target_package_name"] = None
                nifi_response[i]["target_resource_name"] = None
                nifi_response[i]["source_data"] = None
                nifi_response[i]["schedule"] = None
                nifi_response[i]["config_location"] = None
                nifi_response[i]["od_owner"] = None
                nifi_response[i]["etl_description"] = None

        print(nifi_response)
        return {"output": nifi_response}
    except Exception as e:
        print("Failed while getting and parsing NiFi configs")
        print(e)


# Airflow
def get_airflow_configs(**kwargs):
    """Get ETL DAG metadata from airflow"""
    airflow_configs = []
    # Legacy custom DAGs
    dagbag = DagBag()
    for dag in dagbag.dags.values():
        print(dag)
        if "etl_mapping" in dag.default_args.keys():
            for mapping in dag.default_args["etl_mapping"]:

                airflow_configs.append(
                    {
                        "source_data": mapping["source"],
                        "target_package_name": mapping["target_package_name"],
                        "target_resource_name": mapping[
                            "target_resource_name"
                            ],
                        "engine": "Airflow Custom Job",
                        "schedule": dag.schedule_interval,
                        "config_location": dag.filepath,
                        "od_owner": dag.owner,
                        # "etl_description": dag.description,
                    }
                )
        elif dag.dag_id == "upload_remote_files":
            # upload_remote_files
            with open(
                ("/data/operations/dags/sustainment/upload_remote_files/"
                 "config.yaml"), "r"
            ) as f:
                remote_items_config = yaml.load(f, yaml.SafeLoader)

            for package_name in remote_items_config.keys():
                # we can skip the tags dataset
                if package_name.lower == "tags":
                    continue
                for resource_name in list(
                        remote_items_config[package_name].keys()
                        ):
                    resource = remote_items_config[package_name][resource_name]
                    airflow_configs.append(
                        {
                            "source_data": resource["url"],
                            "target_package_name": package_name,
                            "target_resource_name": resource_name,
                            "engine": "Airflow - Remote Upload Files Job",
                            "schedule": dag.schedule_interval,
                            "config_location": dag.filepath,
                            "od_owner": dag.owner,
                        }
                    )
        elif dag.default_args.get("config_folder", False):
            # YAML Jobs
            for config_file in os.listdir(dag.default_args["config_folder"]):
                if (config_file.endswith(".yaml")
                        and config_file[:-5] == dag.dag_id):
                    print(config_file)
                    print(dag.dag_id)

                    # read config file
                    with open(
                        dag.default_args["config_folder"] + "/" + config_file,
                        "r"
                    ) as f:
                        config = yaml.load(f, yaml.SafeLoader)
                        print(config)
                        package_name = list(config.keys())[0]
                        print(package_name)
                        for resource_name in list(
                            config[package_name]["resources"].keys()
                        ):
                            print(resource_name)

                            airflow_configs.append(
                                {
                                    "source_data": config[package_name][
                                        "resources"][resource_name]["url"],
                                    "target_package_name": package_name,
                                    "target_resource_name": resource_name,
                                    "engine": "Airflow - YAML Job",
                                    "schedule": dag.schedule_interval,
                                    "config_location": dag.default_args[
                                        "config_folder"
                                        ]
                                    + "/"
                                    + config_file,
                                    "od_owner": dag.owner,
                                }
                            )
    print(airflow_configs)
    return {"output": airflow_configs}


def combine_configs(**kwargs):
    """Puts configs together into single usable resource"""
    # init output
    output = []

    # get configs from other operators
    nifi_configs = kwargs["ti"].xcom_pull(task_ids="get_nifi_configs")[
        "output"
        ]
    airflow_configs = kwargs["ti"].xcom_pull(task_ids="get_airflow_configs")[
        "output"
        ]
    fme_configs = kwargs["ti"].xcom_pull(task_ids="get_fme_configs")[
        "output"
        ]

    # make a master list of etl configs
    configs = airflow_configs + nifi_configs

    # combine master list of etl configs with ckan resources
    # get packages
    package_names = CKAN.action.package_list()
    print("There are {} package names".format(str(len(package_names))))
    print("There are {} nifi_configs".format(str(len(nifi_configs))))
    print("There are {} airflow_configs".format(str(len(airflow_configs))))
    print("There are {} fme_configs".format(str(len(fme_configs))))
    for package_name in package_names:
        print("Now processing the following package name: " + package_name)
        # skip the tags dataset - we dont care about that as much
        if package_name == "tags":
            continue
        package = CKAN.action.package_show(name_or_id=package_name)
        print(package)
        if package_name in [
                config["target_package_name"] for config in configs]:

            # for each resource in a package,
            # try to connect it to an etl config
            for resource in package["resources"]:
                print(resource["name"])
                for config in configs:
                    # if this ETL config matches a resource name,
                    # combine them and add them to the output
                    if (
                        resource["name"] == config["target_resource_name"]
                        and package_name == config["target_package_name"]
                    ):
                        print("Matched! " + resource["name"])
                        # if this is associated w FME, tag it as such
                        fme = True if package_name in fme_configs else False

                        output.append(
                            {
                                "package_id": package.get("name", None),
                                "resource_name": resource["name"],
                                "engine": config.get("engine", None),
                                "fme": fme,
                                "source_data": config.get(
                                    "source_data", None),
                                "datastore_active": resource[
                                    "datastore_active"
                                    ],
                                "refresh_rate": package.get(
                                    "refresh_rate", None),
                                "schedule": config.get("schedule", None),
                                "is_retired": package.get("is_retired", None),
                                "owner_division": package.get(
                                    "owner_division", None),
                                "owner_unit": package.get("owner_unit", None),
                                "owner_email": package.get(
                                    "owner_email", None),
                                "resource_last_modified": resource[
                                    "last_modified"
                                    ]
                                or resource["created"],
                                "config_location": config.get(
                                    "config_location", None),
                                "od_owner": config.get("od_owner", None),
                                "date_published": package.get(
                                    "date_published", None),
                                "package_last_refreshed": package.get(
                                    "last_refreshed", None
                                ),
                            }
                        )
                # if the resource isnt in the NiFi or Airflow configs,
                # AND it's not a datastore cache file,
                # then keep it with empty ETL info
                if resource["name"] not in [
                    config["target_resource_name"] for config in configs
                ] and resource.get("is_datastore_cache_file", False) in [
                    False,
                    "false",
                    "False",
                ]:
                    output.append(
                        {
                            "package_id": package.get("name", None),
                            "resource_name": resource["name"],
                            "engine": None,
                            "fme": False,
                            "source_data": None,
                            "datastore_active": resource["datastore_active"],
                            "refresh_rate": package.get("refresh_rate", None),
                            "schedule": None,
                            "is_retired": package.get("is_retired", None),
                            "owner_division": package.get(
                                "owner_division", None),
                            "owner_unit": package.get("owner_unit", None),
                            "owner_email": package.get("owner_email", None),
                            "resource_last_modified": (
                                resource["last_modified"]
                                or resource["created"]),
                            "config_location": None,
                            "od_owner": None,
                            "date_published": package.get(
                                "date_published", None),
                            "package_last_refreshed": package.get(
                                "last_refreshed", None
                            ),
                        }
                    )
        else:
            # skip the tags dataset - we dont care about that as much
            if package_name == "tags":
                continue
            # if the package doesnt match anything,
            # add all its resources without ETL info
            for resource in package["resources"]:
                if resource.get("is_datastore_cache_file", False) in [
                    False,
                    "false",
                    "False",
                ]:
                    print("No match - adding " + resource["name"])
                    output.append(
                        {
                            "package_id": package.get("name", None),
                            "resource_name": resource.get("name", None),
                            "engine": None,
                            "fme": False,
                            "source_data": None,
                            "datastore_active": resource.get(
                                "datastore_active", None),
                            "refresh_rate": package.get("refresh_rate", None),
                            "schedule": None,
                            "is_retired": package.get("is_retired", None),
                            "owner_division": package.get(
                                "owner_division", None),
                            "owner_unit": package.get("owner_unit", None),
                            "owner_email": package.get("owner_email", None),
                            "resource_last_modified": resource["last_modified"]
                            or resource["created"],
                            "config_location": None,
                            "od_owner": None,
                            "date_published": package.get(
                                "date_published", None),
                            "package_last_refreshed": package.get(
                                "last_refreshed", None
                            ),
                        }
                    )
    print("Output is this long: " + str(len(output)))

    # sort dicts by package names
    sorted_output = sorted(output, key=lambda d: d["package_id"])

    return {"output": sorted_output}


# Write Configs
def write_configs(**kwargs):
    configs = kwargs["ti"].xcom_pull(task_ids="combine_configs")["output"]
    f = open("/data/tmp/etl_inventory.csv", "w")
    writer = csv.DictWriter(f, fieldnames=list(configs[0].keys()))
    writer.writeheader()
    for config in configs:
        writer.writerow(config)

    f.close()

    # get resource info from other operator
    resource = kwargs["ti"].xcom_pull(task_ids="get_or_create_resource")
    if resource["datastore_active"] in ["true", True]:
        CKAN.action.datastore_delete(id=resource["id"])
    # this is a problematic try - except clause.
    #   The resource we work with here can only be accessed by authorized users
    # so creating the datastore resource works
    # ... but the CKAN extension fails to datastore_cache the resource
    # this is fine for the purposes of this DAG
    # ... but annoying that we have to put a generic try - except clause
    try:
        CKAN.action.datastore_create(
            id=resource["id"],
            fields=[{"id": key, "type": "text"}
                    for key in list(config.keys())],
            records=configs,
        )
    except Exception as e:
        print(e)


# Run DAG
with DAG(
    "pull_etl_configs.py",
    description=DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE,
    tags=TAGS,
) as dag:

    get_nifi_configs = PythonOperator(
        task_id="get_nifi_configs",
        python_callable=get_nifi_configs,
        provide_context=True,
    )

    get_airflow_configs = PythonOperator(
        task_id="get_airflow_configs",
        python_callable=get_airflow_configs,
        provide_context=True,
    )

    get_fme_configs = PythonOperator(
        task_id="get_fme_configs",
        python_callable=get_fme_configs,
        provide_context=True,
    )

    combine_configs = PythonOperator(
        task_id="combine_configs",
        python_callable=combine_configs,
        provide_context=True
    )

    write_configs = PythonOperator(
        task_id="write_configs",
        python_callable=write_configs,
        provide_context=True
    )

    get_or_create_package = GetOrCreatePackageOperator(
        task_id="get_or_create_package",
        address=CKAN_ADDRESS,
        apikey=CKAN_APIKEY,
        package_name_or_id=package_name,
        package_metadata=package_metadata,
    )

    # get or create a resource a file
    get_or_create_resource = GetOrCreateResourceOperator(
        task_id="get_or_create_resource",
        address=CKAN_ADDRESS,
        apikey=CKAN_APIKEY,
        package_name_or_id=package_name,
        resource_name="od-etl-configs",
        resource_attributes=dict(
            format="CSV",
            is_preview=False,
            url_type="datastore",
            extract_job="Airflow",
            package_id=package_name,
            url="placeholder",
        ),
    )

    (
        get_nifi_configs
        >> get_airflow_configs
        >> get_fme_configs
        >> combine_configs
        >> get_or_create_package
        >> get_or_create_resource
        >> write_configs
    )
