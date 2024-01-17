from datetime import datetime, timedelta
import yaml
import logging
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from utils import airflow_utils

from airflow.operators.python import PythonOperator
from pendulum import datetime

from airflow.decorators import dag, task, task_group
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert
#from utils_operators import slack_operators
from utils_operators.slack_operators import MessageFactory, SlackTownCrier

from ckan_operators.package_operator import GetOrCreatePackage

def create_dag(dag_id,
                package_name,
                yaml_file_content,
                schedule,
                default_args):

    potential_metadata_attributes = [
            "title",
            "date_published",
            "refresh_rate",
            "dataset_category",
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

    package_metadata = {}
    for attr in potential_metadata_attributes:
        if attr in yaml_file_content.keys():
            package_metadata[attr] = yaml_file_content[attr]
        # Following lines are unnecessary in the original file.
        # else:
        #     package_metadata[attr] = None

    # init list of resource names
    resource_names = yaml_file_content["resources"].keys()
    record_count = 100

    @dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule=None, #WARNING: the schedule is hard coded to none! it does not read from YAML file.
        catchup = False
    )
    def dag_gen_refactoring():

        # @task(pool = "ckan_pool")
        # def get_or_create_package(package_name, package_metadata):
        #     package = GetOrCreatePackage(package_name, package_metadata).get_or_create_package()
        #     return package

        # @task
        # def scribe(package):
        #     #message = 'The package author is {}'.format(package["author"])
        #     message = {'k1':'failed', 'k2':'v2'}
        #     return message

        @task
        def scribe(package_name, yaml_file_content, record_count):
            #message = slack_operator_v2.scribe(package_name, yaml_file_content, record_count)
            message = MessageFactory(package_name, yaml_file_content, record_count).scribe()
            return message
        
        @task
        def slack_town_crier(dag_id, message_header, message_content, message_body):
            return SlackTownCrier(dag_id, message_header, message_content, message_body).announce()
        
        #package = get_or_create_package(package_name, package_metadata)

        slack_town_crier(
            dag_id = dag_id,
            message_header = "Slack Town Crier - Test",
            message_content = scribe(package_name, yaml_file_content, record_count),
            message_body = "",
        )

    return dag_gen_refactoring()


CONFIG_FOLDER = os.path.dirname(os.path.realpath(__file__))
file_list = ['dag-gen-refactor-test.yaml']
for file_name in file_list:
    if file_name.endswith(".yaml"):

        # read config file
        with open(CONFIG_FOLDER + "/" + file_name, "r") as f:
            yaml_file = yaml.load(f, yaml.SafeLoader)
            package_name = list(yaml_file.keys())[0]

        yaml_file_content = yaml_file[package_name]
        dag_id = 'dag_gen_refactor_'+file_name.split(".yaml")[0]
        schedule = yaml_file_content["schedule"]
        dag_owner_name = yaml_file_content["dag_owner_name"]
        dag_owner_email = yaml_file_content["dag_owner_email"]

        pool = "default_pool"

        default_args = airflow_utils.get_default_args(
            {
                "owner": dag_owner_name,
                "depends_on_past": False,
                "email": [dag_owner_email],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": 3,
                "on_failure_callback": task_failure_slack_alert,
                "start_date": datetime(2023, 5, 5, 0, 0, 0),
                "config_folder": CONFIG_FOLDER,
                "pool": pool,
                "tags": ["dataset", "yaml"], 
                "weight_rule": "upstream"
            }
        )

        globals()[dag_id] = create_dag(dag_id,
                                        package_name,
                                        yaml_file_content,
                                        schedule,
                                        default_args)