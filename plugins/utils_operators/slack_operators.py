# slack_operators.py - logic for sending failure and success messages from airflow to slack

import logging
import os
from pathlib import Path

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from airflow.models import Variable

## Init airflow variables
ACTIVE_ENV = Variable.get("active_env")
SLACK_CONN_ID = 'slack' if ACTIVE_ENV == "prod" else "slack_dev"
AIRFLOW_URL = Variable.get("airflow_urls", deserialize_json=True)[ ACTIVE_ENV ]
USERNAME = "Operator" if ACTIVE_ENV == "prod" else "airflow-test"

slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password


# The following are slack FUNCTIONS that use the built-in slack operator
# Airflow takes functions
def task_success_slack_alert(context):
    """
    Taken largely from https://github.com/tszumowski/airflow_slack_operator/blob/master/dags/slack_operator.py

    Callback task that can be used in DAG to alert of success task completion
    Input:
        context (dict): Context variable passed in from Airflow - not needed if uses as a config option
    Returns:
        Calls the SlackWebhookOperator execute method internally
    """

    slack_msg = """
            :large_green_circle: Task Succeeded  
            *Task*: {task}  
            *Dag*: {dag} 

            *Log Url*:\n {log_url} 
            
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,

            log_url=context.get('task_instance').log_url.replace( "http://localhost:8080/", AIRFLOW_URL ) ,
        )

    succeeded_alert = SlackWebhookOperator(
        task_id="slack",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username=USERNAME,
    )

    return succeeded_alert.execute(context=context)







def task_failure_slack_alert(context):
    """
    Taken largely from https://github.com/tszumowski/airflow_slack_operator/blob/master/dags/slack_operator.py

    Callback task that can be used in DAG to alert of success task failure
    Input:
        context (dict): Context variable passed in from Airflow - not needed if uses as a config option
    Returns:
        Calls the SlackWebhookOperator execute method internally
    """

    slack_msg = """
            :red_circle: Task Failed  
            *Task*: {task}  
            *Dag*: {dag} 
            *Log Url*:\n {log_url} 
            
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            log_url=context.get('task_instance').log_url.replace( "http://localhost:8080/",AIRFLOW_URL ) ,
        )
    

    failed_alert = SlackWebhookOperator(
        task_id="slack",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username=USERNAME,
    )

    return failed_alert.execute(context=context)


class GenericSlackOperator(BaseOperator):
    """
    Writes a message to the appropriate slack channel, depending on the server where this is being run
    
    Expects as input:
    message_header
    :   the first line of the message, to appear in bold font
    message_content
    :   a number of records moved that this operator wants to report on - can be received from another task or a hardcoded value
    message_body
    :   the text that comes after the message content - a hardcoded value to add detail to the message_content
    """
    @apply_defaults
    def __init__(
        self,
        message_body: str = None,
        message_content: str = None,
        message_content_task_id: str = None,
        message_content_task_key: str = None,
        message_header: str = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.message_header = message_header
        self.message_content, self.message_content_task_id, self.message_content_task_key = message_content, message_content_task_id, message_content_task_key
        self.message_content_task_key = message_content_task_key
        self.message_body = message_body

    def execute(self, context):
        ti = context['ti']
        # if a message has no content, dont send a message

        if self.message_content_task_id and self.message_content_task_key:
            self.message_content_task = ti.xcom_pull(task_ids=self.message_content_task_id)
            if self.message_content_task:
                self.message_content = self.message_content_task[self.message_content_task_key]
            else:
                return
            
        # if the message content is a dict, print it nicely
        if isinstance(self.message_content, dict):
            highlight_terms = ["failed", 0, "500"]
            self.message_content = "\n\t\t   ".join( ["*" + key + "*: " + value + ":exclamation:" if value in highlight_terms else
                                                      "*" + key + "*: " + value 
                                                      for (key,value) in self.message_content.items()] )

        slack_message = """
            :robot_face: *{header}*
            *DAG_ID*: `{dag}` 
            {content} {body}
            """.format(
            header=self.message_header,
            dag=context.get('task_instance').dag_id,
            content=self.message_content,
            body=self.message_body
        )



        send_to_slack = SlackWebhookOperator( 
            task_id="slack",
            http_conn_id=SLACK_CONN_ID,
            webhook_token=slack_webhook_token,
            message=slack_message,
            username=USERNAME
        )

        return send_to_slack.execute(context=context)





'''
This Modules contains the classes creating and sending messages to messengers like Slack.

Classes:
- MessageFactory: Generates a formatted message for each resource in a YAML file.
- SlackTownCrier: Writes a message to the appropriate slack channel.
'''


class MessageFactory:
    """
    Build package-specific messages outlining the number of records
    inserted into each resource.

    Attributes:
    - package_name: str
        The name of the package.
    - yaml_file_content: dict
        All the values under the package name in the YAML file.
    - record_count: str or int
        Number of records in a resource

    Methods:
    - __init__(self, package_name, yaml_file_content, record_count): 
        Initialize a new instance of the MessageFactory class
    - scribe(self): 
        Generates a formatted message for each resource in a YAML file.
    """

    def __init__(
        self,
        package_name: str, 
        yaml_file_content, 
        record_count,
    ):
        """
        Initialize a new instance of the MessageFactory class
        and construct all the necessary attributes.

        Arguments:
        - package_name: str
        The name of the package.
        - yaml_file_content: dict
            All the values under the package name in the YAML file.
        - record_count: str or int
            Number of records in a resource; 
            usually reads from the insert_records_resource_name["record_count"].
        
        Returns: None
        """
        self.package_name = package_name 
        self.yaml_file_content = yaml_file_content
        self.record_count = record_count

    def scribe(self):
        """
        Generate a formatted message for each resource in a YAML file.
        Usually, the outpout would be fed into the SlackTownCrier to be announced on Slack.

        Returns:
            Str: The final message.
            None: If no resources are found in the YAML file.
        """
        resources = self.yaml_file_content.get("resources", {})

        if not resources:   
            logging.info("No resources found in the YAML file.")
            return None

        message_lines = [f"*Package*: {self.package_name}", "\t\t\t*Resources*:"]
        
        for resource_name, resource_content in resources.items():
            resource_format = resource_content.get("format")
            
            if resource_format:
                message_line = f"\t\t\t\t- {resource_name} `{resource_format}`: {self.record_count} records"
                message_lines.append(message_line)
            else:
                logging.error(f"No format found for resource: {resource_name}")
        
        # The following `if` statement makes sure that the blanc/meaningless messages won't be created.
        final_message = "\n".join(message_lines) if len(message_lines) > 2 else None
        
        return final_message


class SlackTownCrier:
    """
    Writes a message to the appropriate slack channel, depending on the server where this is being run
    
    Attributes:
        Class-level:
        - ACTIVE_ENV: gets the Airflow active environment
        - USERNAME: sets the username based on the ACTIVE_ENV
        - SLACK_CONN_ID: sets the Slack connection ID based on the ACTIVE_ENV
        - SLACK_WEBHOOK_TOKEN: gets the password related to SLACK_CONN_ID
        Instance-level:
        - dag_id: The DAG ID; It appears after the message_header
        - message_header: The title of the message, to appear in bold font.
        - message_content: The content of the message; Usually supplied by the Scribe.
        - message_body: Sort of a footer. Displayed at the end of the message.

    Methods:
    - announce(self): send the message to the slack
    """

    ## Init airflow variables
    ACTIVE_ENV = Variable.get("active_env")
    #AIRFLOW_URL = Variable.get("airflow_urls", deserialize_json=True)[ ACTIVE_ENV ] ##UNnecessary???
    USERNAME = "Operator" if ACTIVE_ENV == "prod" else "airflow-test"
    SLACK_CONN_ID = 'slack' if ACTIVE_ENV == "prod" else "slack_dev"
    SLACK_WEBHOOK_TOKEN = BaseHook.get_connection(SLACK_CONN_ID).password

    def __init__(
        self,
        dag_id: str = None,
        message_header: str = None,
        message_content: str = None,
        message_body: str = None,
    ) -> None:
        """
        Initialize a new instance of the SlackTownCrier class
        and construct all the necessary attributes.

        Arguments:
        - dag_id: str
            The DAG ID; It appears after the message_header
        - message_header: str
            The title of the message, to appear in bold font.
        - message_content: str
            The content of the message; Usually supplied by the Scribe.
        - message_body: str
            Sort of a footer. Displayed at the end of the message.

        Returns: None
        """
        self.dag_id = dag_id
        self.message_header = message_header
        self.message_content = message_content
        self.message_body = message_body

    def announce(self):
        """
        Send a Slack message after some formatting.
        Return: None
        """
        # if a message has no content, dont send a message
        if self.message_content is None:
            return None

        # if the message content is a dict, print it nicely
        if isinstance(self.message_content, dict):
            highlight_terms = ["failed", 0, "500"]
            self.message_content = "\n\t\t   ".join( ["*" + key + "*: " + value + ":exclamation:" if value in highlight_terms else
                                                      "*" + key + "*: " + value 
                                                      for (key,value) in self.message_content.items()] )

        slack_message = f"""
            :loudspeaker: *{self.message_header}*
            *DAG_ID*: `{self.dag_id}` 
            {self.message_content} 
            {self.message_body}
            """

        send_to_slack = SlackWebhookOperator( 
            task_id="slack",
            http_conn_id=self.SLACK_CONN_ID,
            webhook_token=self.SLACK_WEBHOOK_TOKEN,
            message=slack_message,
            username=self.USERNAME
        )

        return send_to_slack.execute(context={})

