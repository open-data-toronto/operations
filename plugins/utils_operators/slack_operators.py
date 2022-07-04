# slack_operators.py - logic for sending failure and success messages from airflow to slack

import logging
from pathlib import Path

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from airflow.models import Variable

## Init airflow variables
ACTIVE_ENV = Variable.get("active_env")
SLACK_CONN_ID = 'slack' if ACTIVE_ENV == "prod" else "slack_dev"
AIRFLOW_URLS = Variable.get("ckan_credentials_secret", deserialize_json=True)
AIRFLOW_URL = AIRFLOW_URLS[ ACTIVE_ENV ]["address"]
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
