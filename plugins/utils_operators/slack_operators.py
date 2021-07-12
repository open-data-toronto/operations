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
AIRFLOW_URL = "https://od-airflow.intra.dev-toronto.ca/" if Variable.get("active_env") == "prod" else "https://od-airflow2.intra.dev-toronto.ca/"
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
    @apply_defaults
    def __init__(
        self,
        #target_task_id: str,
        #target_return_key: str,
        message: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        #self.target_task_id = target_task_id
        #self.target_return_key = target_return_key
        self.slack_msg = message

        #self.slack_msg = """{{ task_instance.xcom_pull(task_ids=self.target_task_id, key=self.target_return_key) }}"""

    def execute(self, context):
        send_to_slack = SlackWebhookOperator( 
            task_id="slack",
            http_conn_id=SLACK_CONN_ID,
            webhook_token=slack_webhook_token,
            message=self.slack_msg,
            username=USERNAME
        )

        return send_to_slack.execute(context=context)

    # task_instance.xcom_pull(‘other_task’, key=’return_value’)