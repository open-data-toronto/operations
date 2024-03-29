import json
import math
import os
import shutil
from datetime import timedelta
from pathlib import Path
from time import sleep

import requests
from airflow.models import Variable


def get_default_args(args={}):
    return {
        "owner": "Carlos",
        "depends_on_past": False,
        "email": ["carlos.hernandez@toronto.ca"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
        **args
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'dag': dag,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    }


# TODO: move to Operator
def message_slack(name, msg, message_type, prod_webhook=True, active_env=None):
    if active_env is None:
        active_env = Variable.get("active_env")

    header = f"{message_type}\n"
    if message_type.lower() == "error":
        header = header.upper()

    msg_title = f"*{name}* | {active_env.upper()}"

    head = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": msg_title},
    }

    max_block_length = 3000 - len(msg_title)
    number_of_blocks = math.ceil(len(msg) / max_block_length)

    lines = msg.splitlines()
    for n in range(number_of_blocks):
        block_lines = []

        if n > 0:
            sleep(1)

        for i, l in enumerate(lines):
            if len("\n".join(block_lines)) + len(json.dumps(head)) > max_block_length:
                lines = lines[i:]
                break
            block_lines.append(l)

        data = json.dumps(
            {
                "blocks": [
                    head,
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": "\n".join(block_lines)},
                    },
                ]
            }
        )

        webhook_url = (
            "slack_webhook_secret" if prod_webhook else "slack_dev_webhook_secret"
        )

        res = requests.post(
            Variable.get(webhook_url),
            data=data,
            headers={"Content-Type": "application/json"},
        )

        assert (
            res.status_code == 200
        ), f"Request NOT OK - Status code: {res.status_code}: {res.reason} | {data}"


def create_dir_with_dag_name(**kwargs):
    dag_id = kwargs.pop("dag_id")
    dir_variable_name = kwargs.pop("dir_variable_name")
    files_dir = Variable.get(dir_variable_name)

    dir_with_dag_name = Path(files_dir) / dag_id
    dir_with_dag_name.mkdir(parents=True, exist_ok=True)

    return str(dir_with_dag_name)


def delete_tmp_data_dir(**kwargs):
    dag_id = kwargs.pop("dag_id")
    recursively = kwargs.get("recursively")

    files_dir_path = Path(Variable.get("tmp_dir"))
    dag_tmp_dir = files_dir_path / dag_id

    if not recursively:
        os.rmdir(dag_tmp_dir)
    else:
        shutil.rmtree(dag_tmp_dir)


def delete_file(**kwargs):
    task_ids = kwargs.pop("task_ids")
    ti = kwargs.pop("ti")

    for task_id in task_ids:
        filepath = ti.xcom_pull(task_ids=task_id)
        os.remove(filepath)
