from airflow.models import Variable
from datetime import timedelta
from pathlib import Path
from time import sleep
import requests
import yaml
import math
import json

repo_dir = Variable.get("repo_dir")

sys.path.append(repo_dir)


def load_configs():
    with open(Path(repo_dir) / "configs" / "config.yaml", "r") as f:
        config = yaml.load(f, yaml.SafeLoader)

    return config


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


def message_slack(name, msg, message_type, ckan_url):
    header = f"{message_type}\n"
    if message_type.lower() == "error":
        header = header.upper()

    msg_title = "*{}*: {} | {}".format(name, ckan_url, header)

    head = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": msg_title},
    }

    max_block_length = 3000 - len(msg_title)
    number_of_blocks = math.ceil(len(msg) / max_block_length)

    lines = msg.split("\n")
    for n in range(number_of_blocks):
        block_lines = []

        if n > 0:
            sleep(1)

        for i, l in enumerate(lines):
            if any(
                [
                    n > 0 and len("\n".join(block_lines)) > max_block_length,
                    n == 0
                    and len("\n".join(block_lines)) + len(json.dumps(head))
                    > max_block_length,
                ]
            ):
                lines = lines[i:]
                break
            block_lines.append(l)

        requests.post(
            active_env=Variable.get("slack_webhook"),
            data=json.dumps(
                {
                    "blocks": [
                        head,
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "\n".join(block_lines),
                            },
                        },
                    ]
                }
            ),
            headers={"Content-Type": "application/json"},
        )
