import json
import logging
import math
import sys
from time import sleep
import hashlib

import pandas as pd
import requests
import yaml


def make_dirs_if_new(config_filepath, configs):
    for directory in configs["directories"].values():
        file_dir = config_filepath.parent.parent / directory
        file_dir.mkdir(parents=True, exist_ok=True)


def load_yaml(filepath):
    with open(filepath, "r") as f:
        config = yaml.load(f, yaml.SafeLoader)

    return config


def make_logger(name, logs_dir, active_env, log_level="DEBUG"):
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logs_file = logs_dir / f"{active_env.upper()}.{name}.log"

    handler = logging.FileHandler(logs_file, mode="a+")
    handler.setFormatter(formatter)

    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    logger.addHandler(handler)
    logger.addHandler(screen_handler)

    return logger


def send_notifications(name, msg, message_type, configs, ckan_url):
    responses = []

    notification_configs = configs["notifications"]

    if (
        "slack" in notification_configs
        and "webhook_url" in notification_configs["slack"]
    ):
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
        slack_responses = []
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

            response = requests.post(
                notification_configs["slack"]["webhook_url"],
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

            slack_responses.append(response)

        responses.append({"slack": slack_responses})

    if (
        "wirepusher" in notification_configs
        and "id" in notification_configs["wirepusher"]
        and isinstance(msg, str)
    ):
        response = requests.post(
            "https://wirepusher.com/send",
            data={
                "id": notification_configs["wirepusher"]["id"],
                "title": name,
                "message": msg,
                "type": message_type,
            },
        )

        responses.append({"wirepusher": response})

    return responses


def backup_datastore_resource(ckan, resource_id, dest_path, backup_fields):
    resource = ckan.action.resource_show(id=resource_id)
    package = ckan.action.package_show(id=resource["package_id"])

    record_count = ckan.action.datastore_search(id=resource_id, limit=0)["total"]

    datastore_response = ckan.action.datastore_search(
        id=resource_id, limit=record_count
    )

    prefix_parts = [package["name"], resource["name"]]

    results = {}

    data = pd.DataFrame(datastore_response["records"]).drop("_id", axis=1)

    data_hash = hashlib.md5()
    data_hash.update(data.to_csv(index=False).encode("utf-8"))
    prefix_parts.append(data_hash.hexdigest())

    data_path = dest_path / "__".join(prefix_parts + ["data.parquet"])
    if not data_path.exists():
        data.to_parquet(data_path)

    results["data"] = data_path
    results["records"] = data.shape[0]
    results["columns"] = data.shape[1]

    if backup_fields:
        fields = [f for f in datastore_response["fields"] if f["id"] != "_id"]

        fields_path = dest_path / "__".join(prefix_parts + ["fields.json"])
        if not fields_path.exists():
            with open(fields_path, "w") as f:
                json.dump(fields, f)

        results["fields"] = fields_path

    return results


def update_resource_last_modified(ckan, resource_id, new_last_modified):
    return ckan.action.resource_patch(
        id=resource_id, last_modified=new_last_modified.strftime("%Y-%m-%dT%H:%M:%S"),
    )


def get_all_packages(ckan):
    catalogue_size = len(ckan.action.package_list())
    packages = ckan.action.package_search(rows=catalogue_size)["results"]

    return packages
