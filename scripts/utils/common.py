import argparse
import json
import logging
import sys
from pathlib import Path
import math
from time import sleep

import requests
import yaml


class Helper:
    def __init__(
        self, script: Path, log_level: str = "DEBUG", make_directories: bool = True
    ):
        self.root_dir = script.parent.parent

        self.configs_dir = self.root_dir / "configs"
        self.config = self.configs_dir / "config.yaml"
        self.__config = self.__get_config()

        args = self.__parse_runtime_args()
        self.backups_dir = args.backups_dir
        self.staging_dir = args.staging_dir
        self.logs_dir = args.logs_dir
        self.active_env = args.active_env

        if make_directories:
            self.__make_directories()

        log_levels = ["DEBUG", "INFO", "WARN", "ERROR"]
        assert log_level.upper() in log_levels, f"Log level must be in: {log_levels}"

        self.log_level = log_level.upper()

        self.name = script.name.replace(".py", "")

        self.logs = self.logs_dir / f"{self.name}-{self.log_level.lower()}.log"

    def get_all(self):
        return self.get_logger(), self.get_ckan_credentials(), self.get_directories()

    def get_logger(self):
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)-8s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        handler = logging.FileHandler(self.logs, mode="a+")
        handler.setFormatter(formatter)

        screen_handler = logging.StreamHandler(stream=sys.stdout)
        screen_handler.setFormatter(formatter)

        logger = logging.getLogger(self.name)
        logger.setLevel(self.log_level)

        logger.addHandler(handler)
        logger.addHandler(screen_handler)

        return logger

    def get_directories(self):
        return {
            "backups": self.backups_dir,
            "staging": self.staging_dir,
            "logs": self.logs_dir,
            "configs": self.configs_dir,
        }

    def get_ckan_credentials(self):
        return self.__config["ckan"][self.active_env]

    def send_notifications(self, msg: str, message_type: str):
        responses = []

        notification_configs = self.__config["notifications"]

        if (
            "slack" in notification_configs
            and "webhook_url" in notification_configs["slack"]
        ):
            header = f"{message_type}\n"
            if message_type.lower() == "error":
                header = header.upper()

            msg_title = "*{}*: {} | {}".format(
                self.name, self.get_ckan_credentials()["address"], header
            )

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
                    "title": self.name,
                    "message": msg,
                    "type": message_type,
                },
            )

            responses.append({"wirepusher": response})

        return responses

    def __make_directories(self):
        for directory in [self.backups_dir, self.staging_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    def __parse_runtime_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--active_env", nargs="?", default=self.__config["active_env"], type=str
        )
        parser.add_argument(
            "--backups_dir",
            nargs="?",
            default=self.root_dir / self.__config["directories"]["backups"],
            type=str,
        )
        parser.add_argument(
            "--staging_dir",
            nargs="?",
            default=self.root_dir / self.__config["directories"]["staging"],
            type=str,
        )
        parser.add_argument(
            "--logs_dir",
            nargs="?",
            default=self.root_dir / self.__config["directories"]["logs"],
            type=str,
        )
        args = parser.parse_args()

        return args

    def __get_config(self):
        with open(self.config, "r") as f:
            config = yaml.load(f, yaml.SafeLoader)

        return config
