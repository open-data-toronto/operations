import traceback
from typing import List

import ckanapi
import requests
from dateutil import parser
from utils import common
from utils import ckan_helper
from pathlib import Path
import os
import yaml

PATH = Path(os.path.abspath(__file__))
TOOLS = common.Helper(PATH)

LOGGER, CREDS, DIRS = TOOLS.get_all()
CKAN = ckanapi.RemoteCKAN(**CREDS)


def get_packages_to_sync(remote_files: List):
    all_packages = CKAN.action.package_search(rows=10000)["results"]
    to_sync = [p["package_id"] for p in remote_files]

    packages = [p for p in all_packages if p["name"] in to_sync]

    return packages


def sync_resource_timestamps(package: dict, remote_files: list):
    files = [p for p in remote_files if p["package_id"] == package["name"]][0]["files"]
    resources = package["resources"]

    package_sync_results = []
    for f in files:
        resources_with_url = [r for r in resources if r["url"] == f]

        assert len(resources_with_url) == 1, LOGGER.error(
            f"{package['name']}: No resource for file: {f}"
        )

        resource = resources_with_url[0]

        resource_last_modified = resource["last_modified"]
        if not resource["last_modified"]:
            resource_last_modified = resource["created"]
            LOGGER.debug(f"{resource['id']}: No last_modified, using created.")

        resource_last_modified = parser.parse(f"{resource_last_modified} UTC")

        res = requests.head(f)
        file_last_modified = parser.parse(res.headers["Last-Modified"])

        difference_in_seconds = (
            file_last_modified.timestamp() - resource_last_modified.timestamp()
        )

        record = {
            "package_name": package["name"],
            "resource_name": resource["name"],
            "resource_id": resource["id"],
            "file": f,
            "file_last_modified": file_last_modified.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        if difference_in_seconds == 0:
            LOGGER.debug(f"Up to date: {resource['id']} | {f}")
            package_sync_results.append({**record, "result": "unchanged"})
            continue

        resp = ckan_helper.update_resource_last_modified(
            ckan=CKAN, id=resource["id"], new_last_modified=file_last_modified
        )

        LOGGER.info(f'{resource["id"]}: Last modified set to {resp["last_modified"]}')

        package_sync_results.append({**record, "result": "synced"})

    return package_sync_results


def build_notification_message(sync_results: dict):
    message_lines = [""]

    for result_type in ["synced", "error", "unchanged"]:
        result_type_resources = [r for r in sync_results if r["result"] == result_type]
        result_type_packages = set(
            [r["package_name"] for r in sync_results if r["result"] == result_type]
        )

        lines = [
            "*{}\tpackages: {}\tresources: {}*".format(
                result_type, len(result_type_packages), len(result_type_resources),
            )
        ]

        pkg = None
        for index, r in enumerate(result_type_resources):
            if result_type == "unchanged":
                continue

            if pkg != r["package_name"]:
                lines.append(r["package_name"])

            lines.append(
                "{}. _{}_: `{}`".format(
                    index + 1, r["resource_name"], r["file_last_modified"],
                )
            )

        message_lines.extend(lines)

    return "\n".join(message_lines)


try:
    LOGGER.info(f"Started for: {CKAN.address}")
    with open(DIRS["configs"] / "remote_files.yaml", "r") as f:
        REFRESH_LIST = yaml.load(f, yaml.SafeLoader)
        LOGGER.debug("Loaded remote_files.yaml")

    packages = get_packages_to_sync(REFRESH_LIST)
    LOGGER.debug(f"Retrieved {len(packages)} to sync")

    sync_results = []
    for package in packages:
        name = package["name"]
        LOGGER.debug(name)

        try:
            results = sync_resource_timestamps(package, REFRESH_LIST)

        except Exception:
            LOGGER.error(f"{name}:\n{traceback.format_exc()}")
            results = [{"package_name": name, "result": "error"}]

        sync_results.extend(results)

    notification = {
        "message_type": "success",
        "msg": build_notification_message(sync_results),
    }

    TOOLS.send_notifications(**notification)
    LOGGER.info("Finished")

except Exception:
    error = traceback.format_exc()
    message_content = error

    notification = {
        "message_type": "error",
        "msg": message_content,
    }

    TOOLS.send_notifications(**notification)
    LOGGER.error(error)
