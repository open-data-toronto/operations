import json
import logging
import sys
import traceback
from datetime import datetime
from datetime import time
from datetime import timedelta
from datetime import timezone
from time import sleep
from typing import List

import ckanapi
import pytz
import requests
from dateutil import parser


def setup_custom_logger(name):
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.FileHandler("./logs.log", mode="a")
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)

    return logger


def patch_package_last_refreshed(
    ckan: ckanapi.remoteckan.RemoteCKAN, package_id: str, files: List[str]
):
    utcnow = datetime.now(timezone.utc)

    package = ckan.action.package_show(id=package_id)

    last_refreshed = pytz.utc.localize(parser.parse(package["last_refreshed"])).replace(
        microsecond=0
    )
    days_since_last_refreshed = (utcnow - last_refreshed).days

    if days_since_last_refreshed <= 0:
        LOGGER.info(f"{package_id}: last refreshed is up to date. Continuing.")
        return

    LOGGER.info(f"{package_id}: last refreshed {days_since_last_refreshed} days ago")
    resources = [r for r in package["resources"] if all([r["url"], not r["url_type"]])]

    last_modified_times = []
    resource_dates = []

    for f in files:
        try:
            assert f in [r["url"] for r in resources]
        except AssertionError:
            LOGGER.error(
                f"{package_id}: URL of expected file not in CKAN resources: {f}. Skipping package."
            )
            return

        resource = [r for r in resources if r["url"] == f][0]

        res = requests.head(f)

        resource_dates.append(
            {
                "id": resource["id"],
                "name": resource["name"],
                "last_modified": parser.parse(res.headers["Last-Modified"]),
            }
        )

        last_modified = parser.parse(res.headers["Last-Modified"])

    last_modified_dates = [r["last_modified"] for r in resource_dates]
    max_last_modified = max(last_modified_dates)

    if (max_last_modified - last_refreshed).days == 0:
        LOGGER.info(
            "{}: last_refreshed matches latest last_modified date: {} (UTC). Nothing to refresh.".format(
                package_id, last_modified
            )
        )
        return

    for r in resource_dates:
        ckan.action.resource_patch(
            id=r["id"], last_modified=r["last_modified"].strftime("%Y-%m-%dT%H:%M:%S")
        )
        LOGGER.info(
            "{}. Last modified set to {} (UTC)".format(resource["name"], last_modified)
        )


LOGGER = setup_custom_logger("refresher")

EST = pytz.timezone("America/Toronto")

PROD = ckanapi.RemoteCKAN()

try:
    with open("files.json", "r") as f:
        REFRESH_LIST = json.load(f)

    LOGGER.info(f"##### Checking last_refreshed date #####")
    for item in REFRESH_LIST:
        patch_package_last_refreshed(ckan=PROD, **item)
    LOGGER.info(f"Finished.")

    params = {
        "type": "success",
        "message": "\n".join([p["package_id"] for p in REFRESH_LIST]),
    }

except:
    error = traceback.format_exc()
    params = {
        "type": "error",
        "message": error,
    }
    LOGGER.error(error)

requests.get(
    "https://wirepusher.com/send", {"id": "kjmfmpgjD", "title": "Refresher", **params}
)
