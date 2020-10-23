import sys

sys.path.append("../..")

import os
import traceback
from pathlib import Path

import ckanapi
from utils import ckan_helper
from utils import common
from datetime import datetime as dt
from datetime import timedelta

PATH = Path(os.path.abspath(__file__))
TOOLS = common.Helper(PATH)

LOGGER, CREDS, DIRS = TOOLS.get_all()
CKAN = ckanapi.RemoteCKAN(**CREDS)

TIME_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
    "quarterly": 52 * 7 / 4,
    "semi-annually": 52 * 7 / 2,
    "annually": 365,
}

try:
    LOGGER.info("Started")
    packages = ckan_helper.get_all_packages(CKAN)

    run_time = dt.utcnow()

    packages_behind = []

    LOGGER.info("Looping through packages")
    for p in packages:
        refresh_rate = p.pop("refresh_rate").lower()

        if refresh_rate not in TIME_MAP.keys() or p["is_retired"]:
            continue

        last_refreshed = p.pop("last_refreshed")
        last_refreshed = dt.strptime(last_refreshed, "%Y-%m-%dT%H:%M:%S.%f")

        days_behind = (run_time - last_refreshed).days
        next_refreshed = last_refreshed + timedelta(days=TIME_MAP[refresh_rate])

        if days_behind > TIME_MAP[refresh_rate]:
            details = {
                "name": p["name"],
                "email": p["owner_email"],
                "publisher": p["owner_division"],
                "rate": refresh_rate,
                "last": last_refreshed.strftime("%Y-%m-%d"),
                "next": next_refreshed.strftime("%Y-%m-%d"),
                "days": days_behind,
            }
            packages_behind.append(details)
            LOGGER.debug(
                f"{details['name']}: Behind {details['days']} days. {details['email']}"
            )

    lines = [
        "_{name}_ ({rate}): refreshed `{last}`. Behind *{days} days*. {email}".format(
            **p
        )
        for p in sorted(packages_behind, key=lambda x: -x["days"])
    ]

    lines = [f"{i+1}. {l}" for i, l in enumerate(lines)]

    notification = {
        "message_type": "success",
        "msg": "\n".join(lines),
    }

    TOOLS.send_notifications(**notification)

except Exception:
    error = traceback.format_exc()
    notification = {
        "message_type": "error",
        "msg": error,
    }

    TOOLS.send_notifications(**notification)
    LOGGER.error(error)

LOGGER.info("Finished")
