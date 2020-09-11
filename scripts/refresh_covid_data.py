import ckanapi
import pandas as pd
from datetime import datetime
import requests
import os
from pathlib import Path
import traceback
import json
from datetime import datetime, timezone
import logging
import sys

PATH = Path(os.path.dirname(os.path.abspath(__file__)))


def setup_custom_logger(name):
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.FileHandler(PATH / "logs.log", mode="a")
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)

    return logger


LOGGER = setup_custom_logger("covid_refresher")
PACKAGE_NAME = "covid-19-cases-in-toronto"
NEW_DATA_FILE = PATH / "covid19cases.csv"
TMP = PATH / "tmp"
FIELDS_FILE = TMP / "fields.json"
RECORDS_FILE = TMP / "records.parquet"

try:
    ckan = ckanapi.RemoteCKAN()

    LOGGER.info(f"Refreshing COVID data: {ckan.address}")

    package = ckan.action.package_show(id=PACKAGE_NAME)
    LOGGER.info(f"Using Package ID: {PACKAGE_NAME}")

    datastore_resources = [
        r
        for r in package["resources"]
        if r["datastore_active"] or r["url_type"] == "datastore"
    ]

    assert len(datastore_resources) == 1, LOGGER.error(
        f"Expected 1 datastore resource, but instead got {len(datastore_resources)}"
    )

    resource = datastore_resources[0]
    rid = resource["id"]

    current = ckan.action.datastore_search(id=rid, limit=10000000)

    # backup
    fields = [f for f in current["fields"] if f["id"] != "_id"]

    with open(FIELDS_FILE, "w") as f:
        json.dump(fields, f)

    backup_data = pd.DataFrame(current["records"]).drop("_id", axis=1)
    backup_data.to_parquet(RECORDS_FILE)
    LOGGER.info(
        f"Created backup files for fields ({len(fields)}) and records({backup_data.shape[0]}) in {TMP}"
    )

    # read
    data = pd.read_csv(NEW_DATA_FILE)

    data["Episode Date"] = (pd.to_datetime(data["Episode Date"])).dt.strftime(
        "%Y-%m-%d"
    )
    data["Reported Date"] = (pd.to_datetime(data["Reported Date"])).dt.strftime(
        "%Y-%m-%d"
    )
    records = data.sort_values(by="Assigned_ID").to_dict(orient="records")
    LOGGER.info(f"Read new data {data.shape} from {NEW_DATA_FILE}")

    # delete
    delete = ckan.action.datastore_delete(id=rid, filters={})
    LOGGER.info(f"Deleted datastore records")

    # insert
    recreate = ckan.action.datastore_create(id=rid, records=records)
    LOGGER.info(f"Inserted new datastore resources")

    # update timestamp
    utcnow = datetime.now(timezone.utc)
    modified = utcnow.strftime("%Y-%m-%dT%H:%M:%S.%f")
    last_modified = ckan.action.resource_patch(id=rid, last_modified=modified)
    LOGGER.info(
        f"Updated resource last_modified date: from {resource['last_modified']} to {modified}"
    )

    # delete backups
    os.remove(FIELDS_FILE)
    os.remove(RECORDS_FILE)
    LOGGER.info(f"Cleaned up temporary files")
    LOGGER.info(f"Finished refreshing data")

    params = {"type": "success", "message": f"{ckan.address}: {data.shape}"}

except:
    err = traceback.format_exc()

    LOGGER.error(err)
    params = {"type": "error", "message": err}

requests.get(
    "https://wirepusher.com/send",
    {"id": "kjmfmpgjD", "title": "COVID Data Refresh", **params},
)

LOGGER.info(f"Sent notification")
