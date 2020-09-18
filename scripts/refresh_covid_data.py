import os
import traceback
from datetime import datetime
from datetime import timezone
from pathlib import Path
from utils import common
from utils import ckan_helper

import ckanapi
import pandas as pd


PATH = Path(os.path.abspath(__file__))
TOOLS = common.Helper(PATH)

LOGGER, CREDS, DIRS = TOOLS.get_all()
CKAN = ckanapi.RemoteCKAN(**CREDS)

PACKAGE_NAME = "covid-19-cases-in-toronto"
NEW_DATA_FILE = "covid19cases.csv"

try:
    LOGGER.info(f"Started for: {CKAN.address}")

    package = CKAN.action.package_show(id=PACKAGE_NAME)
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

    # backup
    backup_results = ckan_helper.backup_datastore_resource(
        ckan=CKAN,
        resource_id=resource["id"],
        dest_path=DIRS["backups"],
        backup_fields=True,
    )

    LOGGER.info(
        "Backed up {columns} columns and {records} records:\n {data}\n {fields}".format(
            **backup_results
        )
    )

    # read new data
    data = pd.read_csv(DIRS["staging"] / NEW_DATA_FILE)

    for date_field in ["Episode Date", "Reported Date"]:
        data[date_field] = (pd.to_datetime(data["Episode Date"])).dt.strftime(
            "%Y-%m-%d"
        )

    records = data.sort_values(by="Assigned_ID").to_dict(orient="records")
    LOGGER.info(
        f"Loaded {NEW_DATA_FILE}: {data.shape[0]} columns,  {data.shape[1]} rows"
    )

    CKAN.action.datastore_delete(id=resource["id"], filters={})
    LOGGER.info("Deleted datastore records")

    CKAN.action.datastore_create(id=resource["id"], records=records)
    LOGGER.info("Inserted new datastore resources")

    # update timestamp
    update_response = ckan_helper.update_resource_last_modified(
        ckan=CKAN,
        resource_id=resource["id"],
        new_last_modified=datetime.now(timezone.utc),
    )

    LOGGER.info(f"Set last_modified timestamp to {update_response['last_modified']}")

    os.remove(DIRS["staging"] / NEW_DATA_FILE)

    notification = {
        "message_type": "success",
        "msg": f"COVID data refreshed: {data.shape[1]} records",
    }

    LOGGER.info("Finished")

except Exception:
    err = traceback.format_exc()
    LOGGER.error(err)
    notification = {
        "message_type": "error",
        "msg": err,
    }

TOOLS.send_notifications(**notification)
