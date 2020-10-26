import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


PATH = Path(os.path.abspath(__file__))
PACKAGE_ID = PATH.name.replace("_", "-")[:-3]


def run(logger, utils, ckan, configs):
    print(PACKAGE_ID)
    file_name = configs[PATH.parent.name][PATH.name[:-3]]["file_name"]
    directories = configs["directories"]
    package = ckan.action.package_show(id=PACKAGE_ID)
    logger.debug(f"using Package: {PACKAGE_ID}")

    datastore_resources = [
        r
        for r in package["resources"]
        if r["datastore_active"] or r["url_type"] == "datastore"
    ]

    assert len(datastore_resources) == 1, logger.error(
        f"Expected 1 datastore resource, but instead got {len(datastore_resources)}"
    )

    resource = datastore_resources[0]

    backup_results = utils.backup_datastore_resource(
        ckan=ckan,
        resource_id=resource["id"],
        dest_path=directories["backups"],
        backup_fields=True,
    )

    logger.info(
        "Backed up {columns} columns and {records} records:\n {data}\n {fields}".format(
            **backup_results
        )
    )

    data = pd.read_csv(directories["staging"] / file_name)

    for date_field in ["Episode Date", "Reported Date"]:
        data[date_field] = (pd.to_datetime(data[date_field])).dt.strftime("%Y-%m-%d")

    records = data.sort_values(by="Assigned_ID").to_dict(orient="records")
    logger.info(f"Loaded {file_name}: {data.shape[0]} columns,  {data.shape[1]} rows")

    ckan.action.datastore_delete(id=resource["id"], filters={})
    logger.info("Deleted datastore records")

    ckan.action.datastore_create(id=resource["id"], records=records)
    logger.info("Inserted new datastore resources")

    update_response = utils.update_resource_last_modified(
        ckan=ckan,
        resource_id=resource["id"],
        new_last_modified=datetime.now(timezone.utc),
    )

    logger.info(f"Set last_modified timestamp to {update_response['last_modified']}")

    os.remove(directories["staging"] / file_name)

    return {
        "message_type": "success",
        "msg": "COVID data refreshed: from {} to {} records".format(
            backup_results["records"], data.shape[0]
        ),
    }
