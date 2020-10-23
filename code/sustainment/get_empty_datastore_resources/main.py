import sys

sys.path.append("../..")
import os
import traceback
from pathlib import Path

import ckanapi
from utils import ckan_helper
from utils import common

PATH = Path(os.path.abspath(__file__))
TOOLS = common.Helper(PATH)

LOGGER, CREDS, DIRS = TOOLS.get_all()
CKAN = ckanapi.RemoteCKAN(**CREDS)


def pprint_2d_list(matrix):
    s = [[str(e) for e in row] for row in matrix]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = "\t".join("{{:{}}}".format(x) for x in lens)
    table = [fmt.format(*row) for row in s]
    return "\n".join(table)


def check():
    packages = ckan_helper.get_all_packages(CKAN)
    LOGGER.info(f"Retrieved {len(packages)} packages")
    datastore_resources = []

    LOGGER.info("Identifying datastore resources")
    for package in packages:
        for resource in package["resources"]:
            if resource["url_type"] != "datastore":
                continue
            response = CKAN.action.datastore_search(id=resource["id"], limit=0)

            datastore_resources.append(
                {
                    "package_id": package["title"],
                    "resource_id": resource["id"],
                    "resource_name": resource["name"],
                    "extract_job": resource["extract_job"],
                    "row_count": response["total"],
                    "fields": response["fields"],
                }
            )

            LOGGER.debug(
                f'{package["name"]}: {resource["name"]} - {response["total"]} records'
            )

    LOGGER.info(f"Identified {len(datastore_resources)} datastore resources")

    empties = [r for r in datastore_resources if r["row_count"] == 0]

    if not len(empties):
        return False

    empties = sorted(empties, key=lambda i: i["package_id"])

    matrix = [["#", "PACKAGE", "EXTRACT_JOB"]]
    for i, r in enumerate(empties):
        string = [f"{i+1}."]
        string.extend([r[f] for f in ["package_id", "extract_job"] if r[f]])
        matrix.append(string)

    return pprint_2d_list(matrix)


try:
    LOGGER.info(f"Started for: {CKAN.address}")
    empties = check()

    if not empties:
        LOGGER.info("No empty resources found")
        notification = {"message_type": "success", "msg": "No empties"}
    else:
        LOGGER.warning(f"Empty resources found:\n```\n{empties}\n```")

        notification = {
            "message_type": "warning",
            "msg": f"""EMPTIES FOUND:
```
{empties}
```""",
        }

    LOGGER.info("Finished")

except Exception:
    error = traceback.format_exc()
    message_content = error
    LOGGER.error(error)
    notification = {
        "message_type": "error",
        "msg": message_content,
    }

TOOLS.send_notifications(**notification)
