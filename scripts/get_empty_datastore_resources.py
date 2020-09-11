import requests
import logging
import sys
import traceback


def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s %(name)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler('./logs.log', mode='a')
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return logger

def pprint_2d_list(matrix):
    s = [[str(e) for e in row] for row in matrix]
    lens = [max(map(len, col)) for col in zip(*s)]
    fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
    table = [fmt.format(*row) for row in s]
    return '\n'.join(table)

def check():
    LOGGER = setup_custom_logger("datastore_checker")

    url = "https://ckanadmin0.intra.prod-toronto.ca/api/3/action"

    package_list = requests.get(f"{url}/package_list").json()["result"]

    packages = requests.get(
        f"{url}/package_search", params={"rows": len(package_list)}
    ).json()["result"]["results"]

    datastore_resources = []

    LOGGER.info(f"Getting row count for datastore resources")

    for p in packages:
        for r in p["resources"]:
            if r["url_type"] != "datastore":
                continue
    #         print(p["title"], "|||", r["name"] , "|||", r["id"])
            response = requests.get(
                f"{url}/datastore_search?", params={"id": r["id"], "limit": 0}
            ).json()["result"]

            datastore_resources.append(
                {
                    "package_id": p["title"],
                    "resource_id": r["id"],
                    "resource_name": r["name"],
                    "extract_job": r["extract_job"],
                    "row_count": response["total"],
                    "fields": response["fields"]
                }
            )

    LOGGER.info(f"TOTAL datastore resources found: {len(datastore_resources)}")

    empties = [r for r in datastore_resources if r["row_count"] == 0]
    
    if not len(empties):
        logger.info("No empty resources found")
        return False
    
    empties = sorted(empties, key = lambda i: i["package_id"])
    
    matrix = [["#", "PACKAGE", "EXTRACT_JOB"]]
    for i, r in enumerate(empties):
        string = [f"{i+1}."]
        string.extend([
            r[f]
            for f in ["package_id", "extract_job"]
            if r[f]
        ])
        matrix.append(string)

    if len(empties):
        LOGGER.error(f"Empty resources found: {len(empties)}\n```\n{pprint_2d_list(matrix)}\n```")

    return pprint_2d_list(matrix)


try:
    empties = check()

    if empties:
        params = {
            "type": "warning",
            "message": f"""EMPTIES FOUND:
```
{empties}
```"""
    }

    else:
        params = {
            "type": "success",
            "message": "No empties"
            }
except:
    params = {
        "type": "error",
        "message": traceback.format_exc()
    }

requests.get("https://wirepusher.com/send", {
    "id": "kjmfmpgjD",
    "title": "Datastore Checker",
    **params
})