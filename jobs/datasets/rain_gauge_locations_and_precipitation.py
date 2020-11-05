import os
import tempfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
from dateutil import parser
from copy import deepcopy

PATH = Path(os.path.abspath(__file__))
PACKAGE_ID = PATH.name.replace("_", "-")[:-3]


def run(logger, utils, ckan, configs):
    apikey = configs[PATH.parent.name][PATH.name[:-3]]["credentials"]

    def api_request(path, key):
        logger.info(f"API call: {path}")

        req = requests.get(
            f"https://developers.flowworks.com/fwapi/v1/{apikey}/{path}"
        ).json()
        assert "request ok" in req["msg"].lower(), f"{path}: {req['msg']}"

        return req[key]

    def get_latest_resource(package):
        resources = [r for r in package["resources"] if r["format"].lower() == "csv"]

        resource_years = []
        for r in resources:
            year = "".join([s for s in r["name"] if s.isdigit()])
            resource_years.append(int(year))

        resource = [r for r in resources if str(max(resource_years)) in r["name"]][0]

        return resource

    def get_datapoints(site, start_date, to_date):
        site = deepcopy(site)
        del site["channels"]
        channels = api_request(f"site/{site['id']}", "sites")[0]["channels"]
        rainfall_channels = [c for c in channels if c["name"].lower() == "rainfall"]

        assert len(rainfall_channels) == 1, "{} rainfall channels".format(
            len(rainfall_channels)
        )
        channel = rainfall_channels[0]

        datapoints = api_request(
            "site/{}/channel/{}/data/startdate/{}/todate/{}".format(
                site["id"], channel["id"], start_date, to_date
            ),
            "datapoints",
        )
        logger.info(
            "Site: {} / Channel: {} / Records: {}".format(
                site["name"], channel["id"], len(datapoints)
            )
        )
        for d in datapoints:
            d.update(site)
            d["rainfall"] = d.pop("value")

        return datapoints

    package = ckan.action.package_show(id=PACKAGE_ID)

    resource = get_latest_resource(package)
    logger.info(f"using resource: {resource['name']}")

    file_content = requests.get(resource["url"]).content
    resource_data = pd.read_csv(BytesIO(file_content))

    time_lastest_loaded = parser.parse(resource_data["date"].max())
    time_now = datetime.now()
    logger.info(f"latest record is from {time_lastest_loaded}")

    start_date = (time_lastest_loaded + timedelta(seconds=1)).strftime("%Y%m%d%H%M%S")
    to_date = time_now.strftime("%Y%m%d%H%M%S")

    rain_gauge_sites = api_request("sites", "sites")
    records_to_load = []
    for site in rain_gauge_sites:
        records_to_load.extend(
            get_datapoints(site=site, start_date=start_date, to_date=to_date)
        )

    if len(records_to_load) == 0:
        logger.info("No new records found")
        return {"message_type": "success", "msg": "No new records to load"}

    years_to_load = [str(x) for x in range(time_lastest_loaded.year, time_now.year + 1)]
    data_to_load = resource_data.append(pd.DataFrame(records_to_load))

    logger.info(f"{len(records_to_load)} new records found")
    for year in years_to_load:
        year_data = data_to_load[data_to_load["date"].str.startswith(year)]
        resource_name = f"precipitation-data-{year}"

        resource_id = None
        for r in package["resources"]:
            if r["name"] == resource_name:
                resource_id = r["id"]
                break

        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            path = folder / f"{resource_name}.csv"
            year_data.to_csv(path, index=False)

            if resource_id is None:
                logger.info(f"resource does not exist for {year}")
                new_resource = ckan.action.resource_create(
                    package_id=package["id"],
                    name=resource_name,
                    format="csv",
                    upload=open(path, "rb"),
                )
                logger.info(
                    f"created {new_resource['name']} with {data_to_load.shape[0]} rows"
                )
            else:
                logger.info("resource for the year exists")
                ckan.action.resource_patch(
                    id=resource_id,
                    upload=open(path, "rb"),
                )
                logger.info(f"updated resource: {r['name']}")

    return {
        "message_type": "success",
        "msg": f"{len(records_to_load)} new records found since {time_lastest_loaded}",
    }
