import json
import re
import requests
import pandas as pd
import logging
from geopandas import gpd
from datetime import datetime as dt
from shapely.geometry import shape


def parse_datetime(input):
    for format in [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]:
        try:
            delta = dt.strptime(input, format)
            days = (dt.utcnow() - delta).days
            return days
        except ValueError:
            pass


def parse_col_name(s):
    camel_to_snake = re.sub(
        "([a-z0-9])([A-Z])", r"\1_\2", re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    ).lower()

    return [x for x in re.split(r"-|_|\s", camel_to_snake) if len(x)]


def get_etl_inventory(package_name, ckan):
    etl_intentory = ckan.action.package_show(id=package_name)
    rid = etl_intentory["resources"][0]["id"]

    has_more = True
    records = []
    while has_more:
        result = ckan.action.datastore_search(id=rid, limit=5000, offset=len(records))

        records += result["records"]
        has_more = len(records) < result["total"]

    df = pd.DataFrame(records).drop("_id", axis=1)

    return df


def information_url_checker(information_url):
    result_info = ""
    if "http" in information_url:
        call = information_url
    else:
        call = "https://" + information_url

    # record problematic url
    try:
        response = requests.get(call)
        status_code = response.status_code

        if status_code == 404:
            result_info = "~bad_info_url"

    except requests.ConnectionError:
        result_info = "~bad_info_url"

    return result_info


def attribue_description_check(columns):
    counter = 0
    missing_cols = []
    for f in columns:
        if (
            "info" in f
            and (f["info"]["notes"] is not None)
            and (f["info"]["notes"] != "")
        ):
            counter += 1
        else:
            if f["id"] != "geometry":
                missing_cols.append(f["id"])

    return counter == 0, missing_cols


def read_datastore(ckan, rid, dir_path, rows=20000):
    import csv

    # get column names
    result = ckan.action.datastore_search(id=rid, limit=rows)
    column_names = [x["id"] for x in result["fields"]]

    data_path = str(dir_path) + "/data.csv"
    with open(data_path, "w", newline="") as f:
        counter = 0
        dict_writer = csv.DictWriter(f, column_names)
        dict_writer.writeheader()
        has_more = True

        # read and write the data in batch of 20000
        while has_more:
            result = ckan.action.datastore_search(id=rid, limit=rows, offset=counter)
            counter += len(result["records"])
            has_more = counter < result["total"]
            dict_writer.writerows(result["records"])
    logging.info("------------------------- Records read and saved...")

    return data_path, [x for x in result["fields"] if x["id"] != "_id"]

def force_run_all(**kwargs):
    # Get force run from airflow dag configuration
    if "force_run" in kwargs["dag_run"].conf.keys():
        force_run_all = True
    else:
        force_run_all = False

    return force_run_all


def get_run_schedule(**kwargs):
    """
    For daily refreshed dataset, generate data quality code and scores daily;
    For other refresh_rate dataset, generate data quality code weekly.
    Run every Sunday (2023.07.26)
    """
    ti = kwargs.pop("ti")
    check_force_run = ti.xcom_pull(task_ids="check_force_run")

    day_of_week = 2 if check_force_run else dt.now().isoweekday()
    logging.info(f"day {day_of_week} of the week")

    if day_of_week == 2:
        return "run_all_dataset"
    else:
        return "run_daily_dataset"
