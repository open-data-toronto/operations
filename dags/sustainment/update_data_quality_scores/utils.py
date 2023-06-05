import json
import re
import requests
import pandas as pd
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


def read_datastore(ckan, rid, rows=10000):
    records = []

    # is_geospatial = False

    has_more = True
    while has_more:
        result = ckan.action.datastore_search(id=rid, limit=rows, offset=len(records))

        records += result["records"]
        has_more = len(records) < result["total"]

    df = pd.DataFrame(records).drop("_id", axis=1)

    if "geometry" in df.columns:
        df["geometry"] = df["geometry"].apply(
            lambda x: shape(json.loads(x)) if x != "None" else None
        )
        df = gpd.GeoDataFrame(df, crs="epsg:4326")

    return df, [x for x in result["fields"] if x["id"] != "_id"]
