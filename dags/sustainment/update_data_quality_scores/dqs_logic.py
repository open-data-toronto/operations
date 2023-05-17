import json
import logging
import re
from datetime import datetime as dt

import nltk
import numpy as np
import pandas as pd
from geopandas import gpd
from nltk.corpus import wordnet
from shapely.geometry import shape

nltk.download("wordnet")


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


def parse_col_name(s):
    camel_to_snake = re.sub(
        "([a-z0-9])([A-Z])", r"\1_\2", re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    ).lower()

    return [x for x in re.split(r"-|_|\s", camel_to_snake) if len(x)]


def score_usability(columns, data):
    """
    How easy is it to use the data given how it is organized/structured?

    TODO's:
        * level of nested fields?
        * long vs. wide?
        * if ID columns given, are these ID's common across datasets?
    """

    metrics = {
        "col_names": 1,  # Column names easy to understand?
        "col_constant": 1,  # Columns where all values are constant?
    }
    scores = 1

    for f in columns:
        words = parse_col_name(f["id"])
        eng_words = [w for w in words if len(wordnet.synsets(w))]

        if len(eng_words) / len(words) <= 0.2:
            scores -= 1 / len(columns)

        if not f["id"] == "geometry" and data[f["id"]].nunique() <= 1:
            metrics["col_constant"] -= 1 / len(columns)

    metrics["col_names"] = scores if scores <= 0.5 else 1

    return np.mean(list(metrics.values()))


def score_metadata(package, metadata_fields, columns=None):
    """
    How easy is it to understand the context of the data?

    TODOs:
        * Measure the quality of the metadata as well
    """

    metrics = {
        "desc_dataset": 0,  # Does the metadata describe the dataset well?
    }

    for field in metadata_fields:
        if (
            field in package
            and package[field]
            and not (field == "owner_email" and "opendata" in package[field])
        ):
            metrics["desc_dataset"] += 1 / len(metadata_fields)

    if columns:
        metrics["desc_columns"] = 1  # Does the metadata describe the data well?

        for f in columns:
            if (
                "info" in f
                and (f["info"]["notes"] is not None)
                and (f["info"]["notes"] != "")
            ):
                continue
            else:
                if f["id"] != "geometry":
                    metrics["desc_columns"] -= 1 / len(columns)

        return np.mean(list(metrics.values()))

    return np.mean(list(metrics.values()))


def score_freshness(package, time_map, penalty_map):
    """
    How up to date is the data?
    """

    metrics = {}

    rr = package["refresh_rate"].lower()
    if "last_refreshed" in package and package["last_refreshed"]:
        days = parse_datetime(package["last_refreshed"])

        if rr in ["real-time", "as available", "will not be refreshed"]:
            metrics["elapse_periods"] = 1
        elif rr in time_map:
            elapse_periods = max(0, (days - time_map[rr]) / time_map[rr])
            metrics["elapse_periods"] = max(
                0, 1 - (elapse_periods / penalty_map[rr]) ** 2
            )

        # Decrease the score starting from ~0.5 years to ~2 years
        metrics["elapse_days"] = (
            max(0, 1 - (days / (365 * 2)) ** 2) if days > 180 else 1
        )

        return np.mean(list(metrics.values()))

    else:
        logging.info("Last_refreshed date empty.")
        return 1


def score_completeness(data):
    """
    How much of the data is missing?
    """
    missing_rate = np.sum(len(data) - data.count()) / np.prod(data.shape)

    if missing_rate >= 0.5:
        return 1 - (np.sum(len(data) - data.count()) / np.prod(data.shape))
    else:
        return 1


def score_accessibility(p, resource, dataset_type, etl_intentory):
    """
    Is data available via APIs? Have tags?
    """
    metrics = {}
    metrics["tags"] = 1 if "tags" in p and (p["num_tags"] > 0) else 0.5

    if dataset_type == "filestore":
        if resource["name"] in etl_intentory["resource_name"].values.tolist():
            engine_info = etl_intentory.loc[
                etl_intentory["resource_name"] == resource["name"], "engine"
            ].iloc[0]
        else:
            engine_info = None

        metrics["pipeline"] = 0.8 if engine_info else 0.5

    if dataset_type == "datastore":
        metrics["pipeline"] = (
            1 if "extract_job" in resource and resource["extract_job"] else 0.5
        )

    return np.mean(list(metrics.values()))
