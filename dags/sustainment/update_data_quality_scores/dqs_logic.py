import json
import math
import re
from datetime import datetime as dt
import logging

import nltk
import numpy as np
import pandas as pd
from geopandas import gpd
from nltk.corpus import wordnet
from shapely.geometry import shape
from sklearn.preprocessing import MinMaxScaler

nltk.download("wordnet")


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
        df["geometry"] = df["geometry"].apply(lambda x: shape(json.loads(x)))

        df = gpd.GeoDataFrame(df, crs="epsg:4326")

    return df, [x for x in result["fields"] if x["id"] != "_id"]


def calculate_weights(method="sr", **kwargs):
    dims = kwargs.pop("dimensions")
    N = len(dims)

    if method == "sr":
        denom = np.array(
            [((1 / (i + 1)) + ((N + 1 - (i + 1)) / N)) for i, x in enumerate(dims)]
        ).sum()
        weights = [
            ((1 / (i + 1)) + ((N + 1 - (i + 1)) / N)) / denom
            for i, x in enumerate(dims)
        ]
    elif method == "rs":
        denom = np.array([(N + 1 - (i + 1)) for i, x in enumerate(dims)]).sum()
        weights = [(N + 1 - (i + 1)) / denom for i, x in enumerate(dims)]
    elif method == "rr":
        denom = np.array([1 / (i + 1) for i, x in enumerate(dims)]).sum()
        weights = [(1 / (i + 1)) / denom for i, x in enumerate(dims)]
    elif method == "re":
        exp = 0.2
        denom = np.array([(N + 1 - (i + 1)) ** exp for i, x in enumerate(dims)]).sum()
        weights = [(N + 1 - (i + 1)) ** exp / denom for i, x in enumerate(dims)]
    else:
        raise Exception("Invalid weighting method provided")

    return weights


def prepare_and_normalize_scores(**kwargs):
    ti = kwargs.pop("ti")
    data = ti.xcom_pull(task_ids="score_catalogue")
    weights = ti.xcom_pull(task_ids="calculate_weights")
    BINS = kwargs.pop("BINS")
    MODEL_VERSION = kwargs.pop("MODEL_VERSION")
    DIMENSIONS = kwargs.pop("DIMENSIONS")

    df = pd.DataFrame(data).set_index(["package", "resource"])

    scores = pd.DataFrame([weights] * len(df.index))
    scores.index = df.index
    scores.columns = DIMENSIONS

    scores = df.multiply(scores)

    df["score"] = scores.sum(axis=1)
    df["score_norm"] = MinMaxScaler().fit_transform(df[["score"]])

    df = df.groupby("package").mean()

    labels = list(BINS.keys())

    bins = [-1]
    bins.extend(BINS.values())

    df["grade"] = pd.cut(df["score_norm"], bins=bins, labels=labels)
    df["grade_norm"] = pd.cut(df["score_norm"], bins=bins, labels=labels)

    df["recorded_at"] = dt.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    df["version"] = MODEL_VERSION

    df = df.reset_index()
    df = df.round(2)

    return df


def score_catalogue(**kwargs):
    packages = kwargs.pop("ti").xcom_pull(task_ids="get_packages")
    METADATA_FIELDS = kwargs.pop("METADATA_FIELDS")
    TIME_MAP = kwargs.pop("TIME_MAP")
    ckan = kwargs.pop("ckan")

    def score_usability(columns, data):
        """
        How easy is it to use the data given how it is organized/structured?

        TODO's:
            * level of nested fields?
            * long vs. wide?
            * if ID columns given, are these ID's common across datasets?
        """

        def parse_col_name(s):
            camel_to_snake = re.sub(
                "([a-z0-9])([A-Z])", r"\1_\2", re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
            ).lower()

            return (
                camel_to_snake == s,
                [x for x in re.split(r"-|_|\s", camel_to_snake) if len(x)],
            )

        metrics = {
            "col_names": 0,  # Column names easy to understand?
            "col_constant": 1,  # Columns where all values are constant?
        }

        for f in columns:
            is_camel, words = parse_col_name(f["id"])
            eng_words = [w for w in words if len(wordnet.synsets(w))]

            if len(eng_words) / len(words) > 0.8:
                metrics["col_names"] += (1 if not is_camel else 0.5) / len(columns)

            if not f["id"] == "geometry" and data[f["id"]].nunique() <= 1:
                metrics["col_constant"] -= 1 / len(columns)

        if isinstance(data, gpd.GeoDataFrame):
            counts = data["geometry"].is_valid.value_counts()

            metrics["geo_validity"] = (
                1 - (counts[False] / (len(data) * 0.05)) if False in counts else 1
            )

        return np.mean(list(metrics.values()))

    def score_metadata(package, columns):
        """
        How easy is it to understand the context of the data?

        TODOs:
            * Measure the quality of the metadata as well
        """

        metrics = {
            "desc_dataset": 0,  # Does the metadata describe the dataset well?
            "desc_columns": 0,  # Does the metadata describe the data well?
        }

        for field in METADATA_FIELDS:
            if (
                field in package
                and package[field]
                and not (field == "owner_email" and "opendata" in package[field])
            ):
                metrics["desc_dataset"] += 1 / len(METADATA_FIELDS)

        for f in columns:
            if (
                "info" in f
                and len(f["info"]["notes"])
                and f["info"]["notes"].strip() != f["id"]
            ):
                metrics["desc_columns"] += 1 / len(columns)

        return np.mean(list(metrics.values()))

    def score_freshness(package):
        """
        How up to date is the data?
        """

        metrics = {}

        rr = package["refresh_rate"].lower()

        if rr == "real-time":
            return 1
        elif (
            rr in TIME_MAP and "last_refreshed" in package and package["last_refreshed"]
        ):
            days = (
                dt.utcnow()
                - dt.strptime(package["last_refreshed"], "%Y-%m-%dT%H:%M:%S.%f")
            ).days

            # Greater than 2 periods have a score of 0
            metrics["elapse_periods"] = max(0, 1 - math.floor(days / TIME_MAP[rr]) / 2)

            # Decrease the score starting from ~0.5 years to ~3 years
            metrics["elapse_days"] = 1 - (1 / (1 + np.exp(4 * (2.25 - days / 365))))

            return np.mean(list(metrics.values()))

        return 0

    def score_completeness(data):
        """
        How much of the data is missing?
        """
        return 1 - (np.sum(len(data) - data.count()) / np.prod(data.shape))

    def score_accessibility(resource):
        """
        Is data available via APIs?
        """
        return 1 if "extract_job" in resource and resource["extract_job"] else 0.5

    data = []
    for p in packages:
        for r in p["resources"]:
            if "datastore_active" not in r or not r["datastore_active"]:
                continue

            content, fields = read_datastore(ckan, r["id"])

            records = {
                "package": p["name"],
                "resource": r["name"],
                "usability": score_usability(fields, content),
                "metadata": score_metadata(p, fields),
                "freshness": score_freshness(p),
                "completeness": score_completeness(content),
                "accessibility": score_accessibility(r),
            }

            data.append(records)

            logging.info(f"{p['name']}: {r['name']} - {len(content)} records")

    return data
