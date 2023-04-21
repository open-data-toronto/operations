import json
import logging
import math
import re
from datetime import datetime as dt
from pathlib import Path

import nltk
import numpy as np
import pandas as pd
from geopandas import gpd
from nltk.corpus import wordnet
from shapely.geometry import shape
from sklearn.preprocessing import MinMaxScaler
from airflow.models import Variable
import os

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


def calculate_model_weights(method="sr", **kwargs):
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
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    raw_scores_path = Path(ti.xcom_pull(task_ids="score_catalogue"))
    weights = ti.xcom_pull(task_ids="calculate_model_weights")
    BINS = kwargs.pop("BINS")
    MODEL_VERSION = kwargs.pop("MODEL_VERSION")
    DIMENSIONS = kwargs.pop("DIMENSIONS")

    df = pd.read_parquet(raw_scores_path).set_index(["package", "resource"], drop=True)

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

    filename = "final_scores"
    filepath = tmp_dir / f"{filename}.parquet"

    df.to_parquet(filepath, engine="fastparquet", compression=None)

    return str(filepath)


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
        metrics["desc_columns"] = 0  # Does the metadata describe the data well?

        for f in columns:
            if (
                "info" in f
                and (f["info"]["notes"] is not None)
                and f["info"]["notes"].strip() != f["id"]
            ):
                metrics["desc_columns"] += 1 / len(columns)

    return np.mean(list(metrics.values()))


def score_freshness(package, time_map, penalty_map):
    """
    How up to date is the data?
    """

    metrics = {}

    rr = package["refresh_rate"].lower()

    if rr in ["real-time", "as available"]:
        return 1
    elif rr in time_map and "last_refreshed" in package and package["last_refreshed"]:
        days = parse_datetime(package["last_refreshed"])
     
        elapse_periods = max(0, (days - time_map[rr]) / time_map[rr])
        metrics["elapse_periods"] = max(
            0, 1 - elapse_periods / penalty_map[rr]
        )
        logging.info(metrics["elapse_periods"])
        
        # Decrease the score starting from ~0.5 years to ~3 years
        metrics["elapse_days"] = 1 - (1 / (1 + np.exp(4 * (2.25 - days / 365))))

        return np.mean(list(metrics.values()))

    return 0


def score_completeness(data):
    """
    How much of the data is missing?
    """
    missing_rate = (np.sum(len(data) - data.count()) / np.prod(data.shape))
    
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

        metrics["pipeline"] = 0.6 if engine_info else 0.2

    if dataset_type == "datastore":
        metrics["pipeline"] = 1

    return np.mean(list(metrics.values()))


def score_catalogue(**kwargs):
    ti = kwargs.pop("ti")
    packages = ti.xcom_pull(task_ids="get_all_packages")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    METADATA_FIELDS = kwargs.pop("METADATA_FIELDS")
    TIME_MAP = kwargs.pop("TIME_MAP")

    ckan = kwargs.pop("ckan")

    filename = "raw_scores"
    filepath = tmp_dir / f"{filename}.parquet"

    filestore_records = []
    datastore_records = []
    etl_intentory = get_etl_inventory("od-etl-configs", ckan)

    for p in packages:
        logging.info(f"---------Package: {p['name']}")
        if p["name"].lower() == "tags":
            continue
        # skip retired dataset for evaluation
        if "is_retired" in p and (str(p.get("is_retired")).lower() == "true"):
            logging.info(f"Dataset {p['name']} is retired.")
            continue

        # filestore score
        if p["dataset_category"] in ["Document", "Website"]:
            for r in p["resources"]:
                records = {
                    "package": p["name"],
                    "resource": r["name"],
                    "usability": 0,
                    "metadata": score_metadata(p, METADATA_FIELDS),
                    "freshness": score_freshness(p, TIME_MAP, PENALTY_MAP),
                    "completeness": 0,
                    "accessibility": score_accessibility(
                        p, r, "filestore", etl_intentory
                    ),
                }
                logging.info(f"Filestore Score: Package Name {p['name']}")
                filestore_records.append(records)
                logging.info(records)

        # datastore score
        else:
            for r in p["resources"]:
                if (
                    "datastore_active" not in r
                    or str(r["datastore_active"]).lower() == "false"
                ):
                    continue

                content, fields = read_datastore(ckan, r["id"])

                records = {
                    "package": p["name"],
                    "resource": r["name"],
                    "usability": score_usability(fields, content),
                    "metadata": score_metadata(p, METADATA_FIELDS, fields),
                    "freshness": score_freshness(p, TIME_MAP),
                    "completeness": score_completeness(content),
                    "accessibility": score_accessibility(
                        p, r, "datastore", etl_intentory
                    ),
                }

                logging.info(
                    f"Datastore Score {p['name']}: {r['name']} - {len(content)} records"
                )
                logging.info(records)

                datastore_records.append(records)

    df_filestore = pd.DataFrame(filestore_records)
    df_datastore = pd.DataFrame(datastore_records)
    logging.info(df_filestore.shape[0])
    logging.info(df_datastore.shape[0])

    # assign mean value of datastore for filestore dimension "usability" and "completeness"
    df_filestore["usability"] = [df_datastore["usability"].mean()] * df_filestore.shape[
        0
    ]
    df_filestore["completeness"] = [
        df_datastore["completeness"].mean()
    ] * df_filestore.shape[0]

    
    df = df_datastore.append(df_filestore, ignore_index=True)

    df.to_parquet(filepath, engine="fastparquet", compression=None)

    return str(filepath)
