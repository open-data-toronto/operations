import json
import math
import re
from datetime import datetime as dt

import ckanapi
import nltk
import numpy as np
import pandas as pd
import pytz
import requests
from geopandas import gpd
from nltk.corpus import wordnet
from shapely.geometry import shape
from sklearn.preprocessing import MinMaxScaler
from tqdm import tqdm

nltk.download("wordnet")

EST = pytz.timezone("America/Toronto")

METADATA_FIELDS = ["collection_method", "limitations", "topics", "owner_email"]

TIME_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
    "quarterly": 52 * 7 / 4,
    "semi-annually": 52 * 7 / 2,
    "annually": 365,
}

RESOURCE_MODEL = "scoring-models"
MODEL_VERSION = "v0.1.0"

RESOURCE_SCORES = "catalogue-scorecard"
PACKAGE_DQS = "catalogue-quality-scores"

DIMENSIONS = [
    "usability",
    "metadata",
    "freshness",
    "completeness",
    "accessibility",
]  # Ranked in order

BINS = {
    "Bronze": 0.6,
    "Silver": 0.8,
    "Gold": 1,
}


def run(logger, utils, ckan, configs=None):
    def read_datastore(ckan, rid, rows=10000):
        records = []

        # is_geospatial = False

        has_more = True
        while has_more:
            result = ckan.action.datastore_search(
                id=rid, limit=rows, offset=len(records)
            )

            records += result["records"]
            has_more = len(records) < result["total"]

        df = pd.DataFrame(records).drop("_id", axis=1)

        if "geometry" in df.columns:
            df["geometry"] = df["geometry"].apply(lambda x: shape(json.loads(x)))

            df = gpd.GeoDataFrame(df, crs="epsg:4326")

        return df, [x for x in result["fields"] if x["id"] != "_id"]

    def get_framework(ckan, pid=PACKAGE_DQS):
        try:
            framework = ckan.action.package_show(id=pid)
            logger.info(f"Found package to use for data quality scores: {PACKAGE_DQS}")
        except ckanapi.NotAuthorized:
            raise Exception("Permission required to search for the framework package")
        except ckanapi.NotFound:
            raise Exception("Framework package not found")

        return {r["name"]: r for r in framework.pop("resources")}

    def build_response(code, message=""):
        response = {
            "statusCode": code,
            "headers": {"Access-Control-Allow-Origin": "*"},
        }

        if message:
            response["body"] = message

        return response

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
            "col_names": 0,  # Are the column names easy to understand?
            "col_constant": 1,  # Are there columns where all values are constant?
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

        TODO's:
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

    def calculate_weights(dimensions, method="sr"):
        N = len(dimensions)

        if method == "sr":
            denom = np.array(
                [
                    ((1 / (i + 1)) + ((N + 1 - (i + 1)) / N))
                    for i, x in enumerate(dimensions)
                ]
            ).sum()
            weights = [
                ((1 / (i + 1)) + ((N + 1 - (i + 1)) / N)) / denom
                for i, x in enumerate(dimensions)
            ]
        elif method == "rs":
            denom = np.array(
                [(N + 1 - (i + 1)) for i, x in enumerate(dimensions)]
            ).sum()
            weights = [(N + 1 - (i + 1)) / denom for i, x in enumerate(dimensions)]
        elif method == "rr":
            denom = np.array([1 / (i + 1) for i, x in enumerate(dimensions)]).sum()
            weights = [(1 / (i + 1)) / denom for i, x in enumerate(dimensions)]
        elif method == "re":
            exp = 0.2
            denom = np.array(
                [(N + 1 - (i + 1)) ** exp for i, x in enumerate(dimensions)]
            ).sum()
            weights = [
                (N + 1 - (i + 1)) ** exp / denom for i, x in enumerate(dimensions)
            ]
        else:
            raise Exception("Invalid weighting method provided")

        return weights

    def score_catalogue(event={}, context={}):
        weights = calculate_weights(DIMENSIONS)
        fw = {
            "aggregation_methods": {
                "metrics_to_dimension": "avg",
                "dimensions_to_score": "sum_and_reciprocal",
            },
            "dimensions": [
                {"name": dim, "rank": i + 1, "weights": wgt}
                for i, (dim, wgt) in enumerate(zip(DIMENSIONS, weights))
            ],
            "bins": BINS,
        }

        packages = ckan.action.current_package_list_with_resources(limit=500)

        storage = get_framework(ckan)

        data = []
        for p in tqdm(packages, "Datasets scored"):
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

                logger.info(f"{p['name']}: {r['name']} - {len(content)} records")

        if RESOURCE_MODEL not in storage and ckan.apikey:
            logger.info(
                f" {PACKAGE_DQS}: Creating resource {RESOURCE_MODEL} to keep model"
            )
            r = requests.post(
                "{0}/api/3/action/resource_create".format(ckan.address),
                data={
                    "package_id": PACKAGE_DQS,
                    "name": RESOURCE_MODEL,
                    "format": "json",
                    "is_preview": False,
                    "is_zipped": False,
                },
                headers={"Authorization": ckan.apikey},
                files={"upload": ("{0}.json".format(RESOURCE_MODEL), json.dumps({}))},
            )

            storage[RESOURCE_MODEL] = json.loads(r.content)["result"]

        r = requests.get(
            storage[RESOURCE_MODEL]["url"], headers={"Authorization": ckan.apikey}
        )

        scoring_methods = json.loads(r.content)
        scoring_methods[MODEL_VERSION] = fw

        r = requests.post(
            "{0}/api/3/action/resource_patch".format(ckan.address),
            data={"id": storage[RESOURCE_MODEL]["id"]},
            headers={"Authorization": ckan.apikey},
            files={
                "upload": (
                    "{0}.json".format(RESOURCE_MODEL),
                    json.dumps(scoring_methods),
                )
            },
        )

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

        if not ckan.apikey:
            return df

        if RESOURCE_SCORES not in storage:
            logger.info(
                f" {PACKAGE_DQS}: Creating resource {RESOURCE_SCORES} to keep scores"
            )
            storage[RESOURCE_SCORES] = ckan.action.datastore_create(
                resource={
                    "package_id": PACKAGE_DQS,
                    "name": RESOURCE_SCORES,
                    "format": "csv",
                    "is_preview": True,
                    "is_zipped": True,
                },
                fields=[{"id": x} for x in df.columns.values],
                records=df.to_dict(orient="records"),
            )
        else:
            logger.info(f"Loading results to resource: {RESOURCE_SCORES}")
            ckan.action.datastore_upsert(
                method="insert",
                resource_id=storage[RESOURCE_SCORES]["id"],
                records=df.to_dict(orient="records"),
            )

        # build_response(200, message='')
        return df

    df = score_catalogue()

    return {
        "message_type": "success",
        "msg": f"Data quality scores calculated for {df.shape[0]} packages",
    }
