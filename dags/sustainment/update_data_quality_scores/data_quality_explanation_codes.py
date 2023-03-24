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
import ckanapi

nltk.download("wordnet")

ACTIVE_ENV = Variable.get("active_env")
CKAN_CREDS = Variable.get("ckan_credentials_secret", deserialize_json=True)
CKAN = ckanapi.RemoteCKAN(**CKAN_CREDS[ACTIVE_ENV])

DIR_PATH = Path(os.path.dirname(os.path.realpath(__file__))).parent
SCORES_PATH = DIR_PATH / "update_data_quality_scores"

def explanation_code_catalogue(**kwargs):
    ti = kwargs.pop("ti")
    packages = ti.xcom_pull(task_ids="get_all_packages") # can break package list into segments if needed
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    METADATA_FIELDS = kwargs.pop("METADATA_FIELDS")
    TIME_MAP = kwargs.pop("TIME_MAP")

    ckan = kwargs.pop("ckan")

    filename = "raw_scores"
    filepath = tmp_dir / f"{filename}.parquet"

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
        
     def score_metadata(package, columns = None):
        """
        How easy is it to understand the context of the data?

        TODOs:
            * Measure the quality of the metadata as well
        """

        metrics = {
            "desc_dataset": 0,  # Does the metadata describe the dataset well?
        }

        for field in METADATA_FIELDS:
            if (
                field in package
                and package[field]
                and not (field == "owner_email" and "opendata" in package[field])
            ):
                metrics["desc_dataset"] += 1 / len(METADATA_FIELDS)
        
        # For datastore only
        if columns:
            metrics["desc_columns"] = 0 # Does the metadata describe the data well?
        
            for f in columns:
                if (
                    "info" in f
                    and (f["info"]["notes"] is not None)
                    and f["info"]["notes"].strip() != f["id"]
                ):
                    metrics["desc_columns"] += 1 / len(columns)

        return np.mean(list(metrics.values()))
    
    def metadata_explanation_code(package):
        output = []
        for field in METADATA_FIELDS:
            if field not in package pr field is None:
                output.append(field)
        return ",".join(output)

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
            days = parse_datetime( package["last_refreshed"] )
    
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

    def score_accessibility(p, resource, dataset_type):
        """
        Is data available via APIs?
        """
        metrics = {}
        metrics["tags"] = 1 if "tags" in p and (p["num_tags"] > 0) else 0.5
        
        if dataset_type == "filestore":
            if resource["name"] in etl_intentory["resource_name"].values.tolist():
                engine_info = etl_intentory.loc[etl_intentory['resource_name'] == resource["name"], 'engine'].iloc[0]  
            else:
                engine_info = None
            logging.info(f"{resource['name']}, Engine Info: {engine_info}")

            metrics["pipeline"] = 1 if engine_info else 0.5
            
        if dataset_type == "datastore":
            metrics["pipeline"] = 1
        
        return np.mean(list(metrics.values()))

    data = []
    etl_intentory = get_etl_inventory("od-etl-configs")
    
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
                "metadata": score_filestore_metadata(p),
                "freshness": score_freshness(p),
                "completeness": 0,
                "accessibility": score_accessibility(p, r, "filestore"),
                }
                logging.info(f"Filestore Score: Package Name {p['name']}")
                data.append(records)
                logging.info(records)
        else:
            for r in p["resources"]:
                if "datastore_active" not in r or str(r["datastore_active"]).lower() == 'false':
                    continue

                content, fields = read_datastore(ckan, r["id"])

                records = {
                    "package": p["name"],
                    "resource": r["name"],
                    "usability": score_usability(fields, content),
                    "metadata": score_metadata(p, fields),
                    "freshness": score_freshness(p),
                    "completeness": score_completeness(content),
                    "accessibility": score_accessibility(p, r, "datastore"),
                }
                
                logging.info(f"Datastore Score {p['name']}: {r['name']} - {len(content)} records")
                logging.info(records)
            
            data.append(records)

    pd.DataFrame(data).to_parquet(filepath, engine="fastparquet", compression=None)

    return str(filepath)
