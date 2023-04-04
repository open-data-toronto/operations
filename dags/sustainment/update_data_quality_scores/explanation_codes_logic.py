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
from sustainment.update_data_quality_scores import dqs_logic 
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

    filename = "data_quality_explanation_code"
    filepath = tmp_dir / f"{filename}.parquet"

    def usability_explanation_code(columns, data):
        """
        How easy is it to use the data given how it is organized/structured?
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
            
        col_names_message = "Colnames relative hard to understand." if metrics["col_names"] <= 0.25 else ""
        col_constant_messgae = "So many constant columns." if metrics["col_constant"] <= 0.6 else ""
        
        usability_message = col_names_message + col_constant_messgae
        
        return usability_message
    
    def metadata_explanation_code(package, metadata_fields):
        output = []
        for field in metadata_fields:
            if field not in package or field is None:
                output.append(field)
        
        metadata_message = f"Missing Fields: {','.join(output)}" if output else ""
        
        return metadata_message
    
    def freshness_explanation_code(package, time_map):

        rr = package["refresh_rate"].lower()

        if rr in time_map and "last_refreshed" in package and package["last_refreshed"]:            
            days = dqs_logic.parse_datetime( package["last_refreshed"] )
    
            # calculate elapse periods
            elapse_periods = round((days - time_map[rr]) / time_map[rr])
            elapse_period_message = f"{elapse_periods} periods behind, should be refreshed {rr}." if elapse_periods >= 2 else ""   

            # calculate elapse days
            elapse_days_message = f" {days} days elapsed." if days > 180 else ""
            
            freshness_message = elapse_period_message + elapse_days_message
        else:
            freshness_message = ""
            
        return freshness_message

    def completeness_explanation_code(data):
        """
        How much of the data is missing?
        """
        missing_rate = round((np.sum(len(data) - data.count()) / np.prod(data.shape)) * 100)
        completeness_message = f"{missing_rate} % of data is missing" if missing_rate >= 40 else ""
        
        return completeness_message
    
    def accessibility_explanation_code(p, resource, package_type, etl_intentory):
        tags_message = "" if "tags" in p and (p["num_tags"] > 0) else " missing tags."
        if package_type == "filestore":
            if resource["name"] in etl_intentory["resource_name"].values.tolist():
                engine_info = etl_intentory.loc[etl_intentory['resource_name'] == resource["name"], 'engine'].iloc[0]  
            else:
                engine_info = None

            pipeline_message = "" if engine_info else "no pipeline asscociated."
        else:   
            pipeline_message = ""
        
        accessibility_message = pipeline_message + tags_message
        
        return accessibility_message
    
    data = []
    etl_intentory = dqs_logic.get_etl_inventory("od-etl-configs")
    
    for p in packages:
        logging.info(f"---------Package: {p['name']}")
        if p["name"].lower() == "tags":
            continue
        # skip retired dataset for evaluation
        if "is_retired" in p and (str(p.get("is_retired")).lower() == "true"):
            logging.info(f"Dataset {p['name']} is retired.")
            continue
        
        # filestore code
        if p["dataset_category"] in ["Document", "Website"]:
            
            for r in p["resources"]:
                records = {
                "store_type": "filestore",
                "package": p["name"],
                "resource": r["name"],
                "usability": 0,
                "usability_code": "Not Applicable",
                "metadata": dqs_logic.score_metadata(p, METADATA_FIELDS),
                "metadata_code": metadata_explanation_code(p, METADATA_FIELDS),
                "freshness": dqs_logic.score_freshness(p, TIME_MAP),
                "freshness_code": freshness_explanation_code(p, TIME_MAP),
                "completeness": 0,
                "completeness_code": "Not Applicable",
                "accessibility": dqs_logic.score_accessibility(p, r, "filestore", etl_intentory),
                "accessibility_code": accessibility_explanation_code(p, r, "filestore", etl_intentory)
                }
                logging.info(f"Filestore Score: Package Name {p['name']}")
                data.append(records)
                logging.info(records)
        else:
        
            # datastore code
            for r in p["resources"]:
                if "datastore_active" not in r or str(r["datastore_active"]).lower() == 'false':
                    continue

                content, fields = dqs_logic.read_datastore(ckan, r["id"])

                records = {
                    "store_type": "datastore",
                    "package": p["name"],
                    "resource": r["name"],
                    "usability": dqs_logic.score_usability(fields, content),
                    "usability_code": usability_explanation_code(fields, content),
                    "metadata": dqs_logic.score_metadata(p, METADATA_FIELDS, fields),
                    "metadata_code": metadata_explanation_code(p, METADATA_FIELDS),
                    "freshness": dqs_logic.score_freshness(p, TIME_MAP),
                    "freshness_code": freshness_explanation_code(p, TIME_MAP),
                    "completeness": dqs_logic.score_completeness(content),
                    "completeness_code": completeness_explanation_code(content),
                    "accessibility": dqs_logic.score_accessibility(p, r, "datastore", etl_intentory),
                    "accessibility_code": accessibility_explanation_code(p, r, "datastore", etl_intentory)
                }
                
                logging.info(f"Datastore Score {p['name']}: {r['name']} - {len(content)} records")
                logging.info(records)
            
            data.append(records)

    pd.DataFrame(data).to_parquet(filepath, engine="fastparquet", compression=None)
    df = pd.read_parquet(filepath)
    
    explanation_code_file_path = SCORES_PATH / f"explanation_code.csv"
    df.to_csv(explanation_code_file_path)

    return str(filepath)
