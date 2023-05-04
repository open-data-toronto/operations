import logging
import re
from datetime import datetime as dt
from pathlib import Path

import nltk
import numpy as np
import pandas as pd
from nltk.corpus import wordnet
from sustainment.update_data_quality_scores import dqs_logic
import requests
import math
from geopandas import gpd
from sklearn.preprocessing import MinMaxScaler

nltk.download("wordnet")


def information_url_checker(information_url):
    result_info = ""
    if "http" in information_url:
        call = information_url
    else:
        call = "https://" + information_url

    # record probametic url
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
    for f in columns:
        if (
            "info" in f
            and (f["info"]["notes"] is not None)
            and (f["info"]["notes"] != "")
            and f["info"]["notes"].strip() != f["id"]
        ):
            counter += 1

    return counter == 0


def prepare_and_normalize_scores(**kwargs):
    ti = kwargs.pop("ti")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    raw_explanation_code_path = Path(
        ti.xcom_pull(task_ids="explanation_code_catalogue")
    )
    weights = ti.xcom_pull(task_ids="calculate_model_weights")
    BINS = kwargs.pop("BINS")
    DIMENSIONS = kwargs.pop("DIMENSIONS")

    df = pd.read_parquet(raw_explanation_code_path).set_index(
        ["package", "resource"], drop=True
    )

    scores = pd.DataFrame([weights] * len(df.index))
    scores.index = df.index
    scores.columns = DIMENSIONS

    df_scores = df.loc[:, DIMENSIONS]
    df_codes = df.loc[:, ~df.columns.isin(DIMENSIONS)]
    scores = df_scores.multiply(scores)

    df_scores["score"] = scores.sum(axis=1)
    df_scores["score_norm"] = MinMaxScaler().fit_transform(df_scores[["score"]])

    df_scores = df_scores.groupby("package").mean()

    labels = list(BINS.keys())

    bins = [-1]
    bins.extend(BINS.values())

    df_scores["grade"] = pd.cut(df_scores["score_norm"], bins=bins, labels=labels)
    df_scores["grade_norm"] = pd.cut(df_scores["score_norm"], bins=bins, labels=labels)

    df_codes = df_codes.reset_index()

    # calculate median value for usability, completeness dimension for datastore
    usability_median = df_scores["usability"].median()
    completness_median = df_scores["completeness"].median()

    # map the package level score to resource level
    df_output = pd.merge(df_codes, df_scores, on=["package"], how="outer")

    # assign median value for usability and completeness dimensions to filestore
    df_output["usability"] = df_output.apply(
        lambda x: x["usability"]
        if x["store_type"] == "datastore"
        else usability_median,
        axis=1,
    )
    df_output["completeness"] = df_output.apply(
        lambda x: x["completeness"]
        if x["store_type"] == "datastore"
        else completness_median,
        axis=1,
    )

    logging.info(df_output.columns)

    df_output["recorded_at"] = dt.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    df_output = df_output.round(2)

    filename = "final_scores_and_codes"
    filepath = tmp_dir / f"{filename}.parquet"

    df_output.to_parquet(filepath, engine="fastparquet", compression=None)

    return str(filepath)


def explanation_code_catalogue(**kwargs):
    ti = kwargs.pop("ti")
    packages = ti.xcom_pull(
        task_ids="get_all_packages"
    )  # can break package list into segments if needed
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    METADATA_FIELDS = kwargs.pop("METADATA_FIELDS")
    TIME_MAP = kwargs.pop("TIME_MAP")
    PENALTY_MAP = kwargs.pop("PENALTY_MAP")

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
            "col_names": 1,  # Column names easy to understand?
            "col_constant": [],  # Columns where all values are constant?
        }

        for f in columns:
            is_camel, words = parse_col_name(f["id"])
            eng_words = [w for w in words if len(wordnet.synsets(w))]
            if len(eng_words) / len(words) <= 0.2:
                metrics["col_names"] -= 1 / len(columns)

            if not f["id"] == "geometry" and data[f["id"]].nunique() <= 1:
                metrics["col_constant"].append(f["id"])

        col_names_message = "~colnames_unclear" if metrics["col_names"] <= 0.5 else ""
        col_constant_messgae = (
            f"~constant_cols:{','.join(metrics['col_constant'])}"
            if metrics["col_constant"]
            else ""
        )
        geo_validity_message = ""
        if isinstance(data, gpd.GeoDataFrame):
            counts = data["geometry"].is_valid.value_counts()

            geo_validity_message = (
                "~invalid_geospatial"
                if (False in counts and (counts[False] / (len(data) * 0.05) > 0.01))
                else ""
            )

        usability_message = (
            col_names_message + col_constant_messgae + geo_validity_message
        )

        return usability_message

    def metadata_explanation_code(package, metadata_fields, columns=None):
        missing_fields = []
        email_message = ""
        url_message = ""
        for field in metadata_fields:
            if field not in package or field is None:
                missing_fields.append(field)
            elif field == "owner_email" and "opendata" in package[field]:
                email_message = "~owner_is_opendata"
            elif field == "information_url":
                url_message = information_url_checker(package[field])

        missing_fields_message = (
            f"~metadata_missing:{','.join(missing_fields)}" if missing_fields else ""
        )

        metadata_message = missing_fields_message + email_message + url_message

        if columns:
            is_empty = attribue_description_check(columns)
            data_attributes_message = "~data_def_missing" if is_empty else ""

            metadata_message = metadata_message + data_attributes_message

        return metadata_message

    def freshness_explanation_code(package, time_map):
        freshness_message = ""
        rr = package["refresh_rate"].lower()

        if "last_refreshed" in package and package["last_refreshed"]:
            days = dqs_logic.parse_datetime(package["last_refreshed"])
            logging.info(f"elapse days: {days}, refresh_rate: {rr}")

            if rr == "as available":
                freshness_message = "~stale" if days > (365 * 2) else ""

            if rr in time_map:
                # calculate elapse periods
                elapse_periods = math.floor((days - time_map[rr]) / time_map[rr])
                elapse_period_message = (
                    f"~periods_behind:{elapse_periods}" if elapse_periods >= 1 else ""
                )

                freshness_message = elapse_period_message

        return freshness_message

    def completeness_explanation_code(data):
        """
        How much of the data is missing?
        """
        missing_rate = round(
            (np.sum(len(data) - data.count()) / np.prod(data.shape)) * 100
        )
        completeness_message = "~significant_missing_data" if missing_rate >= 50 else ""

        return completeness_message

    def accessibility_explanation_code(p, resource, package_type, etl_intentory):
        tags_message = "" if "tags" in p and (p["num_tags"] > 0) else "~no_tags"
        if package_type == "filestore":
            package_type_message = "~filestore_resource"
            if resource["name"] in etl_intentory["resource_name"].values.tolist():
                engine_info = etl_intentory.loc[
                    etl_intentory["resource_name"] == resource["name"], "engine"
                ].iloc[0]
            else:
                engine_info = None

            pipeline_message = "" if engine_info else "~no_pipeline_found"
        else:
            package_type_message = ""
            pipeline_message = ""

        accessibility_message = pipeline_message + tags_message + package_type_message

        return accessibility_message

    data = []
    etl_intentory = dqs_logic.get_etl_inventory("od-etl-configs", ckan)

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
                    "package": p["name"],
                    "resource": r["name"],
                    "usability": 0,
                    "usability_code": "Not Applicable",
                    "metadata": dqs_logic.score_metadata(p, METADATA_FIELDS),
                    "metadata_code": metadata_explanation_code(p, METADATA_FIELDS),
                    "freshness": dqs_logic.score_freshness(p, TIME_MAP, PENALTY_MAP),
                    "freshness_code": freshness_explanation_code(p, TIME_MAP),
                    "completeness": 0,
                    "completeness_code": "Not Applicable",
                    "accessibility": dqs_logic.score_accessibility(
                        p, r, "filestore", etl_intentory
                    ),
                    "accessibility_code": accessibility_explanation_code(
                        p, r, "filestore", etl_intentory
                    ),
                    "store_type": "filestore",
                }
                logging.info(f"Filestore Score: Package Name {p['name']}")
                data.append(records)
                logging.info(records)
        else:
            # datastore code
            for r in p["resources"]:
                if (
                    "datastore_active" not in r
                    or str(r["datastore_active"]).lower() == "false"
                ):
                    continue

                content, fields = dqs_logic.read_datastore(ckan, r["id"])

                records = {
                    "package": p["name"],
                    "resource": r["name"],
                    "usability": dqs_logic.score_usability(fields, content),
                    "usability_code": usability_explanation_code(fields, content),
                    "metadata": dqs_logic.score_metadata(p, METADATA_FIELDS, fields),
                    "metadata_code": metadata_explanation_code(
                        p, METADATA_FIELDS, fields
                    ),
                    "freshness": dqs_logic.score_freshness(p, TIME_MAP, PENALTY_MAP),
                    "freshness_code": freshness_explanation_code(p, TIME_MAP),
                    "completeness": dqs_logic.score_completeness(content),
                    "completeness_code": completeness_explanation_code(content),
                    "accessibility": dqs_logic.score_accessibility(
                        p, r, "datastore", etl_intentory
                    ),
                    "accessibility_code": accessibility_explanation_code(
                        p, r, "datastore", etl_intentory
                    ),
                    "store_type": "datastore",
                }

                logging.info(
                    f"Datastore Score {p['name']}: {r['name']} - {len(content)} records"
                )
                logging.info(records)

                data.append(records)

    df = pd.DataFrame(data)

    df.to_parquet(filepath, engine="fastparquet", compression=None)

    return str(filepath)
