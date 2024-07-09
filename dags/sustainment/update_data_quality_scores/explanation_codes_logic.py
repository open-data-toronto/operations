import logging
from datetime import datetime as dt
from pathlib import Path

import nltk
import numpy as np
import pandas as pd
from nltk.corpus import wordnet
from sustainment.update_data_quality_scores import dqs_logic, dqs_utils

nltk.download("wordnet")


def usability_explanation_code(columns, data):
    """
    How easy is it to use the data given how it is organized/structured?
    """

    metrics = {
        "col_names": 1,  # Column names easy to understand?
        "col_constant": [],  # Columns where all values are constant?
    }

    for f in columns:
        words = dqs_utils.parse_col_name(f["id"])
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

    usability_message = col_names_message + col_constant_messgae

    return usability_message


def metadata_explanation_code(package, metadata_fields, columns=None):
    missing_fields = []
    email_message = ""
    url_message = ""
    for field in metadata_fields:
        if field not in package or field is None:
            missing_fields.append(field)
        elif field == "owner_email" and "opendata@toronto.ca" in package[field]:
            email_message = "~owner_is_opendata"
        elif field == "information_url":
            url_message = dqs_utils.information_url_checker(package[field])

    missing_fields_message = (
        f"~metadata_missing:{','.join(missing_fields)}" if missing_fields else ""
    )

    metadata_message = missing_fields_message + email_message + url_message

    if columns:
        is_empty, missing_cols = dqs_utils.attribue_description_check(columns)
        if is_empty:
            data_attributes_message = "~all_data_def_missing"
        else:
            data_attributes_message = (
                f"~missing_def_cols:{','.join(missing_cols)}" if missing_cols else ""
            )

        metadata_message = metadata_message + data_attributes_message

    return metadata_message


def freshness_explanation_code(package, time_map):
    freshness_message = ""
    rr = package["refresh_rate"].lower()
    if rr == "real-time":
        return freshness_message

    if "last_refreshed" in package and package["last_refreshed"]:
        days = dqs_utils.parse_datetime(package["last_refreshed"])

        if rr in ["as available", "will not be refreshed"]:
            freshness_message = f"~refresh_rate:{rr}~stale" if days > (365 * 2) else ""

        if rr in time_map:
            # calculate elapse periods
            elapse_periods = round((days - time_map[rr]) / time_map[rr], 1)
            elapse_period_message = (
                f"~refresh_rate:{rr}~periods_behind:{elapse_periods}"
                if elapse_periods > 0
                else ""
            )

            freshness_message = elapse_period_message

    return freshness_message


def completeness_explanation_code(data):
    """
    How much of the data is missing?
    """
    missing_rate = round((np.sum(len(data) - data.count()) / np.prod(data.shape)) * 100)
    completeness_message = "~significant_missing_data" if missing_rate >= 50 else ""

    return completeness_message


def accessibility_explanation_code(p, resource, package_type, etl_inventory):
    accessibility_message = ""
    rr = p["refresh_rate"].lower()
    if rr == "real-time":
        return accessibility_message

    tags_message = "" if "tags" in p and (p["num_tags"] > 0) else "~no_tags"

    # get etl engine info from etl-inventory
    if resource["name"] in etl_inventory["resource_name"].values.tolist():
        engine_info = etl_inventory.loc[
            etl_inventory["resource_name"] == resource["name"], "engine"
        ].iloc[0]
    else:
        engine_info = None

    pipeline_message = "" if engine_info else "~no_pipeline_found"
    package_type_message = "~filestore_resource" if package_type == "filestore" else ""

    accessibility_message = pipeline_message + tags_message + package_type_message

    return accessibility_message


# calculate final scores and codes
def calculate_final_scores_and_codes(
    df, weights_datastore, weights_filestore, dimensions, bins
):
    # Calculate Datastore weighted and overall score
    ds_df = df[df["store_type"] == "datastore"]
    scores = pd.DataFrame([weights_datastore] * len(ds_df.index))
    scores.index = ds_df.index
    scores.columns = dimensions

    ds_scores = ds_df.loc[:, dimensions]
    scores = ds_scores.multiply(scores)
    ds_scores["score"] = scores.sum(axis=1)
    logging.info(f"Datastore resources: {ds_scores.shape[0]}")

    # Calculate Filestore weighted and overall score
    fs_df = df[df["store_type"] == "filestore"]
    scores = pd.DataFrame([weights_filestore] * len(fs_df.index))
    scores.index = fs_df.index
    scores.columns = dimensions[:3]

    fs_scores = fs_df.loc[:, dimensions]
    fs_scores_3d = fs_df.loc[:, dimensions[:3]]
    scores = fs_scores_3d.multiply(scores)
    fs_scores["score"] = scores.sum(axis=1)
    logging.info(f"Filestore resources: {fs_scores.shape[0]}")

    # Combine datastore and filestore, normalize overall score and assign bins
    df_scores = ds_scores.append(fs_scores)

    labels = list(bins.keys())
    bins_cuts = [-1]
    bins_cuts.extend(bins.values())

    df_scores["grade"] = pd.cut(df_scores["score"], bins=bins_cuts, labels=labels)
    df_scores = df_scores.reset_index(drop=False)
    # only keep for final overall score and grade
    df_scores = df_scores.loc[:, ~df_scores.columns.isin(dimensions)]

    # map the final scores
    df = df.reset_index(drop=False)
    df_output = pd.merge(df, df_scores, on=["package", "resource"])

    logging.info(df_output.columns)

    df_output["recorded_at"] = dt.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    df_output = df_output.round(3)

    return df_output


def prepare_and_normalize_scores(**kwargs):
    ti = kwargs.pop("ti")
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    raw_explanation_code_path = Path(
        ti.xcom_pull(task_ids="explanation_code_catalogue")
    )
    WEIGHTS_DATASTORE = kwargs.pop("WEIGHTS_DATASTORE")
    WEIGHTS_FILESTORE = kwargs.pop("WEIGHTS_FILESTORE")
    BINS = kwargs.pop("BINS")
    DIMENSIONS = kwargs.pop("DIMENSIONS")

    df = pd.read_parquet(raw_explanation_code_path).set_index(
        ["package", "resource"], drop=True
    )

    df_output = calculate_final_scores_and_codes(
        df, WEIGHTS_DATASTORE, WEIGHTS_FILESTORE, DIMENSIONS, BINS
    )

    filename = "final_scores_and_codes"
    filepath = tmp_dir / f"{filename}.parquet"

    df_output.to_parquet(filepath, engine="fastparquet", compression=None)

    return str(filepath)


def get_run_list(**kwargs):
    ti = kwargs.pop("ti")
    packages = ti.xcom_pull(task_ids="get_all_packages")
    run_schedule = ti.xcom_pull(task_ids="run_schedule")

    all_dataset_pool = []
    daily_dataset_pool = []

    for p in packages:
        all_dataset_pool.append(p["name"])
        refresh_rate = p["refresh_rate"]
        if refresh_rate == "Daily":
            daily_dataset_pool.append(p["name"])

    if run_schedule == "run_daily_dataset":
        num = len(daily_dataset_pool)
        logging.info(f"Run {num} Daily Datasets!")
        return daily_dataset_pool

    logging.info("Run All Datasets!")
    return all_dataset_pool


def explanation_code_catalogue(**kwargs):
    ti = kwargs.pop("ti")
    packages = ti.xcom_pull(
        task_ids="get_all_packages"
    )  # can break package list into segments if needed
    tmp_dir = Path(ti.xcom_pull(task_ids="create_tmp_data_dir"))
    dataset_run_list = ti.xcom_pull(task_ids="dataset_run_list")
    METADATA_FIELDS = kwargs.pop("METADATA_FIELDS")
    TIME_MAP = kwargs.pop("TIME_MAP")
    PENALTY_MAP = kwargs.pop("PENALTY_MAP")

    ckan = kwargs.pop("ckan")

    filename = "data_quality_explanation_code"
    filepath = tmp_dir / f"{filename}.parquet"

    data = []
    etl_inventory = dqs_utils.get_etl_inventory("od-etl-configs", ckan)

    for p in packages:
        if p["name"] in dataset_run_list:
            logging.info(f"---------Package: {p['name']}")
            if p["name"].lower() == "metadata-catalog":
                continue
            # skip retired dataset for evaluation
            if "is_retired" in p and (str(p.get("is_retired")).lower() == "true"):
                logging.info(f"Dataset {p['name']} is retired.")
                continue

            division = p["owner_division"] if "owner_division" in p else None
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
                        "freshness": dqs_logic.score_freshness(
                            p, TIME_MAP, PENALTY_MAP
                        ),
                        "freshness_code": freshness_explanation_code(p, TIME_MAP),
                        "completeness": 0,
                        "completeness_code": "Not Applicable",
                        "accessibility": dqs_logic.score_accessibility(
                            p, r, "filestore", etl_inventory
                        ),
                        "accessibility_code": accessibility_explanation_code(
                            p, r, "filestore", etl_inventory
                        ),
                        "store_type": "filestore",
                        "division": division,
                    }
                    logging.info(f"Filestore Score: {p['name']}: {r['name']}")
                    data.append(records)
            else:
                # datastore code
                for r in p["resources"]:
                    if (
                        "datastore_active" not in r
                        or str(r["datastore_active"]).lower() == "false"
                    ):
                        continue

                    try:
                        data_path, fields = dqs_utils.read_datastore(ckan, r["id"], tmp_dir)
                        content = pd.read_csv(data_path, index_col=False)
                        
                        # drop columns which is not useful
                        content = content.drop("_id", axis=1)
                        
                        records = {
                            "package": p["name"],
                            "resource": r["name"],
                            "usability": dqs_logic.score_usability(fields, content),
                            "usability_code": usability_explanation_code(
                                fields, content
                            ),
                            "metadata": dqs_logic.score_metadata(
                                p, METADATA_FIELDS, fields
                            ),
                            "metadata_code": metadata_explanation_code(
                                p, METADATA_FIELDS, fields
                            ),
                            "freshness": dqs_logic.score_freshness(
                                p, TIME_MAP, PENALTY_MAP
                            ),
                            "freshness_code": freshness_explanation_code(p, TIME_MAP),
                            "completeness": dqs_logic.score_completeness(content),
                            "completeness_code": completeness_explanation_code(content),
                            "accessibility": dqs_logic.score_accessibility(
                                p, r, "datastore", etl_inventory
                            ),
                            "accessibility_code": accessibility_explanation_code(
                                p, r, "datastore", etl_inventory
                            ),
                            "store_type": "datastore",
                            "division": division,
                        }

                        logging.info(
                            f"Datastore Score {p['name']}: {r['name']} - {len(content)} records"
                        )
                        data.append(records)
                    except Exception as e:
                        logging.warning(
                            "Invalid Datastore Resource. Skipping Calculation for this resource."
                        )

    df = pd.DataFrame(data)
    df.to_parquet(filepath, engine="fastparquet", compression=None)

    return str(filepath)
