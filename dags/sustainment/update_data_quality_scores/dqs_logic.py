import logging
import nltk
import numpy as np
from nltk.corpus import wordnet
from sustainment.update_data_quality_scores import dqs_utils

nltk.download("wordnet")


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
        words = dqs_utils.parse_col_name(f["id"])
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
            and not (field == "owner_email" and "opendata@toronto.ca" in package[field])
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
    if rr == "real-time":
        return 1

    if "last_refreshed" in package and package["last_refreshed"]:
        days = dqs_utils.parse_datetime(package["last_refreshed"])

        if rr in ["as available", "will not be refreshed"]:
            metrics["elapse_periods"] = 1
        elif rr in time_map:
            elapse_periods = max(0, (days - time_map[rr]) / time_map[rr])
            metrics["elapse_periods"] = (
                max(0, 1 - (elapse_periods / penalty_map[rr]) ** 2)
                if elapse_periods > 0
                else 1
            )

        # Decrease the score starting from ~2 years to ~3 years
        metrics["elapse_days"] = (
            max(0, 1 - (days / (365 * 3)) ** 2) if days > (365 * 2) else 1
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


def score_accessibility(p, resource, dataset_type, etl_inventory):
    """
    Is data available via APIs? Have tags?
    """

    # Don't penalize for being filestore/don't have pipeline for real-time dataset
    rr = p["refresh_rate"].lower()
    if rr == "real-time":
        return 1

    metrics = {}
    metrics["tags"] = 1 if "tags" in p and (p["num_tags"] > 0) else 0.5

    # get etl engine info from etl-inventory
    if resource["name"] in etl_inventory["resource_name"].values.tolist():
        engine_info = etl_inventory.loc[
            etl_inventory["resource_name"] == resource["name"], "engine"
        ].iloc[0]
    else:
        engine_info = None

    if dataset_type == "datastore":
        metrics["pipeline"] = 1 if engine_info else 0.5
    if dataset_type == "filestore":
        metrics["pipeline"] = 0.8 if engine_info else 0.5

    return np.mean(list(metrics.values()))
