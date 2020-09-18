from pathlib import Path
from datetime import datetime
import pandas as pd
import hashlib
import ckanapi
import json


def backup_datastore_resource(
    ckan: ckanapi.RemoteCKAN,
    resource_id: str,
    dest_path: Path,
    backup_fields: bool,
    record_limit: int = 1000000,
):
    resource = ckan.action.resource_show(id=resource_id)
    package = ckan.action.package_show(id=resource["package_id"])
    datastore_response = ckan.action.datastore_search(
        id=resource_id, limit=record_limit
    )

    prefix_parts = [package["name"], resource["name"]]

    results = {}

    data = pd.DataFrame(datastore_response["records"]).drop("_id", axis=1)

    data_hash = hashlib.md5()
    data_hash.update(data.to_csv(index=False).encode("utf-8"))
    prefix_parts.append(data_hash.hexdigest())

    data_path = dest_path / "__".join(prefix_parts + ["data.parquet"])
    if not data_path.exists():
        data.to_parquet(data_path)

    results["data"] = data_path
    results["records"] = data.shape[0]
    results["columns"] = data.shape[1]

    if backup_fields:
        fields = [f for f in datastore_response["fields"] if f["id"] != "_id"]

        fields_path = dest_path / "__".join(prefix_parts + ["fields.json"])
        if not fields_path.exists():
            with open(fields_path, "w") as f:
                json.dump(fields, f)

        results["fields"] = fields_path

    return results


def update_resource_last_modified(
    ckan: ckanapi.RemoteCKAN, resource_id: str, new_last_modified: datetime
):
    return ckan.action.resource_patch(
        id=resource_id, last_modified=new_last_modified.strftime("%Y-%m-%dT%H:%M:%S"),
    )
