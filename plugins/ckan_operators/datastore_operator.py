import hashlib
import json
import logging
from pathlib import Path
from typing import List

import ckanapi
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class BackupDatastoreResourceOperator(BaseOperator):
    """
    Reads datastore resource, creates backup files for fields (json) and records (parquet). Args:
        - address: CKAN instance URL
        - apikey: CKAN API key
        - resource_task_id: task_id that returns resource object (ie. GetOrCreateResourcePackage)
        - dir_task_id: task_id that returns backup directory


    Returns dictionary containing:
        - fields: json file path containing fields for datastore resource
        - data: parquet file path containing fields for datastore resource
        - columns: number of columns in datastore resource
        - rows: number of rows in datastore_resource
        - resource_id: datastore resource ID
    """

    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        resource_task_id: str,
        dir_task_id: str,
        sort_columns: List[str] = [],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dir_task_id = dir_task_id
        self.resource_task_id = resource_task_id
        self.sort_columns = sort_columns
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def _checksum_datastore_response(self, datastore_response):
        data = pd.DataFrame(datastore_response["records"])
        data = data.drop("_id", axis=1)
        if len(self.sort_columns) > 0:
            data = data.sort_values(by=self.sort_columns)

        data_hash = hashlib.md5()
        data_hash.update(data.to_csv(index=False).encode("utf-8"))

        return data_hash.hexdigest()

    def _build_dataframe(self, records):
        data = pd.DataFrame(records)
        if "_id" in data.columns.values:
            data = data.drop("_id", axis=1)

        return data

    def _save_fields_json(self, datastore_response, checksum, backups_dir):
        fields_file_path = backups_dir / f"fields.{checksum}.json"

        if not fields_file_path.exists():
            fields = [f for f in datastore_response["fields"] if f["id"] != "_id"]
            with open(fields_file_path, "w") as f:
                json.dump(fields, f)

        return fields_file_path

    def _save_data_parquet(self, datastore_response, checksum, backups_dir, data):
        data_file_path = backups_dir / f"data.{checksum}.parquet"

        if not data_file_path.exists():
            data.to_parquet(path=data_file_path, engine="fastparquet", compression=None)

        return data_file_path

    def execute(self, context):
        ti = context["ti"]
        resource = ti.xcom_pull(task_ids=self.resource_task_id)
        backups_dir = Path(ti.xcom_pull(task_ids=self.dir_task_id))

        record_count = self.ckan.action.datastore_search(id=resource["id"], limit=0)[
            "total"
        ]

        datastore_response = self.ckan.action.datastore_search(
            id=resource["id"], limit=record_count
        )

        data = self._build_dataframe(datastore_response["records"])
        checksum = self._checksum_datastore_response(datastore_response)

        result = {
            "fields_file_path": self._save_fields_json(
                datastore_response, checksum, backups_dir
            ),
            "data_file_path": self._save_data_parquet(
                datastore_response, checksum, backups_dir, data
            ),
            "records": data.shape[0],
            "columns": data.shape[1],
            "resource_id": datastore_response["resource_id"],
            "checksum": checksum,
        }

        logging.info(f"Returning: {result}")

        return result


class DeleteDatastoreResourceRecordsOperator(BaseOperator):
    """
    Deletes datastore resource records. Args:
        - address: CKAN instance URL
        - apikey: CKAN API key
        - backup_task_id: task_id that returns backup file information (BackupDatastoreResourceOperator)
    """

    @apply_defaults
    def __init__(
        self, address: str, apikey: str, backup_task_id: str, **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.backup_task_id = backup_task_id
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def execute(self, context):
        backups_info = context["ti"].xcom_pull(task_ids=self.backup_task_id)

        self.ckan.action.datastore_delete(id=backups_info["resource_id"])

        with open(Path(backups_info["fields_file_path"]), "r") as f:
            fields = json.load(f)

        self.ckan.action.datastore_create(id=backups_info["resource_id"], fields=fields)

        record_count = self.ckan.action.datastore_search(
            id=backups_info["resource_id"], limit=0
        )["total"]

        assert record_count == 0, f"Resource not empty after cleanup: {record_count}"


class InsertDatastoreResourceRecordsOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        resource_task_id: str,
        parquet_filepath_task_id: str = None,
        fields_json_path_task_id: str = None,
        chunk_size: int = 20000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parquet_filepath_task_id = parquet_filepath_task_id
        self.resource_task_id = resource_task_id
        self.chunk_size = chunk_size
        self.fields_json_path_task_id = fields_json_path_task_id
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def _create_empty_resource_with_fields(self, fields_path, resource_id):
        with open(fields_path, "r") as f:
            fields = json.load(f)

        self.ckan.action.datastore_create(id=resource_id, fields=fields)

    def execute(self, context):
        ti = context["ti"]
        resource = ti.xcom_pull(task_ids=self.resource_task_id)

        if self.fields_json_path_task_id is not None:
            fields_path = Path(ti.xcom_pull(task_ids=self.fields_json_path_task_id))
            self._create_empty_resource_with_fields(fields_path, resource["id"])

        if self.parquet_filepath_task_id is not None:
            path = Path(ti.xcom_pull(task_ids=self.parquet_filepath_task_id))

            data = pd.read_parquet(path)
            records = data.to_dict(orient="records")

            chunks = [
                records[i : i + self.chunk_size]
                for i in range(0, len(records), self.chunk_size)
            ]

            for chunk in chunks:
                clean_records = []
                logging.info(f"Removing NaNs and inserting {len(records)} records")
                for r in chunk:
                    record = {}
                    for key, value in r.items():
                        if value == value:
                            record[key] = value
                    clean_records.append(record)

                self.ckan.action.datastore_create(
                    id=resource["id"], records=clean_records
                )

            logging.info(f"Records inserted: {data.shape[0]}")

            return data.shape[0]


class RestoreDatastoreResourceBackupOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, address: str, apikey: str, backup_task_id: str, **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.backup_task_id = backup_task_id
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def execute(self, context):
        backups_info = context["ti"].xcom_pull(task_ids=self.backup_task_id)

        assert backups_info is not None, "No backup information"

        resource_id = backups_info["resource_id"]

        with open(Path(backups_info["fields_file_path"]), "r") as f:
            fields = json.load(f)

        data = pd.read_parquet(Path(backups_info["data_file_path"]))
        records = data.to_dict(orient="records")

        try:
            self.ckan.action.datastore_delete(id=resource_id)
        except Exception as e:
            logging.error(e.msg)

        result = self.ckan.action.datastore_create(
            id=resource_id, fields=fields, records=records
        )

        logging.info(f"Result: {result}")

        return result
