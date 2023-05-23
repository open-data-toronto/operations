import hashlib
import json
import csv
import logging
import codecs
import openpyxl

from datetime import datetime
from pathlib import Path
from typing import List

from ckan_operators import nested_file_readers

import ckanapi
import requests
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class BackupDatastoreResourceOperator(BaseOperator):
    """
    Reads datastore resource, creates backup files for fields (json) and
    records (parquet). Args:
        - address: CKAN instance URL
        - apikey: CKAN API key
        - resource_task_id: task_id that returns resource object
          (ie. GetOrCreateResourcePackage)
        - dir_task_id: task_id that returns backup director


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
        if "_id" in data.columns.values:
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
            data.to_parquet(
                path=data_file_path,
                engine="fastparquet",
                compression=None)

        return data_file_path

    def execute(self, context):
        # get a resource and backup directory via xcom
        ti = context["ti"]
        resource = ti.xcom_pull(task_ids=self.resource_task_id)
        backups_dir = Path(ti.xcom_pull(task_ids=self.dir_task_id))

        # get number of records for this datastore resource
        record_count = self.ckan.action.datastore_search(id=resource["id"], limit=0)[
            "total"
        ]

        # get data from datastore resource
        datastore_response = self.ckan.action.datastore_search(
            id=resource["id"], limit=record_count
        )

        # turn data into dataframe
        data = self._build_dataframe(datastore_response["records"])
        checksum = self._checksum_datastore_response(datastore_response)

        # return filepath for fields json, data parquet, row/col counts,
        # checksum, and resource_id
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


class DeleteDatastoreResourceOperator(BaseOperator):
    """
    Deletes a datastore resource
    Inputs:
        - address: CKAN instance URL
        - apikey: CKAN API key
        - resource_id: CKAN resource id to be deleted

        Resource id can be given with n actual value, or with a reference to a
        task_id and task_key that returns the value

        Note: Deleting the entire resource also deletes the data dictionary
        (i.e. schema, field definitions and types).
        To keep the existing schema, delete the datastore resource records
        instead by using the DeleteDatastoreResourceRecordsOperator - this
        keeps the schema.
    """

    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        resource_id: str = None,
        resource_id_task_id: str = None,
        resource_id_task_key: str = None,
        **kwargs,
    ) -> None:
        # init ckan client and resource_id to be truncated
        super().__init__(**kwargs)
        self.resource_id, self.resource_id_task_id, self.resource_id_task_key = (
            resource_id,
            resource_id_task_id,
            resource_id_task_key,
        )
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def execute(self, context):
        # get task instance from context
        ti = context["ti"]

        # get resource id from task, if task info provided in input
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[
                self.resource_id_task_key
            ]
            self.resource = ti.xcom_pull(task_ids=self.resource_id_task_id)
            logging.info(self.resource)
            logging.info(
                "Pulled {} from {} via xcom".format(
                    self.resource_id, self.resource_id_task_id
                )
            )

        assert (
            self.resource_id
        ), ("Resource ID is empty! This operator needs a way to get the "
            "resource ID in order to delete the right datastore resource!")
        # Delete the resource
        try:
            self.ckan.action.datastore_delete(id=self.resource_id, force=True)
            logging.info("Deleted " + self.resource_id)

        except Exception as e:
            logging.error("Error while trying to delete resource: " + e)


class DeleteDatastoreResourceRecordsOperator(BaseOperator):
    """
    Deletes datastore resource records. Args:
        - address: CKAN instance URL
        - apikey: CKAN API key
        - backup_task_id: task_id that returns backup file information
          (BackupDatastoreResourceOperator)
    """

    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        backup_task_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.backup_task_id = backup_task_id
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def execute(self, context):
        backups_info = context["ti"].xcom_pull(task_ids=self.backup_task_id)

        self.ckan.action.datastore_delete(id=backups_info["resource_id"], force=True)

        with open(Path(backups_info["fields_file_path"]), "r") as f:
            fields = json.load(f)

        self.ckan.action.datastore_create(
            id=backups_info["resource_id"], fields=fields, force=True
        )

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

        self.ckan.action.datastore_create(id=resource_id, fields=fields, force=True)

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
                records[i: i + self.chunk_size]
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
                    id=resource["id"], records=clean_records, force=True
                )

            logging.info(f"Records inserted: {data.shape[0]}")

            return data.shape[0]


class RestoreDatastoreResourceBackupOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        backup_task_id: str,
        **kwargs,
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
            logging.error(e)

        result = self.ckan.action.datastore_create(
            id=resource_id, fields=fields, records=records
        )

        logging.info(f"Result: {result}")

        return result


class InsertDatastoreResourceRecordsFromJSONOperator(BaseOperator):
    """
    Reads a JSON file and write the output into a CKAN datastore resource.
    JSON must be a list of dicts, with each dict being a record, like the following:
    [
        { "column1": "string", "column2": 100, "column3": true},
        { "column1": "some other string", "column2": 34, "column3": false}
    ]

    The fields must match the CKAN standard, like the following:
    [
        {
            "id": "column1",
            "type": "text" ,
            "info": {
            "notes": "Description of the field goes here. Info key is optional."
            }
        },
        {
            "id": "column2",
            "type": "int"
        },
        {
            "id": "column3",
            "type": "bool"
        }
    ]

    Expects as inputs:
    - address - url of target ckan
    - apikey - key needed to make authorized ckan calls
    - resource_id - id of the resource that will receive this data
    - data_path - location of the json data file
    - fields_path - location of the data's fields, already in a CKAN-friendly
      format

    All of the above, except the address and apikey, can be given with an
    actual value, or with a reference to a task_id and task_key that returns
    the value
    """

    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        resource_id: str = None,
        resource_id_task_id: str = None,
        resource_id_task_key: str = None,
        data_path: str = None,
        data_path_task_id: str = None,
        data_path_task_key: str = None,
        fields_path: str = None,
        fields_path_task_id: str = None,
        fields_path_task_key: str = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_id, self.resource_id_task_id, self.resource_id_task_key = (
            resource_id,
            resource_id_task_id,
            resource_id_task_key,
        )
        self.data_path, self.data_path_task_id, self.data_path_task_key = (
            data_path,
            data_path_task_id,
            data_path_task_key,
        )
        self.fields_path, self.fields_path_task_id, self.fields_path_task_key = (
            fields_path,
            fields_path_task_id,
            fields_path_task_key,
        )
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def execute(self, context):
        # init task instance from context
        ti = context["ti"]

        # assign important vars if provided from other tasks
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[
                self.resource_id_task_key
            ]

        if self.data_path_task_id and self.data_path_task_key:
            self.data_path = ti.xcom_pull(task_ids=self.data_path_task_id)[
                self.data_path_task_key
            ]

        if self.fields_path_task_id and self.fields_path_task_key:
            self.fields_path = ti.xcom_pull(task_ids=self.fields_path_task_id)[
                self.fields_path_task_key
            ]

        # get fields from file
        with open(self.fields_path, "r") as f:
            fields = json.load(f)
            logging.info(
                "Loaded the following fields from {}: {}".format(
                    self.fields_path, fields
                )
            )

        # populate that resource w data from the path provided
        assert (
            self.data_path
        ), "Data path, or the filepath to the data to be inserted, required!"
        with open(self.data_path) as f:
            data = json.load(f)

        logging.info("Data parsed from JSON file")
        logging.info("Fields from fields file: " + str(fields))
        logging.info("Fields from data file: " + str(data[0].keys()))

        self.ckan.action.datastore_create(
            id=self.resource_id, fields=fields, records=data
        )
        logging.info("Resource created + populated from input fields and data")

        return {"resource_id": self.resource_id, "data_inserted": len(data)}


class InsertDatastoreFromYAMLConfigOperator(BaseOperator):
    """
    Inserts a file's data into a datastore resource based YAML config
    """

    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        resource_id: str = None,
        resource_id_task_id: str = None,
        resource_id_task_key: str = None,
        data_path: str = None,
        data_path_task_id: str = None,
        data_path_task_key: str = None,
        config: dict = {},
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_id, self.resource_id_task_id, self.resource_id_task_key = (
            resource_id,
            resource_id_task_id,
            resource_id_task_key,
        )
        self.data_path, self.data_path_task_id, self.data_path_task_key = (
            data_path,
            data_path_task_id,
            data_path_task_key,
        )
        self.config = config
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def clean_string(self, input):
        if input is None:
            return ""
        if not isinstance(input, str):
            return str(input)
        else:
            return str(input).strip()

    def clean_int(self, input):
        if input:
            return int(input)
        else:
            return None

    def clean_float(self, input):
        if input:
            return float(input)
        else:
            return None

    def clean_date_format(self, input, input_format=None):
        # loops through the list of formats and tries to return an input
        # string into a datetime of one of those formats

        if input is None:
            return

        if len(input) == 0:
            return

        assert isinstance(
            input, str
        ), "Utils.clean_date_format() accepts strings - it got {}".format(
            type(input)
        )

        format_dict = {
            "%Y-%m-%dT%H:%M:%S.%f": "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f": "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S": "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S": "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d": "%Y-%m-%d",
            "%d-%b-%Y": "%Y-%m-%d",
            "%d-%b-%y": "%Y-%m-%d",
            "%b-%d-%Y": "%Y-%m-%d",
            "%m-%d-%y": "%Y-%m-%d",
            "%m-%d-%Y": "%Y-%m-%d",
            "%d-%m-%y": "%Y-%m-%d",
            "%d-%m-%Y": "%Y-%m-%d",
            "%Y%m%d%H%M%S": "%Y-%m-%dT%H:%M:%S",
            "%d%m%Y": "%Y-%m-%d",
            "%d%b%Y": "%Y-%m-%d",
            "%Y-%b-%d": "%Y-%m-%d",
        }

        if input_format:
            input = input.replace("/", "-")
            input_format = input_format.replace("/", "-")
            datetime_object = datetime.strptime(input, input_format)
            output = datetime_object.strftime(format_dict[input_format])

            return output

        else:
            for format in format_dict.keys():
                try:
                    input = input.replace("/", "-")
                    datetime_object = datetime.strptime(input, format)
                    output = datetime_object.strftime(format_dict[format])
                    return output
                except ValueError:
                    pass

    # reads a csv and returns a list of dicts - one dict for each row
    def read_csv_file(self):
        output = []

        # we'll try to detect lat, long attributes here and put them into a
        # geometry attribute
        # it's important to note that incoming CSVs with geometries MUST be in
        # EPSG 4326
        # it's also important to note that this logic will only work with
        # point data, not line or polygon
        latitude_attributes = ["lat", "latitude", "y", "y coordinate"]
        longitude_attributes = ["long", "longitude", "x", "x coordinate"]
        self.geometry_needs_parsing = False

        dictreader = csv.DictReader(codecs.open(self.data_path, "rbU", "latin1"))

        # looking for possible lat long attributes if there isnt a geometry
        # object already...
        # (dictreader object cant be indexed, so we start a loop, go through
        # the first item, then break the loop)
        for firstrow in dictreader:
            if "geometry" not in firstrow.keys() and "geometry" in [
                attr.get("id", None) for attr in self.config["attributes"]
            ]:
                for attr in firstrow.keys():
                    if attr.lower() in latitude_attributes:
                        latitude_attribute = attr
                        self.geometry_needs_parsing = True

                    if attr.lower() in longitude_attributes:
                        longitude_attribute = attr
                        self.geometry_needs_parsing = True
                break

        dictreader = csv.DictReader(codecs.open(self.data_path, "rbU", "latin1"))
        for row in dictreader:
            # strip each attribute name - CKAN requires it
            output_row = {}
            for attr in row.keys():
                output_row[attr.strip()] = row[attr]
            if (
                self.geometry_needs_parsing
                and row[longitude_attribute]
                and row[latitude_attribute]
            ):
                output_row["geometry"] = json.dumps(
                    {
                        "type": "Point",
                        "coordinates": [
                            float(row[longitude_attribute]),
                            float(row[latitude_attribute]),
                        ],
                    }
                )
            elif (
                self.geometry_needs_parsing
                and not row[longitude_attribute]
                and not row[latitude_attribute]
            ):
                output_row["geometry"] = None

            output.append(output_row)

        logging.info("Read {} records from {}".format(len(output), self.data_path))

        return output

    def read_json_file(self):
        return json.load(open(self.data_path, "r", encoding="latin-1"))

    def read_xlsx_file(self):
        workbook = openpyxl.load_workbook(self.data_path)
        worksheet = workbook[self.config["sheet"]]

        # init output and sheet column names
        output = []
        column_names = [col.value for col in worksheet[1]]

        # we'll try to detect lat, long attributes here and put them into a
        # geometry attribute
        # it's important to note that incoming CSVs with geometries MUST be in
        # EPSG 4326
        # it's also important to note that this logic will only work with
        # point data, not line or polygon
        latitude_attributes = ["lat", "latitude", "y", "y coordinate"]
        longitude_attributes = ["long", "longitude", "x", "x coordinate"]
        self.geometry_needs_parsing = False

        if "geometry" not in column_names and "geometry" in [
            attr.get("id", None) for attr in self.config["attributes"]
        ]:
            for attr_index in range(len(column_names)):
                if column_names[attr_index].lower() in latitude_attributes:
                    latitude_attribute = attr_index
                    self.geometry_needs_parsing = True

                if column_names[attr_index].lower() in longitude_attributes:
                    longitude_attribute = attr_index
                    self.geometry_needs_parsing = True

        for row in worksheet.iter_rows(min_row=2):
            output_row = {
                column_names[i]: self.clean_string(row[i].value)
                for i in range(len(row))
            }
            if self.geometry_needs_parsing:
                output_row["geometry"] = json.dumps(
                    {
                        "type": "Point",
                        "coordinates": [
                            row[longitude_attribute].value,
                            row[latitude_attribute].value,
                        ],
                    }
                )

            output.append(output_row)

        return output

    # put input file into memory based on input format
    def read_file(self):
        readers = {
            "csv": self.read_csv_file,
            "geojson": self.read_json_file,
            "json": self.read_json_file,
            "xlsx": self.read_xlsx_file,
        }

        # use a custom reader if the config is nested
        if self.config.get("nested", None):
            nested_readers = nested_file_readers.nested_readers
            return nested_readers[self.config["url"]](self.data_path)

        if "zip" in self.config.keys():
            if self.config["zip"]:
                self.data_path = self.data_path[self.config["filename"]]

        return readers[self.config["format"].lower()]()

    # parse file attributes into correct data types in a dict based on input fields
    def parse_file(self, read_file):
        # init output
        output = []

        # list of functions used to convert input to desired data_type
        formatters = {
            "text": str,
            "int": self.clean_int,
            "float": self.clean_float,
            "timestamp": self.clean_date_format,
            "date": self.clean_date_format,
        }

        # if input is tabular, convert each column in each input row
        for row in read_file:
            new_row = {}

            # for each attribute ...
            for i in range(len(self.config["attributes"])):
                # map source column names to target column names, where needed
                if (
                    "source_name" in self.config["attributes"][i].keys()
                    and "target_name" in self.config["attributes"][i].keys()
                ):
                    self.config["attributes"][i]["id"] = self.config["attributes"][i][
                        "target_name"
                    ]
                    src = row[self.config["attributes"][i]["source_name"]]
                else:
                    src = row[self.config["attributes"][i]["id"]]

                # if date column, consider hardcoded format attribute
                if self.config["attributes"][i]["type"] in ["date", "timestamp"]:
                    new_row[self.config["attributes"][i]["id"]] = formatters[
                        self.config["attributes"][i]["type"]
                    ](src, self.config["attributes"][i].get("format", None))
                else:
                    new_row[self.config["attributes"][i]["id"]] = formatters[
                        self.config["attributes"][i]["type"]
                    ](src)
            output.append(new_row)

        logging.info("Cleaned {} records".format(len(output)))
        return output

    # put dict into datastore_create call
    def insert_into_datastore(self, resource_id, records):
        # map source column names to target column names, where needed
        for i in range(len(self.config["attributes"])):
            if (
                "source_name" in self.config["attributes"][i].keys()
                and "target_name" in self.config["attributes"][i].keys()
            ):
                self.config["attributes"][i]["id"] = self.config["attributes"][i][
                    "target_name"
                ]

        # Smaller datasets can all be loaded at once
        if len(records) < 200000:
            self.ckan.action.datastore_create(
                id=resource_id,
                fields=self.config["attributes"],
                records=records,
                force=True,
            )

        # Larger datasets, however, need to be loaded in chunks
        else:
            chunk_size = 2000 if "geometry" in records[0].keys() else 20000
            logging.info(
                "Dataset is too big to load all at once - loading in chunks of "
                + str(chunk_size)
            )
            for i in range((len(records) // chunk_size) + 1):
                if i % 10 == 0:
                    logging.info(
                        "Loading chunk {} out of {} with {} records".format(
                            str(i),
                            str(len(records) // chunk_size),
                            str(len(records[chunk_size * i: chunk_size * (i + 1)])),
                        )
                    )
                self.ckan.action.datastore_create(
                    id=resource_id,
                    fields=self.config["attributes"],
                    records=records[chunk_size * i: chunk_size * (i + 1)],
                    force=True,
                )

        logging.info("Loaded {} records".format(str(len(records))))
        return len(records)

    def execute(self, context):
        # init task instance from context
        ti = context["ti"]

        # assign important vars if provided from other tasks
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[
                self.resource_id_task_key
            ]

        if self.data_path_task_id and self.data_path_task_key:
            self.data_path = ti.xcom_pull(task_ids=self.data_path_task_id)[
                self.data_path_task_key
            ]

        # read and parse file
        read_file = self.read_file()
        parsed_data = self.parse_file(read_file)

        # put parsed_data into ckan datastore
        record_count = self.insert_into_datastore(self.resource_id, parsed_data)

        return {"Success": True, "record_count": record_count}


class DeltaCheckOperator(InsertDatastoreFromYAMLConfigOperator):
    """
    Input a reference to a CKAN datastore resource and a DownloadOperator
    Optional input a YAML config, where column name mappings are stored
    Output whether there is a difference in their columns and content
    """

    def __init__(
        self,
        resource_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_name = resource_name

    def execute(self, context):
        # init task instance from context
        ti = context["ti"]

        # assign important vars if provided from other tasks
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[
                self.resource_id_task_key
            ]

        if self.data_path_task_id and self.data_path_task_key:
            self.data_path = ti.xcom_pull(task_ids=self.data_path_task_id)[
                self.data_path_task_key
            ]

        # read and parse incoming data - parsed data is already trasnformed
        # and its columns are renamed to match what might be in the datastore
        # already
        read_file = self.read_file()
        parsed_data = self.parse_file(read_file)
        print("data parsed")

        # if the incoming and incumbent data are a different length, green
        # light an update
        try:
            datastore_resource = self.ckan.action.datastore_search(
                id=self.resource_id, limit=1
            )
            record_count = datastore_resource["total"]
        except Exception as e:
            logging.error(e)
            logging.info(
                "Resource {} isn't datastore - update existing dataset".format(
                    self.resource_name
                )
            )
            return "update_resource_" + self.resource_name

        if len(parsed_data) != record_count:
            logging.info(
                "Incoming record count {} != current count {}".format(
                    str(len(parsed_data)), str(record_count)
                )
            )
            return "update_resource_" + self.resource_name

        print("matching record counts")

        # remove _id column from existing datastore_resource
        datastore_record = datastore_resource["records"][0]
        del datastore_record["_id"]
        # if the data has different column names, green light an update
        # TODO: check for columns renamed in YAML processing
        if parsed_data[0].keys() != datastore_record.keys():
            logging.info("Current column names dont match incoming colnames")
            logging.info("Current attributes:" + str(datastore_record.keys()))
            logging.info("Incoming attributes:" + str(parsed_data[0].keys()))
            return "update_resource_" + self.resource_name

        print("matching column names")

        # If the dataset is "large", we dont do a record by record comparison
        # as we dont have enough memory in the EC2s right now (July 2022)
        if record_count > 500000:
            return "dont_update_resource_" + self.resource_name

        # Record by record comparison
        print("Starting record by record comparison")
        max_chunk_size = 32000
        datastore_resource = self.ckan.action.datastore_search(
            id=self.resource_id,
            limit=max_chunk_size,
            include_total=False,
            fields=list(datastore_record.keys()),
        )
        for incumbent_record in datastore_resource["records"]:
            if incumbent_record in parsed_data:
                continue
            elif incumbent_record not in datastore_resource["records"]:
                return "update_resource_" + self.resource_name

        # if the resource is too big to get in a single call, make multiple calls
        if record_count >= max_chunk_size:
            iteration = 1
            while len(datastore_resource["records"]):
                datastore_resource = self.ckan.action.datastore_search(
                    id=self.resource_id,
                    limit=max_chunk_size,
                    offset=max_chunk_size * iteration,
                    include_total=False,
                    fields=list(datastore_record.keys()),
                )
                for incumbent_record in datastore_resource["records"]:
                    if incumbent_record in parsed_data:
                        continue
                    elif incumbent_record not in datastore_resource["records"]:
                        return "update_resource_" + self.resource_name
                # datastore_resource["records"].append( next_chunk["records"] )
                iteration += 1

        return "dont_update_resource_" + self.resource_name


class CheckCkanResourceDescriptionOperator(BaseOperator):
    """
    Check if the description of a resource is missing
    """

    @apply_defaults
    def __init__(
            self,
            input_dag_id: str = None,
            address: str = None,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.input_dag_id = input_dag_id
        self.address = address

    def execute(self, context):
        ti = context["ti"]
        # Get full package name list
        package_list = ti.xcom_pull(task_ids=self.input_dag_id)
        result_info = {}
        for package_name in package_list:
            url = self.address + "api/3/action/package_show?id=" + package_name
            resources = requests.get(url).json()["result"]["resources"]
            # Iterate over all resources in a package
            for resource in resources:
                # Perform analysis only when datastore is active
                if str(resource["datastore_active"]) == "True":
                    # Send datastore_search request
                    url = (
                        self.address
                        + "api/3/action/datastore_search?resource_id="
                        + resource["id"]
                    )
                    fields = requests.get(url).json()["result"]["fields"]

                    # check "info" field exists and "notes" is not empty
                    field_flag = [
                        True
                        if ("info" in item.keys())
                        and (item["info"]["notes"] is not None)
                        else False
                        for item in fields
                    ]
                    # resource_flag == true, means that resource dont have ANY
                    # column name descriptions
                    resource_flag = all([flag is False for flag in field_flag])

                    if resource_flag:
                        logging.warning(
                            'Resource description MISSING!' +
                            'package id or name: {package_name} ' +
                            'resource id: {resource["id"]}'
                        )

                        # collect package and resource info for missing descriptions
                        if package_name in result_info:
                            result_info[package_name] = (
                                result_info[package_name] + ", " + resource["id"]
                            )
                        else:
                            result_info[package_name] = resource["id"]

                    else:
                        logging.info("Resource description OK!")
        return {"package_and_resource": result_info}
