import json
import csv
import logging
import sys

from typing import List
from utils import misc_utils
from pathlib import Path

from ckan_operators import nested_file_readers

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
        resource_task_id: str,
        dir_task_id: str,
        sort_columns: List[str] = [],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dir_task_id = dir_task_id
        self.resource_task_id = resource_task_id
        self.sort_columns = sort_columns
        

    def _checksum_datastore_response(self, datastore_response):

        import hashlib
        import pandas as pd

        data = pd.DataFrame(datastore_response["records"])
        if "_id" in data.columns.values:
            data = data.drop("_id", axis=1)
        if len(self.sort_columns) > 0:
            data = data.sort_values(by=self.sort_columns)

        data_hash = hashlib.md5()
        data_hash.update(data.to_csv(index=False).encode("utf-8"))

        return data_hash.hexdigest()

    def _build_dataframe(self, records):
        import pandas as pd

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

        return str(fields_file_path)

    def _save_data_parquet(self, datastore_response, checksum, backups_dir, data):
        data_file_path = backups_dir / f"data.{checksum}.parquet"

        if not data_file_path.exists():
            data.to_parquet(
                path=data_file_path,
                engine="fastparquet",
                compression=None)

        return str(data_file_path)

    def execute(self, context):

        import pandas as pd

        self.ckan = misc_utils.connect_to_ckan()

        # get a resource and backup directory via xcom
        ti = context["ti"]
        resource = ti.xcom_pull(task_ids=self.resource_task_id)
        backups_dir = ti.xcom_pull(task_ids=self.dir_task_id)
        backups_dir = Path(backups_dir)

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
        

    def execute(self, context):

        self.ckan = misc_utils.connect_to_ckan()

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
            logging.error("Error while trying to delete resource: " + str(e))
            if str(e) == "NotFound":
                logging.error(self.resource_id + " is not a datastore resource")


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
        backup_task_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.backup_task_id = backup_task_id


    def execute(self, context):

        self.ckan = misc_utils.connect_to_ckan()

        backups_info = context["ti"].xcom_pull(task_ids=self.backup_task_id)

        self.ckan.action.datastore_delete(id=backups_info["resource_id"], force=True)

        with open(backups_info["fields_file_path"], "r") as f:
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
        
    def _create_empty_resource_with_fields(self, fields_path, resource_id):
        with open(fields_path, "r") as f:
            fields = json.load(f)

        self.ckan.action.datastore_create(id=resource_id, fields=fields, force=True)

    def execute(self, context):

        import pandas as pd

        self.ckan = misc_utils.connect_to_ckan()

        ti = context["ti"]
        resource = ti.xcom_pull(task_ids=self.resource_task_id)

        if self.fields_json_path_task_id is not None:
            fields_path = ti.xcom_pull(task_ids=self.fields_json_path_task_id)
            self._create_empty_resource_with_fields(fields_path, resource["id"])

        if self.parquet_filepath_task_id is not None:
            path = ti.xcom_pull(task_ids=self.parquet_filepath_task_id)

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
        backup_task_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.backup_task_id = backup_task_id

    def execute(self, context):

        import pandas as pd

        self.ckan = misc_utils.connect_to_ckan()

        backups_info = context["ti"].xcom_pull(task_ids=self.backup_task_id)

        assert backups_info is not None, "No backup information"

        resource_id = backups_info["resource_id"]

        with open(backups_info["fields_file_path"], "r") as f:
            fields = json.load(f)

        data = pd.read_parquet(backups_info["data_file_path"])
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


class InsertDatastoreFromYAMLConfigOperator(BaseOperator):
    """
    Inserts a file's data into a datastore resource based YAML config
    """

    @apply_defaults
    def __init__(
        self,
        resource_id: str = None,
        resource_id_task_id: str = None,
        resource_id_task_key: str = None,
        data_path: str = None,
        data_path_task_id: str = None,
        data_path_task_key: str = None,
        config: dict = {},
        do_not_cache: bool = False,
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
        self.do_not_cache = do_not_cache

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
            "%b %d, %Y": "%Y-%m-%d",
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
        import codecs
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
        import openpyxl
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
                column_names[i]: str(row[i].value).strip()
                for i in range(len(row))
            }
            if self.geometry_needs_parsing:
                output_row["geometry"] = json.dumps(
                    {
                        "type": "Point",
                        "coordinates": [
                            row[longitude_attribute],
                            row[latitude_attribute],
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

        if self.config.get("agol", None):
            return readers["csv"]()

        return readers[self.config["format"].lower()]()

    # parse file attributes into correct data types in a dict based on input fields
    def parse_file(self, read_file):
        # init output
        output = []

        # list of functions used to convert input to desired data_type
        formatters = {
            "text": str,
            "int": misc_utils.clean_int,
            "float": misc_utils.clean_float,
            "timestamp": misc_utils.clean_date_format,
            "date": misc_utils.clean_date_format,
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
                do_not_cache=self.do_not_cache,
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

        self.ckan = misc_utils.connect_to_ckan()

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

        return {"success": True, "record_count": record_count}


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

        self.ckan = misc_utils.connect_to_ckan()
        
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
            return {"needs_update": True}

        if len(parsed_data) != record_count:
            logging.info(
                "Incoming record count {} != current count {}".format(
                    str(len(parsed_data)), str(record_count)
                )
            )
            return {"needs_update": True}

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
            return {"needs_update": True}

        print("matching column names")

        # If the dataset is "large", we dont do a record by record comparison
        # as we dont have enough memory in the EC2s right now (July 2022)
        if record_count > 500000:
            return {"needs_update": False}

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
                return {"needs_update": True}

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
                        return {"needs_update": True}
                # datastore_resource["records"].append( next_chunk["records"] )
                iteration += 1

        return {"needs_update": False}


class CSVStreamToDatastoreYAMLOperator(BaseOperator):
    """
    Streams a CSV file's data into a datastore resource based YAML config
    """

    @apply_defaults
    def __init__(
        self,
        resource_id: str = None,
        resource_id_task_id: str = None,
        resource_id_task_key: str = None,
        data_path: str = None,
        data_path_task_id: str = None,
        data_path_task_key: str = None,
        config: dict = {},
        do_not_cache: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_id = resource_id
        self.resource_id_task_id = resource_id_task_id
        self.resource_id_task_key = resource_id_task_key
        self.data_path = data_path
        self.data_path_task_id = data_path_task_id
        self.data_path_task_key = data_path_task_key
        self.do_not_cache = do_not_cache
         
        self.config = config

        # this var will be reset later if there are geometry fields that need 
        # to be put into a geometry object
        self.geometry_needs_parsing = False

        # ensure CSVs can be really wide
        csv.field_size_limit(sys.maxsize)


    def read_file(self):
        '''reads CSV at input filepath and returns generator'''
        
        # make sure input filepath is CSV
        #assert self.data_path.endswith(".csv")

        # get fieldnames from first row of CSV
        with open(self.data_path, "r", encoding="windows-1252") as f:
            self.fieldnames = next(csv.reader(f))
            f.close()

        # return generator        
        return misc_utils.csv_to_generator(self.data_path, self.fieldnames)

    def does_geometry_need_parsing(self):
        '''determines if geometric attributes need to be parsed into a 
        geometry object, then finds those geometric attributes'''
        

        latitude_attributes = ["lat", "latitude", "y", "y coordinate"]
        longitude_attributes = ["long", "longitude", "x", "x coordinate"]

        # check if there are supposed to be geometric fields
        if "geometry" not in self.fieldnames and "geometry" in [
            attr.get("id", None) for attr in self.config["attributes"]
        ]:
            # geometry needs to be parsed 
            # lets find which columns hold geometric data
            self.geometry_needs_parsing = True

            for attr_index in range(len(self.fieldnames)):
                if self.fieldnames[attr_index].lower() in latitude_attributes:
                    self.latitude_attribute = self.fieldnames[attr_index]

                if self.fieldnames[attr_index].lower() in longitude_attributes:
                    self.longitude_attribute = self.fieldnames[attr_index]


    def parse_data(self, input_data):
        '''receives one row of data as a dict, returns CKAN-friendly data'''
        
        output = {} 

        # map geometric columns to geometry column, if needed
        if self.geometry_needs_parsing:
            if input_data[self.longitude_attribute] and input_data[self.latitude_attribute]:
                output["geometry"] = json.dumps(
                    {
                        "type": "Point",
                        "coordinates": [
                            float(input_data[self.longitude_attribute]),
                            float(input_data[self.latitude_attribute]),
                        ],
                    }
                )
            else:
                output["geometry"] = json.dumps(
                    {"type": "Point","coordinates": [None,None]})


        # list of functions used to convert input to desired data_type
        formatters = {
            "text": str,
            "int": misc_utils.clean_int,
            "float": misc_utils.clean_float,
            "timestamp": misc_utils.clean_date_format,
            "date": misc_utils.clean_date_format,
            "json": json.loads
        }


        for i in range(len(self.config["attributes"])):
            # if geometry was parsed above, we dont need to do it again
            if self.geometry_needs_parsing and self.config["attributes"][i]["id"] == "geometry":
                continue

            # map source column names to target column names, where needed
            if (
                "source_name" in self.config["attributes"][i].keys()
                and "target_name" in self.config["attributes"][i].keys()
            ):
                self.config["attributes"][i]["id"] = self.config["attributes"][i][
                    "target_name"
                ]
                src = input_data[self.config["attributes"][i]["source_name"]]
            else:
                src = input_data[self.config["attributes"][i]["id"]]

            # massage data values so CKAN will ingest it

            # if date column, consider hardcoded format attribute
            if self.config["attributes"][i]["type"] in ["date", "timestamp"]:
                output[self.config["attributes"][i]["id"]] = formatters[
                    self.config["attributes"][i]["type"]
                ](src, self.config["attributes"][i].get("format", None))
            # if not a date column, consider another formatter
            else:
                output[self.config["attributes"][i]["id"]] = formatters[
                    self.config["attributes"][i]["type"]
                ](src)
        
        return output


    def insert_into_ckan(self, records):
        '''receives data as list of dicts, puts that data in CKAN'''

        self.ckan.action.datastore_create(
            id=self.resource_id,
            fields=self.config["attributes"],
            records=records,
            force=True,
            do_not_cache=self.do_not_cache
        )
        

    def execute(self, context):

        self.ckan = misc_utils.connect_to_ckan()

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
        
        logging.info(self.data_path)

        # init csv generator
        csv_generator = self.read_file()

        # determine whether we need to parse geometry
        self.does_geometry_need_parsing()

        # init some vars so batch calls to CKAN API
        total_count = 0
        this_count = 0
        this_batch = []
        batch_size = 20000

        for row in csv_generator:
            total_count += 1
            this_count += 1
            this_batch.append(self.parse_data(row))

            # when this batch is the max size, insert the data into CKAN
            if len(this_batch) >= batch_size:
                logging.info("Loading {}th records".format(str(total_count)))
                self.insert_into_ckan(this_batch)
                this_batch = []
                this_count = 0

        # insert the last batch into CKAN
        if len(this_batch) != 0:
            logging.info("Loading last records")
            self.insert_into_ckan(this_batch)

        logging.info("Inserted {} records into CKAN".format(str(total_count)))

        return {"success": True, "record_count":str(total_count)}


class DatastoreCacheOperator(BaseOperator):
    """
    Streams a CSV file's data into a datastore resource based YAML config
    """

    @apply_defaults
    def __init__(
        self,
        resource_id: str = None,
        resource_id_task_id: str = None,
        resource_id_task_key: str = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_id = resource_id
        self.resource_id_task_id = resource_id_task_id
        self.resource_id_task_key = resource_id_task_key

    def execute(self, context):

        self.ckan = misc_utils.connect_to_ckan()

        # init task instance from context
        ti = context["ti"]

        # assign important vars if provided from other tasks
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[
                self.resource_id_task_key
            ]

        self.ckan.action.datastore_cache(
            resource_id=self.resource_id,
        )



'''
This Modules contains the classes operating at datastore resource level.

Classes:
- DeleteDatastoreResource: delete a datastore resource 
'''

class DeleteDatastoreResource:
    """
    Delete a datastore resource

    Attributes:
    - resource_id: str
        Datastore resource ID; it is in form of a hash-string usually.
    - keep_schema: bool
        Wheter to keep the schema of the datastore resource after deleting the records.
        Defaults to False. 
    - ckan:
        CKAN API connector. This attribute comes from misc_utils module. 
    
    Methods:
    - delete_datastore_resource()

    """

    def __init__(
        self,
        resource_id: str,
        keep_schema: bool = False,
    ) -> None:
        """
        Initialize a new instance of the DeleteDatastoreResource class
        and construct all the necessary attributes.

        Arguments:
        - resource_id: str
            Datastore resource ID; it is in form of a hash-string usually.
        - keep_schema: bool (optional; Defaults to False.)
            Wheter to keep the schema of the datastore resource after
            deleting the records.
        
        Returns: None
        """
        self.resource_id = resource_id
        self.keep_schema = keep_schema
        # Connect to the CKAN API
        self.ckan = misc_utils.connect_to_ckan()

    def delete_datastore_resource(self):
        """
        Delete a given datastore rescouce id by calling ckan.action.datastore_delete.

        The method keeps the resource schema is the keep_schema= True.
        An exeption will be raised if any error occurs. 
        A special error will be raised if the provided DS resource_id is not found in the CKAN database.
        
        Args: None

        Returns: None
        """
        try:
            if self.keep_schema is True:
                self.ckan.action.datastore_delete(
                    resource_id = self.resource_id,
                    filters = {},
                    force = True)
                logging.info(f"SUCCESSFULLY DELETED the datastore resource with ID = {self.resource_id} while keeping the schema")
            else:
                self.ckan.action.datastore_delete(
                    resource_id = self.resource_id,
                    force = True)
                logging.info(f"SUCCESSFULLY DELETED the datastore resource with ID = {self.resource_id}")

        except Exception as e:
            logging.error(f"Error while trying to delete resource: {str(e)}")
            if str(e) == "NotFound":
                logging.error(f"{self.resource_id} is not a datastore resource")


import codecs
from typing import Dict, List, Generator

def stream_to_datastore(
    resource_id: str,
    file_path: str,
    attributes: Dict,
    primary_key: None,
    encoding: str = "latin1",
    batch_size: int = 20000,
    do_not_cache: bool = False,
    **kwargs,
) -> None:
    """
    Stream records from csv; insert records into ckan datastore in batch
    based on yaml config attributes.

    Parameters:
    - resource_id : str
        the id of the datastore resource
    - file_path : str
        the path of data file
    - attributes : Dict
        the attributes section of yaml config (data fields)
    - primary_key : str, Optional
        the unique key of datastore
    - encoding: str, Optional
        the encoding form of data file
    - batch_size: int, Optional
        total number of records in a batch
    - do_not_cache: bool, Optional
        indicate how datastore_create store records when insert in batch

    Returns:
        None

    """
    logging.info("---------Start Streaming to CKAN---------------")

    def read_csv(file_path: str) -> Generator:
        """
        reads CSV at input filepath and returns generator
        Parameters:
        - file_path : str
            the path of data file

        Returns:
            data : Generator
        """

        # grab fieldnames from csv
        with open(file_path, "r", encoding=encoding) as f:
            fieldnames = next(csv.reader(f))

        return misc_utils.csv_to_generator(file_path, fieldnames, encoding)

    def insert_into_ckan(records: List) -> None:
        """
        receives data as list of dicts, puts that data in CKAN
        Parameters:
        - records : List
            data records as list of dicts

        Returns:
            None
        """
        working_attributes = misc_utils.parse_source_and_target_names(attributes)

        # insert data into ckan
        ckan.action.datastore_create(
            id=resource_id,
            fields=working_attributes,
            records=records,
            force=True,
            do_not_cache=do_not_cache,
        )

    def upsert_into_ckan(records: List) -> None:
        """
        receives data as list of dicts, puts that data in CKAN
        Parameters:
        - records : List
            data records as list of dicts

        Returns:
            None
        """
        working_attributes = misc_utils.parse_source_and_target_names(attributes)

        # check if datastore_active (datastore exists)
        datastore_active = ckan.action.resource_show(id=resource_id)["datastore_active"]

        if str(datastore_active).lower() == "true":
            ckan.action.datastore_upsert(
                resource_id=resource_id,
                records=records,
                force=True,
            )
        else:
            logging.info("Resource not exists, create resource first.")
            logging.info(f"Creating datastore_resource: {resource_id}")
            ckan.action.datastore_create(
                id=resource_id,
                records=records,
                primary_key=primary_key,
                fields=working_attributes,
                force=True,
                do_not_cache=True,
            )

    csv.field_size_limit(sys.maxsize)
    ckan = misc_utils.connect_to_ckan()

    # init csv generator
    csv_generator = read_csv(file_path)

    # init counter vars, make calls to CKAN API in batch
    total_count = 0
    curr_batch_count = 0
    records = []
    if primary_key:
        logging.info(f"Primary key is: {primary_key}")

    for row in csv_generator:
        total_count += 1
        curr_batch_count += 1
        records.append(row)

        # when this batch is the max size, insert the data into CKAN
        if len(records) >= batch_size:
            logging.info("Loading {}th records".format(str(total_count)))

            if primary_key:
                # if primary key has value; use upsert method
                logging.info(f"Upserting datastore_resource...")
                upsert_into_ckan(records)
            else:
                # regular insert into ckan method
                logging.info(f"Inserting datastore_resource...")
                insert_into_ckan(records)

            # reset current batch
            records.clear()
            curr_batch_count = 0

    # insert the last batch into CKAN
    if len(records) != 0:
        logging.info(f"Loading last {len(records)} records")

        if primary_key:
            # if primary key has value; use upsert method
            logging.info(f"Upserting datastore_resource...")
            upsert_into_ckan(records)
        else:
            # regular insert into ckan method
            logging.info(f"Inserting datastore_resource...")
            insert_into_ckan(records)

    # make sure datastore_active is set to True
    datastore_active = ckan.action.resource_show(id=resource_id)["datastore_active"]
    if str(datastore_active).lower() != "true":
        ckan.action.resource_patch(
            id=resource_id,
            datastore_active=True,
        )
        logging.info("Manually set datastore_active to True.")

    logging.info(f"Inserted {total_count} records into CKAN")

    return {"success": True, "record_count": str(total_count)}
