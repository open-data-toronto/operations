import hashlib
import json
import csv
import logging
import codecs
from datetime import datetime
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
            data.to_parquet(path=data_file_path, engine="fastparquet", compression=None)

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

        # return filepath for fields json, data parquet, row/col counts, checksum, and resource_id
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

        Resource id can be given with n actual value, or with a reference to a task_id and task_key that returns the value
    
        Note: Deleting the entire resource also deletes the data dictionary (i.e. schema, field definitions and types). 
        To keep the existing schema, delete the datastore resource records instead by using the DeleteDatastoreResourceRecordsOperator - this keeps the schema.
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
        self.resource_id, self.resource_id_task_id, self.resource_id_task_key = resource_id, resource_id_task_id, resource_id_task_key
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)


    def execute(self, context):
        # get task instance from context
        ti = context['ti']

        # get resource id from task, if task info provided in input
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[self.resource_id_task_key]
            self.resource = ti.xcom_pull(task_ids=self.resource_id_task_id)
            logging.info(self.resource)
            logging.info("Pulled {} from {} via xcom".format(self.resource_id, self.resource_id_task_id) )

        assert self.resource_id, "Resource ID is empty! This operator needs a way to get the resource ID in order to delete the right datastore resource!"
        # Delete the resource
        try:
            self.ckan.action.datastore_delete(id=self.resource_id)
            logging.info("Deleted " + self.resource_id)

        except Exception as e:
            logging.error("Error while trying to delete resource: " + e)


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
            logging.error(e)

        result = self.ckan.action.datastore_create(
            id=resource_id, fields=fields, records=records
        )

        logging.info(f"Result: {result}")

        return result


class InsertDatastoreResourceRecordsFromJSONOperator(BaseOperator):
    '''
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
    - fields_path - location of the data's fields, already in a CKAN-friendly format

    All of the above, except the address and apikey, can be given with an actual value, or with a reference to a task_id and task_key that returns the value
    '''
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
        self.resource_id, self.resource_id_task_id, self.resource_id_task_key = resource_id, resource_id_task_id, resource_id_task_key
        self.data_path, self.data_path_task_id, self.data_path_task_key = data_path, data_path_task_id, data_path_task_key
        self.fields_path, self.fields_path_task_id, self.fields_path_task_key = fields_path, fields_path_task_id, fields_path_task_key
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)


    def execute(self, context):
        # init task instance from context
        ti = context['ti']

        # assign important vars if provided from other tasks
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[self.resource_id_task_key]

        if self.data_path_task_id and self.data_path_task_key:
            self.data_path = ti.xcom_pull(task_ids=self.data_path_task_id)[self.data_path_task_key]

        if self.fields_path_task_id and self.fields_path_task_key:
            self.fields_path = ti.xcom_pull(task_ids=self.fields_path_task_id)[self.fields_path_task_key]


        # get fields from file
        with open(self.fields_path, "r") as f:
            fields = json.load(f)
            logging.info("Loaded the following fields from {}: {}".format( self.fields_path, fields ))

        # populate that resource w data from the path provided
        assert self.data_path, "Data path, or the filepath to the data to be inserted, must be provided!"
        with open(self.data_path) as f:
            data = json.load(f)
        
        logging.info("Data parsed from JSON file")
        logging.info("Fields from fields file: " + str(fields))
        logging.info("Fields from data file: " + str(data[0].keys()))

        self.ckan.action.datastore_create(id=self.resource_id, fields=fields, records=data)
        logging.info("Resource created and populated from input fields and data")

        return {"resource_id": self.resource_id, "data_inserted": len(data)}


class InsertDatastoreFromYAMLConfigOperator(BaseOperator):
    """
    Inserts a file's data into a datastore resource based on specs from a YAML file
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

        fields: dict = None,
        fields_task_id: str = None,
        fields_task_key: str = None,

        format: str = None,
        format_task_id: str = None,
        format_task_key: str = None,

        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_id, self.resource_id_task_id, self.resource_id_task_key = resource_id, resource_id_task_id, resource_id_task_key
        self.data_path, self.data_path_task_id, self.data_path_task_key = data_path, data_path_task_id, data_path_task_key
        self.fields, self.fields_task_id, self.fields_task_key = fields, fields_task_id, fields_task_key
        self.format, self.format_task_id, self.format_task_key = format, format_task_id, format_task_key
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)


    def str_to_datetime(self, input):
        # loops through the list of formats and tries to return an input string into a datetime of one of those formats
        assert isinstance(input, str), "Utils str_to_datetime() function can only receive strings - it instead received {}".format(type(input))
        for format in [
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d-%b-%Y",
            "%d-%^b-%Y"
        ]:
            try:
                output = datetime.strptime(input, format)
                return output
            except ValueError:
                pass
        logging.error("No valid datetime format in utils.str_to_datetime() for input string {}".format(input))

    # reads a csv and returns a list of dicts - one dict for each row
    def read_csv_file(self, filepath):
        output = []
        dictreader = csv.DictReader(codecs.open(filepath, "rbU", "latin1"))
        for row in dictreader:
            # strip each attribute name - CKAN requires it
            output_row = {}
            for attr in row.keys():
                output_row[ attr.strip() ] = row[attr]
            output.append(output_row)

        #f.close()
        logging.info("Read {} records from {}".format( len(output), filepath))
        return output

    # put input file into memory based on input format
    def read_file(self, data_path, format):
        readers = {
            "csv": self.read_csv_file,
        }

        return  readers[format](data_path)
    
    
    # parse file attributes into correct data types in a dict based on input fields
    def parse_file(self, read_file, format, fields):

        # init output
        output = []

        # list of functions used to convert input to desired data_type
        formatters = {
            "text": str,
            "int": int,
            "float": float,
            "timestamp": self.str_to_datetime,
        }

        # convert each column in each row
        for row in read_file:
            new_row = {}
            for field in fields:
                src = row[field["id"]]
                new_row[field["id"]] = formatters[ field["type"] ](src)
            output.append( new_row )

        logging.info( "Cleaned {} records".format(len(output)) )
        return output
                

    # put dict into datastore_create call
    def insert_into_datastore(self, resource_id, fields, records):
        self.ckan.action.datastore_create( id=resource_id, fields=fields, records=records )

    def execute(self, context):
        # init task instance from context
        ti = context['ti']

        # assign important vars if provided from other tasks
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[self.resource_id_task_key]

        if self.data_path_task_id and self.data_path_task_key:
            self.data_path = ti.xcom_pull(task_ids=self.data_path_task_id)[self.data_path_task_key]

        if self.fields_task_id and self.fields_task_key:
            self.fields = ti.xcom_pull(task_ids=self.fields_task_id)[self.fields_task_key]

        # read and parse file
        read_file = self.read_file(self.data_path, self.format)
        parsed_data = self.parse_file(read_file, self.format, self.fields)

        # put parsed_data into ckan datastore
        self.insert_into_datastore(self.resource_id, self.fields, parsed_data)

        return {"Success": True}