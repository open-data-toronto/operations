import logging
from pathlib import Path

import ckanapi
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dateutil import parser
from datetime import datetime

class GetOrCreateResourceOperator(BaseOperator):
    """
    Returns CKAN resource object, or creates it if it does not exist 
    """

    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        package_name_or_id: str,
        resource_name: str = None,
        resource_id: str = None,
        resource_attributes: str = None,
        resource_id_filepath = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.package_id = package_name_or_id
        self.resource_name = resource_name
        self.resource_id = resource_id
        self.resource_attributes = resource_attributes
        self.resource = None
        self.resource_id_filepath = resource_id_filepath
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def _resource_exists(self):
        package = self.ckan.action.package_show(id=self.package_id)

        for r in package["resources"]:
            # find which resource in the package matches the input name
            # this search ignores suffixes to the resource name, like date ranges
            if r["name"] == self.resource_name or r["name"][:len(self.resource_name)] == self.resource_name:
                self.resource = r
                return True

        return False

    def execute(self, context):
        if self.resource_id is not None:
            logging.info("Resource ID used to get resource")
            resource = self.ckan.action.resource_show(id=self.resource_id)
        elif self._resource_exists():
            logging.info("Resource found using package id")
            resource = self.resource
        else: 
            logging.info("Resource not found - creating resource: " + self.resource_name)
            resource = self.ckan.action.resource_create(
                package_id=self.package_id,
                name=self.resource_name,
                **self.resource_attributes,
            )

        logging.info("Received resource")
        logging.info(resource)

        self.resource_id = resource["id"]
        logging.info("Received resource id: " + self.resource_id)

        # write the resource id to an input filepath, if the filepath is given
        if self.resource_id_filepath and self.resource_id:
            logging.info("Writing resource id to " + str(self.resource_id_filepath))
            f = open( self.resource_id_filepath, "w")
            f.write( self.resource_id )


        logging.info(f"Returning: {resource}")
        return resource

class UpdateResourceMetadataOperator():
    ## TODO
    """
    input: 
        - resource_id = a ckan resource id
        - address = details for correct CKAN env
        - apikey = details for correct CKAN env
        - ckan resource metadata attributes to be updated, like "name" or "last_modified", prefixed with "new_"
            ... ex "new_name", "new_last_modified"

    output:
        - runs resource_patch with the inputs
        - currently only works for last_modified and name
        
    """

    @apply_defaults
    def __init__(
        self,
        resource_id_filepath: str = None,
        apikey: str = None,
        address: str = None,
        new_last_modified = None,
        new_name = None
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        with open(self.resource_id_filepath, 'r') as f:
            self.resource_id = f.read()
        self.new_name = new_name
        self.new_last_modified = last_modified
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def _resource_exists(self):
        # request the resource from CKAN, return whether the request succeeded
        check = self.ckan.action.resource_show(id=self.resource_id)
        return check["success"]

    def execute(self, context):
        if self._resource_exists():
            if self.new_name:
                self.ckan.action.resource_patch(
                id=self.resource_id,
                name=self.new_name
            )
            if self.new_last_modified:
                self.ckan.action.resource_patch(
                id=self.resource_id,
                last_modified=self.new_last_modified
            )

            
        else:
            logging.error("The input resource id doesnt have a corresponding resource in CKAN")

class ResourceAndFileOperator(BaseOperator):
    """
            - download_file_task_id: task_id that returns the download file info (ie. DownloadFileOperator)
            - resource_task_id: task_id that returns resource object (ie. GetOrCreateResourcePackage)
    """

    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        resource_task_id: str,
        download_file_task_id: str,
        sync_timestamp: bool,
        upload_to_ckan: bool,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_task_id = resource_task_id
        self.download_file_task_id = download_file_task_id
        self.sync = sync_timestamp
        self.upload = upload_to_ckan
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def _datetime_to_string(self, datetime):
        return datetime.strftime("%Y-%m-%dT%H:%M:%S")

    def execute(self, context):
        ti = context["ti"]
        resource = ti.xcom_pull(task_ids=self.resource_task_id)
        download_file_info = ti.xcom_pull(task_ids=self.download_file_task_id)

        if self.upload:
            self.ckan.action.resource_patch(
                id=self.resource["id"],
                upload=open(Path(download_file_info["path"]), "rb"),
            )

        try:
            file_last_modified = parser.parse(download_file_info["last_modified"])
        except:
            file_last_modified = parser.parse(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + " GMT")
        
        resource_last_modified = parser.parse(
            (
                resource["last_modified"]
                if resource["last_modified"]
                else resource["created"]
            )
            + " UTC"
        )

        difference_in_seconds = abs(
            file_last_modified.timestamp() - resource_last_modified.timestamp()
        )

        if self.sync and difference_in_seconds > 0:
            self.ckan.action.resource_patch(
                id=resource["id"],
                last_modified=self._datetime_to_string(file_last_modified),
            )

        result = {
            "resource_last_modified": self._datetime_to_string(resource_last_modified),
            "file_last_modified": self._datetime_to_string(file_last_modified),
            "timestamps_were_synced": self.sync and difference_in_seconds > 0,
            "difference_in_seconds": difference_in_seconds,
        }

        logging.info(f"Returning: {result}")

        return result
