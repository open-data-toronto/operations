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

        package_name_or_id: str = None,
        package_name_or_id_task_id: str = None,
        package_name_or_id_task_key: str = None,

        resource_name: str = None,
        resource_name_task_id: str = None,
        resource_name_task_key: str = None,

        resource_id: str = None,
        resource_id_task_id: str = None,
        resource_id_task_key: str = None,


        resource_attributes: str = None,
        #resource_id_filepath = None,

        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.package_name_or_id, self.package_name_or_id_task_id, self.package_name_or_id_task_key = package_name_or_id, package_name_or_id_task_id, package_name_or_id_task_key
        self.resource_name, self.resource_name_task_id, self.resource_name_task_key = resource_name, resource_name_task_id, resource_name_task_key
        self.resource_id, self.resource_id_task_id, self.resource_id_task_key = resource_id, resource_id_task_id, resource_id_task_key

        self.resource_attributes = resource_attributes
        # self.resource = None
        # self.resource_id_filepath = resource_id_filepath
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def _resource_exists(self):
        # get the package id from a task if its been provided from a task
        if self.package_name_or_id_task_id and self.package_name_or_id_task_key:
            self.package_name_or_id = ti.xcom_pull(task_ids=self.package_name_or_id_task_id)[self.package_name_or_id_task_key]

        package = self.ckan.action.package_show(id=self.package_name_or_id)

        for r in package["resources"]:
            # get resource name if provided from another task
            if self.resource_name_task_id and self.resource_name_task_key:
                self.resource_name = ti.xcom_pull(task_ids=self.resource_name_task_id)[self.resource_name_task_key]

            # find which resource in the package matches the input name
            # this search ignores suffixes to the resource name, like date ranges
            if r["name"] == self.resource_name or r["name"][:len(self.resource_name)] == self.resource_name:
                self.resource = r
                return True
             

        return False

    def execute(self, context):
        # get resource id from a task if its provided by another task
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id, key=self.resource_id_task_key)
        
        if self.resource_id is not None:
            logging.info("Resource ID used to get resource")
            resource = self.ckan.action.resource_show(id=self.resource_id)
        elif self._resource_exists():
            logging.info("Resource found using the package id {} and resource name {}". format(self.package_name_or_id, self.resource_name) )
            resource = self.resource
        else: 
            logging.info("Resource not found - creating a resource called {} in package {}".format(self.resource_name, self.package_name_or_id))
            resource = self.ckan.action.resource_create(
                package_name_or_id=self.package_name_or_id,
                name=self.resource_name,
                **self.resource_attributes,
            )

        logging.info(resource)

        self.resource_id = resource["id"]
        logging.info("Resource id: " + self.resource_id)

        # # write the resource id to an input filepath, if the filepath is given
        # if self.resource_id_filepath and self.resource_id:
        #     logging.info("Writing resource id to " + str(self.resource_id_filepath))
        #     f = open( self.resource_id_filepath, "w")
        #     f.write( self.resource_id )


        logging.info(f"Returning: {resource}")
        return resource

class EditResourceMetadataOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,

        resource_id: str = None,
        resource_id_task_id: str = None,
        resource_id_task_key: str = None,

        new_resource_name: str = None,
        new_resource_name_task_id: str = None,
        new_resource_name_task_key: str = None,

        last_modified = None,
        last_modified_task_id: str = None,
        last_modified_task_key: str = None,

        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        
        self.resource_id, self.resource_id_task_id, self.resource_id_task_key = resource_id, resource_id_task_id, resource_id_task_key
        self.new_resource_name, self.new_resource_name_task_id, self.new_resource_name_task_key = new_resource_name, new_resource_name_task_id, new_resource_name_task_key
        self.last_modified, self.last_modified_task_id, self.last_modified_task_key = last_modified, last_modified_task_id, last_modified_task_key
        
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def _datetime_to_string(self, datetime):
        return datetime.strftime("%Y-%m-%dT%H:%M:%S")

    def execute(self, context):
        # init task instance from context
        ti = context['ti']

        # assign vars if they come from another task
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[self.resource_id_task_key]

        if self.new_resource_name_task_id and self.new_resource_name_task_key:
            self.new_resource_name = ti.xcom_pull(task_ids=self.new_resource_name_task_id)[self.new_resource_name_task_key]

        if self.last_modified_task_id and self.last_modified_task_key:
            self.last_modified = ti.xcom_pull(task_ids=self.last_modified_task_id)[self.last_modified_task_key]

        # resource_patch api call below for non null input vars
        # last modified date
        if self.last_modified:
            last_modified = self._datetime_to_string(self.last_modified)
            logging.info("Setting last-modified of resource_id {} to {}".format(self.resource_id, last_modified))
            self.ckan.action.resource_patch(
                id=self.resource_id,
                last_modified=last_modified,
            )
        # resource_name
        if self.new_resource_name:
            logging.info("Setting name of resource_id {} to {}".format(self.resource_id, self.new_resource_name))
            self.ckan.action.resource_patch(
                id=self.resource_id,
                name=self.new_resource_name,
            )
        



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