import logging
from pathlib import Path
import time

import ckanapi
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dateutil import parser
from datetime import datetime
from utils import misc_utils


class GetOrCreateResourceOperator(BaseOperator):
    """
    Returns CKAN resource object, or creates it if it does not exist

    Expects as inputs:
    - address: CKAN instance URL
    - apikey: CKAN API key 
    - package_name_or_id: the name or id of the package wherein we'll look for/make a resource
    - resource_name: the name of the resource this operator will look for/make
    - resource_id: the resource_id this operator will look for/make
    - resource_attributes: the attributes that will be in a resource if we create it

    Each of the above (except resource attributes, address and apikey) can be given with an actual value or with a reference to a task_id and task_key that returns the value
    """

    @apply_defaults
    def __init__(
        self,

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

        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.package_name_or_id, self.package_name_or_id_task_id, self.package_name_or_id_task_key = package_name_or_id, package_name_or_id_task_id, package_name_or_id_task_key
        self.resource_name, self.resource_name_task_id, self.resource_name_task_key = resource_name, resource_name_task_id, resource_name_task_key
        self.resource_id, self.resource_id_task_id, self.resource_id_task_key = resource_id, resource_id_task_id, resource_id_task_key

        self.resource_attributes = resource_attributes
        
    def _resource_exists(self):
        # get the package id from a task if its been provided from a task
        if self.package_name_or_id_task_id and self.package_name_or_id_task_key:
            self.package_name_or_id = ti.xcom_pull(task_ids=self.package_name_or_id_task_id)[self.package_name_or_id_task_key]

        package = self.ckan.action.package_show(id=self.package_name_or_id)

        for r in package["resources"]:
            # get resource name if provided from another task
            if self.resource_name_task_id and self.resource_name_task_key:
                self.resource_name = ti.xcom_pull(task_ids=self.resource_name_task_id)[self.resource_name_task_key]

            # find which resource in the package matches the input resource name
            if r["name"] == self.resource_name:
                self.resource = r
                return True
             

        return False

    def execute(self, context):

        self.ckan = misc_utils.connect_to_ckan()

        # get resource id from a task if its provided by another task
        if self.resource_id_task_id and self.resource_id_task_key:
            self.resource_id = ti.xcom_pull(task_ids=self.resource_id_task_id)[self.resource_id_task_key]
        
        # branching to determine whether resource is found or created, set resource["is_new"] for branching in dags
        if self.resource_id is not None:
            logging.info("Resource ID used to get resource")
            resource = self.ckan.action.resource_show(id=self.resource_id)
            if resource['datastore_active']:
                resource["is_new"] = False
            else:
                resource["is_new"] = True   # considered as new if there is no datastore data
        elif self._resource_exists():
            logging.info("Resource found using the package id {} and resource name {}". format(self.package_name_or_id, self.resource_name) )
            resource = self.resource
            if resource['datastore_active']:
                resource["is_new"] = False
            else:
                resource["is_new"] = True   # considered as new if there is no datastore data
        else: 
            # create a resource and check that its created - if it isnt, then create it again
            # try to make the resource
            logging.info("Resource not found - creating a resource called {} in package {}".format(self.resource_name, self.package_name_or_id))
            resource = self.ckan.action.resource_create(
                package_name_or_id=self.package_name_or_id,
                name=self.resource_name,
                **self.resource_attributes,
            )
            resource["is_new"] = True

            while True:
                # try to get that created resource
                try:
                    # pause a moment before checking that the resource is truly loaded - sometimes a resource_create call reports success but never made a resource
                    logging.info("Pausing a moment before checking that the resource is truly loaded")
                    time.sleep(10)
                    logging.info("Fetching resource to make sure it was created successfully...")
                    assert self.ckan.action.resource_show(id=resource["id"])
                    break

                # but if we cant, make it again and repeat the cycle
                except:
                    logging.info("Resource not made successfully - trying again ... ")
                    logging.info("Resource not found - creating a resource called {} in package {}".format(self.resource_name, self.package_name_or_id))
                    resource = self.ckan.action.resource_create(
                        package_name_or_id=self.package_name_or_id,
                        name=self.resource_name,
                        **self.resource_attributes,
                    )
                    resource["is_new"] = True

        self.resource_id = resource["id"]
        logging.info(f"Returning: {resource}")

        return resource


class EditResourceMetadataOperator(BaseOperator):
    """
    Edits a resource's name or last_modified date

    Expects as inputs:
    - address: CKAN instance URL
    - apikey: CKAN API key
    - resource_id - the id of the ckan resource whose metadata will be updated
    - new_resource_name - the new name of the resource
    - last_modified - the last_modified date of the resource

    Only one of name or last_modified is required
    either of these can be given with an actual value, or with a reference to a task_id and task_key that returns the value
    """
    @apply_defaults
    def __init__(
        self,

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
        

    def _datetime_to_string(self, datetime):
        try:
            output = datetime.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception as e:
            output = datetime
        return output


    def execute(self, context):

        self.ckan = misc_utils.connect_to_ckan()

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

    def _datetime_to_string(self, datetime):
        return datetime.strftime("%Y-%m-%dT%H:%M:%S")

    def execute(self, context):

        self.ckan = misc_utils.connect_to_ckan()

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