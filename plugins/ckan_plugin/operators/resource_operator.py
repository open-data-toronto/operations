import logging
from pathlib import Path

import ckanapi
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dateutil import parser


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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.package_id = package_name_or_id
        self.resource_name = resource_name
        self.resource_id = resource_id
        self.resource_attributes = resource_attributes
        self.resource = None
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def _resource_exists(self):
        package = self.ckan.action.package_show(id=self.package_id)

        for r in package["resources"]:
            if r["name"] == self.resource_name:
                self.resource = r
                return True

        return False

    def execute(self, context):
        if self.resource_id is not None:
            resource = self.ckan.action.resource_show(id=self.resource_id)
        elif self._resource_exists():
            resource = self.resource
        else:
            resource = self.ckan.action.resource_create(
                package_id=self.package_id,
                name=self.resource_name,
                **self.resource_attributes,
            )

        logging.info(f"Returning: {resource}")
        return resource


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

        file_last_modified = parser.parse(download_file_info["last_modified"])

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