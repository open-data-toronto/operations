import tempfile
from pathlib import Path

import ckanapi
import requests
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
            return self.ckan.action.resource_show(id=self.resource_id)

        if self._resource_exists():
            return self.resource

        return self.ckan.action.resource_create(
            package_id=self.package_id,
            name=self.resource_name,
            **self.resource_attributes,
        )


class ResourceAndFileOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,
        resource_id: str,
        file_url: str,
        sync_timestamp: bool = True,
        upload_to_ckan: bool = True,
    ) -> None:
        self.resource_id = resource_id
        self.file_url = file_url
        self.sync = sync_timestamp
        self.upload = upload_to_ckan
        self.apikey = apikey
        self.address = address
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def _datetime_to_string(datetime):
        return datetime.strftime("%Y-%m-%dT%H:%M:%S")

    def execute(self, context):
        resource = self.ckan.action.resource_show(id=self.resource_id)

        response = None
        if self.upload:
            with tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / f"{resource['name'].resource['format']}"

                with open(path, "wb") as f:
                    response = requests.get(self.file_url)
                    f.write(response.content)

                self.ckan.action.resource_patch(
                    id=self.resource_id, upload=open(path, "rb"),
                )

        file_last_modified = parser.parse(
            requests.head(self.file_url).headers["last-modified"]
            if response is None
            else response.headers["last-modified"]
        )

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
                id=self.resource_id,
                last_modified=self._datetime_to_string(file_last_modified),
            )

        return {
            "resource_last_modified": self._datetime_to_string(resource_last_modified),
            "file_last_modified": self._datetime_to_string(file_last_modified),
            "timestamps_were_synced": self.sync and difference_in_seconds > 0,
            "difference_in_seconds": difference_in_seconds,
        }
