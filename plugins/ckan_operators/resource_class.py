import logging
import time

from typing import Dict
from datetime import datetime
from ckan_operators import ckan_connector


class GetOrCreateResource:
    """
    A class to get or create package.

    Attributes:
        - package_id : str
            the id of the package wherein we'll look for/make a resource
        - resource_name : str
            the name of the resource this operator will look for/make
        - resource_attributes : dict
            the attributes that will be in a resource if we create it

    Methods:
        _resource_exists(package_id, resource_name):
            private method check if ckan resource exists
        
        get_or_create_resource(package_id, resource_name, resource_attributes):
            Return CKAN resource object, or create it if it does not exist
        
    """
    def __init__(
        self,
        package_id: str,
        resource_name: str,
        resource_attributes: Dict,
        **kwargs,
    ) -> Dict:
        """
        Construct all the necessary attributes for the GetOrCreateResource object.

        Parameters:
            - package_id : str
                the id of the package wherein we'll look for/make a resource
            - resource_name : str
                the name of the resource this operator will look for/make
            - resource_attributes : dict
                the attributes that will be in a resource if we create it
        """
        
        self.package_id = package_id
        self.resource_name = resource_name
        self.resource_attributes = resource_attributes

        self.ckan = ckan_connector.connect_to_ckan()

    # check if resource exists
    def _resource_exists(self):
        package = self.ckan.action.package_show(id=self.package_id)
        for r in package["resources"]:
            if r["name"] == self.resource_name:
                self.resource = r
                return True
        return False

    def get_or_create_resource(self):
        """
        Return ckan resource object, or create it if it does not exist

        Parameters:
            - package_id : str 
                the id of the package wherein we'll look for/make a resource
            - resource_name : str
                the name of the resource this operator will look for/make
            - resource_attributes : dict 
                the attributes that will be in a resource if we create it

        Returns:
            ckan resource : dict

        """
        if self._resource_exists():
            logging.info(
                "Resource found using the package id {} and resource name {}".format(
                    self.package_id, self.resource_name
                )
            )
            resource = self.resource
            if resource["datastore_active"]:
                resource["is_new"] = False
            else:
                resource[
                    "is_new"
                ] = True  # considered as new if there is no datastore data
        else:
            # create a resource and check if created - if not, then create it again
            logging.info(
                "Resource not found - creating a resource called {} in package {}".format(
                    self.resource_name, self.package_id
                )
            )
            resource = self.ckan.action.resource_create(
                package_id=self.package_id,
                name=self.resource_name,
                **self.resource_attributes,
            )
            resource["is_new"] = True

            while True:
                # try to get that created resource
                try:
                    # pause a moment before checking that the resource is truly loaded - sometimes a resource_create call reports success but never made a resource
                    logging.info(
                        "Pausing a moment before checking that the resource is truly loaded"
                    )
                    time.sleep(10)
                    logging.info(
                        "Fetching resource to make sure it was created successfully..."
                    )
                    assert self.ckan.action.resource_show(id=resource["id"])
                    break

                # but if we cant, make it again and repeat the cycle
                except Exception as e:
                    logging.error(e)
                    logging.info("Resource not made successfully - trying again ... ")
                    logging.info(
                        "Resource not found - creating a resource called {} in package {}".format(
                            self.resource_name, self.package_id
                        )
                    )
                    resource = self.ckan.action.resource_create(
                        package_id=self.package_id,
                        name=self.resource_name,
                        **self.resource_attributes,
                    )
                    resource["is_new"] = True

        logging.info(f"Returning: {resource}")

        return resource


class EditResourceMetadata:
    """
    A class to edit ckan resource metadata

    Attributes:
        - resource_id : str
                the id of the ckan resource whose metadata will be updated
        - new_resource_name : str
            the new name of the resource
        - last_modified : datetime 
            the last_modified date of the resource

    Methods:
        _datetime_to_string(package_id, resource_name):
            convert last_modified from datetime object to string format
        
        edit_resource_metadata(resource_id, new_resource_name, last_modified):
            Edit a resource's name or last_modified date
        
    """

    def __init__(
        self,
        resource_id: str,
        new_resource_name: str = None,
        last_modified: datetime = None,
    ) -> Dict:
        """
        Construct all the necessary attributes for the EditResourceMetadata object.

        Parameters:
            - resource_id : str
                the id of the ckan resource whose metadata will be updated
            - new_resource_name : str
                the new name of the resource
            - last_modified : datetime 
                the last_modified date of the resource
        """
        
        self.resource_id = resource_id
        self.new_resource_name = new_resource_name
        self.last_modified = last_modified

        self.ckan = ckan_connector.connect_to_ckan()

    def _datetime_to_string(self, datetime):
        try:
            output = datetime.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception as e:
            output = datetime
        return output

    # resource_patch api call below for non null input vars
    def edit_resource_metadata(self):
        """
        Edit a resource's name or last_modified date, only one of name or last_modified is required

        Parameters:
            - resource_id : str
                the id of the ckan resource whose metadata will be updated
            - new_resource_name : str
                the new name of the resource
            - last_modified : datetime 
                the last_modified date of the resource
        
        Return: None
        """
        # last modified date
        if self.last_modified:
            last_modified = self._datetime_to_string(self.last_modified)
            logging.info(
                "Setting last-modified of resource_id {} to {}".format(
                    self.resource_id, last_modified
                )
            )
            self.ckan.action.resource_patch(
                id=self.resource_id,
                last_modified=last_modified,
            )
        # new resource_name
        if self.new_resource_name:
            logging.info(
                "Setting name of resource_id {} to {}".format(
                    self.resource_id, self.new_resource_name
                )
            )
            self.ckan.action.resource_patch(
                id=self.resource_id,
                name=self.new_resource_name,
            )
