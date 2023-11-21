import logging
import time

from typing import Dict
from ckan_operators import ckan_connector


class GetOrCreateResource:
    """
    Returns CKAN resource object, or creates it if it does not exist

    Expects as inputs:
    - package_id: the id of the package wherein we'll look for/make a resource
    - resource_name: the name of the resource this operator will look for/make
    - resource_attributes: the attributes that will be in a resource if we create it

    """

    def __init__(
        self,
        package_id: str,
        resource_name: str,
        resource_attributes: Dict,
        **kwargs,
    ) -> Dict:
        self.package_id = package_id
        self.resource_name = resource_name
        self.resource_attributes = resource_attributes

        self.ckan = ckan_connector.connect_to_ckan()

    # check if resource exists
    def resource_exists(self):
        package = self.ckan.action.package_show(id=self.package_id)
        for r in package["resources"]:
            if r["name"] == self.resource_name:
                self.resource = r
                return True
        return False

    def get_or_create_resource(self):
        """
        branching to determine whether resource is found or created,
        set resource["is_new"] (add "is_new" key to resource dictionary) for branching in dags
        """

        if self.resource_exists():
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
