'''
This Modules contains the classes operating at the package level.

Classes:
- GetOrCreatePackage: Return the the existing pachage or create one
'''

# Standard libraries
import logging

# Related third-party libraries

# Local application-specific libraries
from utils import misc_utils

class GetOrCreatePackage:
    """
    Get the existing ckan package object or create one.

    Attributes:
        Class-level:
        - CKAN: 
            connects to the CKAN API; The variable is used in a way
            that can be overridden by the class instance.
        Instance-level:
        - package_name: str
            the package name/id to be checked/created.
        - package_metadata: dict 
            the package metadata in form of a dict. 
            Required when the function wants to create the package.

    Methods:
    - get_or_create_package(self):
        Get the existing ckan package object or create one.
    """

    # Connect to CKAN API
    CKAN = misc_utils.connect_to_ckan()

    def __init__(
        self,
        package_name: str, 
        package_metadata: dict = None,
    ):
        """
        Initialize a new instance of the GetOrCreatePackage class
        and construct all the necessary attributes.

        Arguments:
        - package_name: str 
            the package name/id to be checked/created.
        - package_metadata: dict 
            the package metadata in form of a dict. 
            Required when the function wants to create the package.

        Return: None
        """
        self.package_name = package_name
        self.package_metadata = package_metadata

    def get_or_create_package(self):
        """
        Get the existing ckan package object or create one.

        Return: Ckan Package Object Metadata
        Return Type: Dict
        """
        logging.info(f"Package metadata: {str(self.package_metadata)}")

        # Check if the package exist
        try:
            logging.info(f"Attempting to get package {self.package_name}")
            existing_ckan_package = self.CKAN.action.package_show(id=self.package_name)
            return existing_ckan_package

        # Create an empty new package
        except:
            logging.info(f"Unable to get package {self.package_name} - creating it instead")
            new_ckan_package = self.CKAN.action.package_create(
                name = self.package_name,
                owner_org = "city-of-toronto",
                # Optional; Overwrite auto-generated hash-like id with the actual package name.
                # As a result, the package name and id would be the same.
                id=self.package_name,
                # Use license_id(str): id of the dataset’s license, see ckan.logic.action.get.license_list 
                license_url="https://open.toronto.ca/open-data-license/",
                **self.package_metadata,
            )
            return new_ckan_package