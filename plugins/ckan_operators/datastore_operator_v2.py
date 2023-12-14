'''
This Modules contains the classes operating at datastore resource level.

Classes:
- DeleteDatastoreResource: delete a datastore resource 
'''

# Standard libraries
import logging

# Related third-party libraries

# Local application-specific libraries
from utils import misc_utils

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


