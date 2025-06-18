import ckanapi
import logging
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils import misc_utils



class GetOrCreatePackageOperator(BaseOperator):
    """
    Returns, or creates, package with input name
    """
    @apply_defaults
    def __init__(
        self,

        package_name_or_id: str = None,
        package_name_or_id_task_id: str = None,
        package_name_or_id_task_key: str = None,

        package_metadata: dict = None,
        package_metadata_task_id: dict = None,
        package_metadata_task_key: dict = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.package_name_or_id, self.package_name_or_id_task_id, self.package_name_or_id_task_key = package_name_or_id, package_name_or_id_task_id, package_name_or_id_task_key
        self.package_metadata, self.package_metadata_task_id, self.package_metadata_task_key = package_metadata, package_metadata_task_id, package_metadata_task_key

    def execute(self, context):

        self.ckan = misc_utils.connect_to_ckan()

        # get the package id and notes from a task if its been provided from a task
        if self.package_name_or_id_task_id and self.package_name_or_id_task_key:
            self.package_name_or_id = ti.xcom_pull(task_ids=self.package_name_or_id_task_id)[self.package_name_or_id_task_key]

        logging.info("Package metadata: " + str(self.package_metadata))

        try:
            logging.info("Attempting to get package {}".format(self.package_name_or_id))
            #return self.ckan.action.package_show(id=self.package_name_or_id)
            return self.ckan.action.package_show(
                id=self.package_name_or_id,
                #owner_org="city-of-toronto",
                #name=self.package_name_or_id,
                #license_url="https://open.toronto.ca/open-data-license/",
                #**self.package_metadata,
            )

        except:
            logging.info("Unable to get package {} - creating it instead".format(self.package_name_or_id))
            return self.ckan.action.package_create(
                id=self.package_name_or_id,
                owner_org="city-of-toronto",
                name=self.package_name_or_id,
                license_url="https://open.toronto.ca/open-data-license/",
                **self.package_metadata,
            )


class GetPackageOperator(BaseOperator):
    """
    Returns package object from CKAN
    """

    @apply_defaults
    def __init__(self, package_name_or_id, **kwargs):
        super().__init__(**kwargs)
        self.package_id = package_name_or_id

    def execute(self, context):
        self.ckan = misc_utils.connect_to_ckan()
        return self.ckan.action.package_show(id=self.package_id)


class GetAllPackagesOperator(BaseOperator):
    """
    Returns all packages CKAN via the package_search call
    """

    @apply_defaults
    def __init__(self, address: str = None, **kwargs):
        self.address = address
        super().__init__(**kwargs)

    def execute(self, context):

        if self.address:
            self.ckan = ckanapi.RemoteCKAN(apikey="", address=self.address)
        else:
            self.ckan = misc_utils.connect_to_ckan()

        # Initiate a loop of getting each package from CKAN, 1000 packages at a time
        output = []
        offset = 0

        # Until we see all of the packages, keep asking CKAN for a batch of 1000 packages
        while True:
            # Append up to 1000 packages to the output
            package_object = self.ckan.action.package_search(rows=1000, start=offset)
            output += package_object["results"]

            # Add 1000 to our offset so if we query 1000 more packages, we dont get ones we already have
            offset += 1000

            # IF the query result has less records than our new offset
            # OR
            # IF the query result gave us less than the maximum number of returnable packages (1000 packages)
            # THEN break the loop - we've got all the packages
            if package_object["count"] < offset or package_object["count"] > 1000:
                break

        return {"packages": output}


class AssertIdenticalPackagesOperator(BaseOperator):
    """
    Passes only if both package_a and package_b contain identical CKAN packages or identical lists of packages

    package_a
    :   a package dict object from CKAN, or a list thereof

    package_b
    :   another package dict object (or list thereof) from CKAN to which package_a will be compared

    On success, returns a dictionary: `{"result": True}`
    """

    @apply_defaults
    def __init__(
        self,

        package_a: str = None,
        package_a_task_id: str = None,
        package_a_task_key: str = None,

        package_b: str = None,
        package_b_task_id: str = None,
        package_b_task_key: str = None,


        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.package_a = package_a
        """The actual package_a dict object, or a list thereof """
        self.package_a_task_id = package_a_task_id
        """A reference to a task that returns a package_a"""
        self.package_a_task_key = package_a_task_key
        """A reference to the key in the returned dict for the package_a_task_id task"""

        self.package_b = package_b
        """The actual package_b dict object, or a list thereof """
        self.package_b_task_id = package_b_task_id
        """A reference to a task that returns a package_b"""
        self.package_b_task_key = package_b_task_key
        """A reference to the key in the returned dict for the package_b_task_id task"""
    
    
    def get_packages(self, ti):
        # get package_a
        if self.package_a_task_id and self.package_a_task_key:
            self.package_a = ti.xcom_pull(task_ids=self.package_a_task_id)[self.package_a_task_key]

        # get package_b
        if self.package_b_task_id and self.package_b_task_key:
            self.package_b = ti.xcom_pull(task_ids=self.package_b_task_id)[self.package_b_task_key]

        # make sure these inputs are both lists or both dicts
        assert type(self.package_a) == type(self.package_b), "Inputs are not of the same data type! Both must be lists or both must be dicts"


    def lists_match(self, package_a, package_b):
        # find a package in the first list that matches one in the second, by name
        assert len(package_a) == len(package_b), "Input lists of packages are not the same length! Contrib has {} while Delivery has {}".format(len(package_a), len(package_b))
        assert isinstance(package_a, list) and isinstance(package_b, list), "Package comparison failed because a non-list object was given when a list of packages was expected"
        for a in package_a:
            match_found = False
            for b in package_b:
                # if 2 packages have a matching name, see if their contents match
                if a["name"] == b["name"]:
                    assert self.packages_match(a, b), "2 Packages named {} are not identical!".format(a["name"])
                    match_found = True
                    break
            # if no match gets found, raise an error
            assert match_found, "No package was found to match {}".format(a["name"]) 
        
        return True

        

    def packages_match(self, package_a, package_b):
        # make sure packages have typical CKAN package attributes
        assert all( [attr in package_a.keys() for attr in ["resources", "state", "id", "name"]]  ), "Package A is missing typical ckan package keys"
        assert all( [attr in package_a.keys() for attr in ["resources", "state", "id", "name"]]  ), "Package B is missing typical ckan package keys"
        
        # make sure these packages have the same keys
        assert package_a.keys() == package_b.keys(), "Package A and B dont have the same keys"

        # ensure packages have the same number of resources
        assert len(package_a["resources"]) == len(package_b["resources"]), "Package A and B have a different number of resources"

        # ensure that values match for certain key attributes in each resource
        for key in ["name", "datastore_active", "format", "last_modified", "state", "id"]:
            assert all( [ resource_a[key] in [resource[key] for resource in package_b["resources"]] for resource_a in package_a["resources"]] ), "{} is out of sync for a resource in package {}".format(key, package_a["name"])

        return True
        

    def execute(self, context):
        # get task instance context
        ti = context['ti']

        # get input package data
        self.get_packages(ti)        

        # compare lists of packages
        if isinstance(self.package_a, list):
            if self.lists_match( self.package_a, self.package_b ):
                return {"result": True}

        # TODO: add logic for comparing only packages and not lists of packages


class GetCkanPackageListOperator(BaseOperator):
    """
    Get full package list
    """
    @apply_defaults
    def __init__(
        self,

        **kwargs
    ) -> None:
        super().__init__(**kwargs)


    def execute(self, context):
        self.ckan = misc_utils.connect_to_ckan()
        package_list = self.ckan.action.package_list()
        logging.info(f'package list {package_list}')
        return package_list



'''
This Modules contains the classes operating at the package level.

Classes:
- GetOrCreatePackage: Return the the existing pachage or create one
'''

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
        # Connect to CKAN API
        self.CKAN = misc_utils.connect_to_ckan()
        
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
        except Exception as e:
            logging.info(f"Unable to get package {self.package_name} - creating it instead")
            new_ckan_package = self.CKAN.action.package_create(
                name = self.package_name,
                owner_org = "city-of-toronto",
                
                # Use license_id(str): id of the dataset’s license, see ckan.logic.action.get.license_list 
                license_url="https://open.toronto.ca/open-data-license/",
                **self.package_metadata,
            )
            return new_ckan_package