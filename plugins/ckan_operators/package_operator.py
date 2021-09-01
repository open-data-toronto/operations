import ckanapi
import logging
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults



class GetPackageOperator(BaseOperator):
    """
    Returns package object from CKAN
    """

    @apply_defaults
    def __init__(self, address, apikey, package_name_or_id, **kwargs):
        super().__init__(**kwargs)
        self.package_id = package_name_or_id
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def execute(self, context):
        return self.ckan.action.package_show(id=self.package_id)


class GetAllPackagesOperator(BaseOperator):
    """
    Returns all packages CKAN via the package_search call
    """

    @apply_defaults
    def __init__(self, address, apikey, **kwargs):
        super().__init__(**kwargs)
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def execute(self, context):
        packages = self.ckan.action.package_list()

        return {"packages": self.ckan.action.package_search(rows=1000)["results"]}



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