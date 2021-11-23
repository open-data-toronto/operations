import ckanapi
import logging
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class GetOrCreatePackageOperator(BaseOperator):
    """
    Returns, or creates, package with input name
    """
    @apply_defaults
    def __init__(
        self,
        address: str,
        apikey: str,

        package_name_or_id: str = None,
        package_name_or_id_task_id: str = None,
        package_name_or_id_task_key: str = None,
        
        package_notes: str = None,
        package_notes_task_id: str = None,
        package_notes_task_key: str = None,

        package_limitations: str = None,
        package_limitations_task_id: str = None,
        package_limitations_task_key: str = None,

        package_refresh_rate: str = None,
        package_refresh_rate_task_id: str = None,
        package_refresh_rate_task_key: str = None,

        dataset_category: str = None,
        dataset_category_task_id: str = None,
        dataset_category_task_key: str = None,

        owner_division: str = None,
        owner_division_task_id: str = None,
        owner_division_task_key: str = None,
    
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.package_name_or_id, self.package_name_or_id_task_id, self.package_name_or_id_task_key = package_name_or_id, package_name_or_id_task_id, package_name_or_id_task_key
        self.package_notes, self.package_notes_task_id, self.package_notes_task_key = package_notes, package_notes_task_id, package_notes_task_key
        self.package_limitations, self.package_limitations_task_id, self.package_limitations_task_key = package_limitations, package_limitations_task_id, package_limitations_task_key
        self.package_refresh_rate, self.package_refresh_rate_task_id, self.package_refresh_rate_task_key = package_refresh_rate, package_refresh_rate_task_id, package_refresh_rate_task_key
        self.dataset_category, self.dataset_category_task_id, self.dataset_category_task_key = dataset_category, dataset_category_task_id, dataset_category_task_key
        self.owner_division, self.owner_division_task_id, self.owner_division_task_key = owner_division, owner_division_task_id, owner_division_task_key
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def execute(self, context):
        # get the package id and notes from a task if its been provided from a task
        if self.package_name_or_id_task_id and self.package_name_or_id_task_key:
            self.package_name_or_id = ti.xcom_pull(task_ids=self.package_name_or_id_task_id)[self.package_name_or_id_task_key]

        if self.package_notes_task_id and self.package_notes_task_key:
            self.package_notes = ti.xcom_pull(task_ids=self.package_notes_task_id)[self.package_notes_task_key]

        if self.package_limitations_task_id and self.package_limitations_task_key:
            self.package_limitations = ti.xcom_pull(task_ids=self.package_limitations_task_id)[self.package_limitations_task_key]

        if self.package_refresh_rate_task_id and self.package_refresh_rate_task_key:
            self.package_refresh_rate = ti.xcom_pull(task_ids=self.package_refresh_rate_task_id)[self.package_refresh_rate_task_key]

        if self.dataset_category_task_id and self.dataset_category_task_key:
            self.dataset_category = ti.xcom_pull(task_ids=self.dataset_category_task_id)[self.dataset_category_task_key]

        if self.owner_division_task_id and self.owner_division_task_key:
            self.owner_division = ti.xcom_pull(task_ids=self.owner_division_task_id)[self.owner_division_task_key]

        # return a package, if the input package id exists
        try:
            logging.info("Attempting to get package {}".format(self.package_name_or_id))
            return self.ckan.action.package_show(id=self.package_name_or_id)

        except:
            logging.info("Unable to get package {} - creating it instead".format(self.package_name_or_id))
            return self.ckan.action.package_create(
                id=self.package_name_or_id,
                owner_org="city-of-toronto",
                name=self.package_name_or_id,
                title=self.package_name_or_id.replace("-", " ").title(),
                notes=self.package_notes,
                limitations=self.package_limitations,
                refresh_rate=self.package_refresh_rate,
                dataset_category=self.dataset_category,
                owner_division=self.owner_division
            )


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