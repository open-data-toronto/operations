import ckanapi
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
