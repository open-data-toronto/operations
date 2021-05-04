from airflow.plugins_manager import AirflowPlugin

from operators.package_operator import GetPackageOperator


class CKANPlugin(AirflowPlugin):
    name = "ckan_plugin"
    operators = [GetPackageOperator]
