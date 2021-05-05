from airflow.plugins_manager import AirflowPlugin

from operators import package_operator, resource_operator

# TODO: Releplace ckanapi library with standard HTTP requests (SimpleHTTPOperator)


class CKANPlugin(AirflowPlugin):
    name = "ckan_plugin"
    operators = [
        package_operator.GetPackageOperator,
        resource_operator.GetOrCreateResource,
    ]
