from airflow.plugins_manager import AirflowPlugin

from operators import directory_operator


class UtilsPlugin(AirflowPlugin):
    name = "utils_plugin"
    operators = [
        directory_operator.CreateLocalDirectoryOperator,
        directory_operator.DeleteLocalDirectoryOperator,
    ]
