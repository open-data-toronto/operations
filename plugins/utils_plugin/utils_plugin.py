from airflow.plugins_manager import AirflowPlugin

from operators import directory_operator, file_operator


class UtilsPlugin(AirflowPlugin):
    name = "utils_plugin"
    operators = [
        directory_operator.CreateLocalDirectoryOperator,
        directory_operator.DeleteLocalDirectoryOperator,
        file_operator.CreateLocalDirectoryOperator,
        file_operator.DeleteLocalDirectoryOperator,
    ]
