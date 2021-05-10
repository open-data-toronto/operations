from airflow.plugins_manager import AirflowPlugin

from operators.datastore_operator import *
from operators.package_operator import *
from operators.resource_operator import *

# TODO: Releplace ckanapi library with standard HTTP requests (SimpleHTTPOperator)


class CKANPlugin(AirflowPlugin):
    name = "ckan_plugin"
    operators = [
        GetPackageOperator,
        GetOrCreateResource,
        ResourceAndFileOperator,
        BackupDatastoreResourceOperator,
        DeleteDatastoreResourceRecordsOperator,
        InsertDatastoreResourceRecordsOperator,
        RestoreDatastoreResourceBackupOperator,
    ]
