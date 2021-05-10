from airflow.plugins_manager import AirflowPlugin

import operators

# TODO: Releplace ckanapi library with standard HTTP requests (SimpleHTTPOperator)


class CKANPlugin(AirflowPlugin):
    name = "ckan_plugin"
    operators = [
        operators.package_operator.GetPackageOperator,
        operators.resource_operator.GetOrCreateResource,
        operators.resource_operator.ResourceAndFileOperator,
        operators.datastore_operator.BackupDatastoreResourceOperator,
        operators.datastore_operator.DeleteDatastoreResourceRecordsOperator,
        operators.datastore_operator.InsertDatastoreResourceRecordsOperator,
        operators.datastore_operator.RestoreDatastoreResourceBackupOperator,
    ]
