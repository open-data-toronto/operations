# from airflow.plugins_manager import AirflowPlugin

# from operators import datastore_operator, package_operator, resource_operator

# # TODO: Releplace ckanapi library with standard HTTP requests (SimpleHTTPOperator)


# class CKANPlugin(AirflowPlugin):
#     name = "ckan_plugin"
#     operators = [
#         package_operator.GetPackageOperator,
#         resource_operator.GetOrCreateResource,
#         resource_operator.ResourceAndFileOperator,
#         datastore_operator.BackupDatastoreResourceOperator,
#         datastore_operator.DeleteDatastoreResourceRecordsOperator,
#         datastore_operator.InsertDatastoreResourceRecordsOperator,
#         datastore_operator.RestoreDatastoreResourceBackupOperator,
#     ]
