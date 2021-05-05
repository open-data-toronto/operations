import ckanapi
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class BackupDatastoreResourceOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, address, apikey, package_name, backups_dir_task_id, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.package_name = package_name
        self.backups_dir_task_id = backups_dir_task_id
        self.ckan = ckanapi.RemoteCKAN(apikey=apikey, address=address)

    def execute(self, context):
        return self.ckan.action.package_show(id=self.package_name)
