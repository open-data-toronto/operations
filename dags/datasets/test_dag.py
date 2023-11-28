from datetime import datetime
from typing import Dict

from airflow.decorators import dag, task, task_group

from utils_operators.slack_operators import task_failure_slack_alert
from ckan_operators.resource_class import GetOrCreateResource, EditResourceMetadata


default_args = {
    "owner": "Yanan",
    "email": ["yanan.zhang@toronto.ca"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": task_failure_slack_alert,
    "retry_delay": 5,
    "pool": "ckan_pool",
    "retries": 1,
}


@dag(
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2023, 11, 21),
    catchup=False,
)
def test_publishing_pipeline():
    package_id = "template-test-pipeline"
    resource_name = "Test Resource Name"

    @task
    def get_or_create_resource(package_id: str, resource_name: str) -> Dict:
        resource = GetOrCreateResource(
            package_id=package_id,
            resource_name=resource_name,
            resource_attributes=dict(
                format="csv",
                is_preview=True,
                url_type="datastore",
                extract_job=f"Airflow: {package_id}",
                url="placeholder",
            ),
        )

        return resource.get_or_create_resource()

    @task
    def edit_resource_metadata(
        resource_id: str,
        new_last_modified: datetime = None,
        new_resource_name: str = None,
    ) -> None:
        edited_resource = EditResourceMetadata(resource_id=resource_id)
        
        edited_resource.edit_resource_metadata(
            new_resource_name=new_resource_name, new_last_modified=new_last_modified
        )

    # Main Flow
    resource = get_or_create_resource(package_id, resource_name)
    
    @task_group(group_id='edit_resource_task_group')
    def tg1():
        edit_resource_metadata(
            resource_id=resource["id"], new_resource_name="edited new resource name"
        )
        edit_resource_metadata(resource_id=resource["id"], new_last_modified="2020-08-18 14:33:07")
    
    resource >> tg1()


test_publishing_pipeline()
