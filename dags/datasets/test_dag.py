from datetime import datetime
from airflow.decorators import dag, task
from utils_operators.slack_operators import task_failure_slack_alert
from ckan_operators.resource_class import GetOrCreateResource


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
    package_id = "refactor-template-test-pipeline"
    resource_name = "Test Resource"
    
    @task
    def get_or_create_resource(package_id, resource_name):
        resource = GetOrCreateResource(
            package_id=package_id,
            resource_name= resource_name,
            resource_attributes=dict(
                format="csv",
                is_preview=True,
                url_type="datastore",
                extract_job=f"Airflow: {package_id}",
                url="placeholder",
                title = 
            )
        )
        
        return resource.get_or_create_resource()
        
    get_or_create_resource(package_id, resource_name)
    

test_publishing_pipeline()