import pendulum
from airflow.decorators import dag, task, task_group
from ckan_operators.package_operator import GetOrCreatePackage
from ckan_operators.datastore_operator import DeleteDatastoreResource
from utils_operators.slack_operators import SlackTownCrier

package_name = 'test-package-dag-refactor'
package_metadata = {
  'date_published': "2023-05-01 14:18:24.389728",
  'refresh_rate': 'Semi-annually',
  'dataset_category': 'Table',
  'owner_division': 'Toronto Public Health',
}

#resource_id  = 'd68a18f4-47cb-4955-8bf5-d1f085f7957a'
resource_id  = '5241eaac-520d-4bae-bb9c-54b605c66164'

@dag(schedule=None, start_date=pendulum.datetime(2021, 1, 1, tz="UTC"))
def test_dag_refactor():

    @task
    def get_or_create_package(package_name, package_metadata):
        GetOrCreatePackage(package_name, package_metadata).get_or_create_package()
    
    @task
    def scribe(package):
        message = 'The package author is Reza'
        return message

    @task
    def slack_town_crier(dag_id, message_header, message_content, message_body):
        return SlackTownCrier(dag_id, message_header, message_content, message_body).announce()
        

    package = get_or_create_package(package_name, package_metadata)

    slack_town_crier(
        dag_id = "test_dag_refactor",
        message_header = "Slack Town Crier - Tests",
        message_content = scribe(package),
        message_body = "",
    )
    # @task
    # def delete_datastore_resource(rsid):
    #   ddr = DeleteDatastoreResource(resource_id = rsid, keep_schema=True)
    #   return ddr.delete_datastore_resource()

    # delete_datastore_resource(resource_id)

test_dag_refactor()