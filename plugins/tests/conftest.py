import pytest
from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag(
        dag_folder="/data/operations/dags/datasets/files_to_datastore",
        include_examples=False,
    )
