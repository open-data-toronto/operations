This folder of YAMLs will not be parsed into a DAG by Airflow.

We move folders here to not be parsed so that
1. Airflow doesn't spend compute on DAGs that aren't being used
2. We dont lose the configurations of these ETLs, just in case we need them later

This was inspired by the below error we received when our YAML-generated DAG count got close to 250:

airflow.exceptions.AirflowTaskTimeout: DagBag import timeout for /data/operations/dags/datasets/files_to_datastore/DAG_generator.py after 120.0s.
Please take a look at these docs to improve your DAG import time:
* https://airflow.apache.org/docs/apache-airflow/2.6.3/best-practices.html#top-level-python-code
* https://airflow.apache.org/docs/apache-airflow/2.6.3/best-practices.html#reducing-dag-complexity, PID: 72506

