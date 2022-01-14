# test_dag_validity.py - runs `python path/to/dag.py` for each dag in the dags folder

import pytest
import os
import sys

# init current dir
dir = os.path.dirname(os.path.realpath(__file__))

# init list of dag subdirs
subdirs = ["datasets", "sustainment"]

# check each DAG for syntax errors to determine whether it can be loaded into airflow
def validity_check(subdir):
    print("Sys path: " + str(sys.path))
    ran_list = []
    for dag in os.listdir(dir + "/" + subdir):
        if dag not in ran_list and dag.endswith(".py"):
            assert os.system("python -m " + dir + "/" + subdir + "/" + dag ) == 0, dag + " did not load - it may have a syntax error"
            ran_list.append( dag )

def test_datasets_validity():
    validity_check("datasets")

def test_sustainment_validity():
    validity_check("sustainment")


if __name__ == "__main__":
    validity_check("datasets")