# test_dag_validity.py - runs `python path/to/dag.py` for each dag in the dags folder

import pytest
import os

# init current dir
dir = os.path.dirname(os.path.realpath(__file__))

# init list of dag subdirs
subdirs = ["datasets", "sustainment"]

# check each DAG for syntax errors to determine whether it can be loaded into airflow
def test_validity():
    ran_list = []
    for subdir in subdirs:
        for dag in os.listdir(dir + "/" + subdir):
            print(dag)
            if dag not in ran_list and dag.endswith(".py"):
                print("Testing " + dag)
                print( os.system("python " + dir + "/" + subdir + "/" + dag ) )
                ran_list.append( dag )

if __name__ == "__main__":
    test_validity()