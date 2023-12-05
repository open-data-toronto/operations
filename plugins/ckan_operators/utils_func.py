import os
import shutil
import logging
from pathlib import Path

from airflow.models import Variable


def create_dir_with_dag_id(dag_id: str, dir_variable_name: str) -> str:
    """
    Create a directory with dag name

    Parameters:
    - dag_id : str
        the id of the dag(pipeline)
    - dir_variable_name : str
        the name of the variable stored dir_path

    Returns:
        full directory path string: str

    """
    files_dir = Variable.get(dir_variable_name)

    dir_with_dag_name = Path(files_dir) / dag_id
    dir_with_dag_name.mkdir(parents=False, exist_ok=True)

    return str(dir_with_dag_name)


def delete_tmp_dir(dag_id: str, delete_recursively: bool = True) -> None:
    """
    Delete tmp directory with dag id

    Parameters:
    - dag_id : str
        the id of the dag(pipeline)
    - delete_recursively : bool, Optional
        flag if delete all files and subdirectories

    Returns:
        None

    """
    files_dir_path = Path(Variable.get("tmp_dir"))
    dag_tmp_dir = files_dir_path / dag_id

    # list all files under current directory before delete directory.
    logging.info(os.listdir(dag_tmp_dir))

    if not delete_recursively:
        os.rmdir(dag_tmp_dir)
    else:
        shutil.rmtree(dag_tmp_dir)


def delete_file(dag_tmp_dir: str, file_name: str) -> None:
    """
    Delete file under current directory
    Parameters:
    - dag_tmp_dir : str
        the directory path of the file to be deleted
    - file_name : str
        the name of the file to be deleted

    Returns:
        None

    """

    file_path = Path(dag_tmp_dir) / file_name
    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"{file_name} deleted.")
    else:
        logging.info("The file does not exist.")
