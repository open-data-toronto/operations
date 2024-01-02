import codecs
import csv
import sys
import logging
from typing import Dict, List, Generator

from utils import misc_utils


def stream_to_datastore(
    resource_id: str,
    file_path: str,
    attributes: Dict,
    encoding: str = "latin1",
    batch_size: int = 20000,
    do_not_cache: bool = False,
    **kwargs,
) -> None:
    """
    Stream records from csv; insert records into ckan datastore in batch
    based on yaml config attributes.

    Parameters:
    - resource_id : str
        the id of the datastore resource
    - file_path : str
        the path of data file
    - attributes : Dict
        the attributes section of yaml config (data fields)
    - encoding: str, Optional
        the encoding form of data file
    - batch_size: int, Optional
        total number of records in a batch
    - do_not_cache: bool, Optional
        indicate how datastore_create store records when insert in batch

    Returns:
        None

    """
    logging.info("---------Start Streaming to CKAN---------------")

    def read_csv(file_path: str) -> Generator:
        """
        reads CSV at input filepath and returns generator
        Parameters:
        - file_path : str
            the path of data file

        Returns:
            data : Generator
        """

        # grab fieldnames from csv
        with open(file_path, "r", encoding=encoding) as f:
            fieldnames = next(csv.reader(f))

        return misc_utils.csv_to_generator(file_path, fieldnames, encoding)

    def insert_into_ckan(records: List) -> None:
        """
        receives data as list of dicts, puts that data in CKAN
        Parameters:
        - records : List
            data records as list of dicts

        Returns:
            None
        """
        ckan.action.datastore_create(
            id=resource_id,
            fields=attributes,
            records=records,
            force=True,
            do_not_cache=do_not_cache,
        )

    csv.field_size_limit(sys.maxsize)
    ckan = misc_utils.connect_to_ckan()

    # init csv generator
    csv_generator = read_csv(file_path)

    # init counter vars, make calls to CKAN API in batch
    total_count = 0
    curr_batch_count = 0
    records = []

    for row in csv_generator:
        total_count += 1
        curr_batch_count += 1
        records.append(row)

        # when this batch is the max size, insert the data into CKAN
        if len(records) >= batch_size:
            logging.info("Loading {}th records".format(str(total_count)))
            insert_into_ckan(records)

            # reset current batch
            records.clear()
            curr_batch_count = 0

    # insert the last batch into CKAN
    if len(records) != 0:
        logging.info(f"Loading last {len(records)} records")
        insert_into_ckan(records)

    print(f"Inserted {total_count} records into CKAN")

    return {"success": True, "record_count": str(total_count)}
