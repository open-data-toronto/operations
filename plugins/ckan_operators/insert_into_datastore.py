import codecs
import csv
import sys
import logging
from typing import Dict, List, Generator

from utils import misc_utils


def stream_to_datastore(
    resource_id: str,
    file_path: str,
    config: Dict,
    encoding: str = "latin1",
    batch_size: int = 20000,
    **kwargs,
) -> None:
    logging.info("-----------------------------")
    logging.info(config)
    def read_csv(file_path: str) -> Generator:
        """reads CSV at input filepath and returns generator"""

        # grab fieldnames from csv
        with open(
            file_path, "r", encoding=encoding
        ) as f: 
            fieldnames = next(csv.reader(f))

        return misc_utils.csv_to_generator(file_path, fieldnames, encoding)

    def insert_into_ckan(records: List) -> None:
        ckan.action.datastore_create(
            id=resource_id,
            fields=config["attributes"],
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
        logging.info("Loading last records")
        insert_into_ckan(records)

    print(f"Inserted {total_count} records into CKAN")

    return {"success": True, "record_count": str(total_count)}
