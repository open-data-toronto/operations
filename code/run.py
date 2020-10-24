import os
import traceback
from pathlib import Path

import ckanapi
from utils import common as utils
import sustainment
import datasets
import argparse

PATH = Path(os.path.abspath(__file__))
ROOT_DIR = PATH.parent.parent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", nargs="?", type=str, default="sync_remote_file_times")
    parser.add_argument(
        "--config_file",
        nargs="?",
        type=str,
        default=ROOT_DIR / "configs" / "config.yaml",
    )
    parser.add_argument(
        "--log_level", nargs="?", type=str, default="DEBUG",
    )
    parser.add_argument(
        "--active_env", nargs="?", type=str,
    )
    args = parser.parse_args()

    assert any(
        [hasattr(datasets, args.job), hasattr(sustainment, args.job)]
    ), f"No script for job '{args.job}' in datasets, sustainment"

    return args


args = parse_args()

configs = utils.load_yaml(filepath=args.config_file)
utils.make_dirs_if_new(config_filepath=args.config_file, configs=configs)

active_env = configs["active_env"] if args.active_env is None else args.active_env
name = args.job

job = getattr(datasets, name) if hasattr(datasets, name) else getattr(sustainment, name)

logger = utils.make_logger(
    log_level=args.log_level,
    name=name,
    logs_dir=ROOT_DIR / configs["directories"]["logs"],
)

try:
    ckan = ckanapi.RemoteCKAN(**configs["ckan"][active_env])
    logger.info(f"Started for: {ckan.address}")

    result = job.run(logger=logger, utils=utils, ckan=ckan, configs=configs)

except Exception:
    error = traceback.format_exc()
    message_content = error
    logger.error(error)
    result = {
        "message_type": "error",
        "msg": message_content,
    }

utils.send_notifications(name=name, configs=configs, ckan_url=ckan.address, **result)

logger.info("Finished")
