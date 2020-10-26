import os
import traceback
from pathlib import Path

import ckanapi
from utils import common as utils
import tasks
import datasets
import argparse
import scheduler

PATH = Path(os.path.abspath(__file__))
ROOT_DIR = PATH.parent.parent


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job",
        nargs="?",
        type=str,
        help="Name of job (in datasets or tasks) to run one time.",
    )
    parser.add_argument(
        "--config_file",
        nargs="?",
        type=str,
        default=ROOT_DIR / "configs" / "config.yaml",
        help="Location of config file (OPTIONAL). Defaults to root directory configs folder.",
    )
    parser.add_argument(
        "--active_env",
        nargs="?",
        type=str,
        help="Override of environment in config file (OPTIONAL). Values: dev, qa, prod",
    )
    parser.add_argument(
        "--schedule", default=False, action="store_true", help="Run in schedule mode."
    )
    args = parser.parse_args()

    print(args.schedule)

    assert (
        args.schedule is False or args.job is None
    ), "Can either run in schedule mode or a single job, not both."

    if args.job is not None:
        assert any(
            [hasattr(datasets, args.job), hasattr(tasks, args.job)]
        ), f"No script for job '{args.job}' in datasets or tasks"

    return args


args = parse_args()

configs = utils.load_yaml(filepath=args.config_file)

for folder, relative_path in configs["directories"].items():
    configs["directories"][folder] = ROOT_DIR / configs["directories"][relative_path]

utils.make_dirs_if_new(config_filepath=args.config_file, configs=configs)

active_env = configs["active_env"] if args.active_env is None else args.active_env


def get_job(name, logger):
    job = getattr(datasets, name) if hasattr(datasets, name) else getattr(tasks, name)

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

    utils.send_notifications(
        name=name, configs=configs, ckan_url=ckan.address, **result
    )

    logger.info("Finished")


if args.schedule is False and args.job is not None:
    name = args.job
    get_job(
        name,
        logger=utils.make_logger(name=name, logs_dir=configs["directories"]["logs"],),
    )

elif args.schedule:
    logger = utils.make_logger(
        name="scheduler", logs_dir=configs["directories"]["logs"],
    )
    logger.debug("Running schedule")
    schedule = scheduler.get_job_schedule(get_job, configs)

    for job in schedule.jobs:
        logger.debug(f"Scheduler job: {job}")

    try:
        while True:
            schedule.run_pending()
    except Exception:
        logger.error(traceback.format_exc())

    print("finishing up")
