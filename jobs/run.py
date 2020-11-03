import os
import traceback
from pathlib import Path

import ckanapi
import jobs.utils.common as utils
import jobs.sustainment as tasks
import jobs.datasets as datasets
import argparse

PATH = Path(os.path.abspath(__file__))
ROOT_DIR = PATH.parent.parent


def parse_args(args_list):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job",
        type=str,
        help="Name of job (in datasets or tasks) to run one time.",
    )
    parser.add_argument(
        "--config_file",
        nargs="?",
        type=str,
        default=ROOT_DIR / "configs" / "config.yaml",
        help="Config file path (OPTIONAL). Defaults to root directory configs folder.",
    )
    parser.add_argument(
        "--active_env",
        nargs="?",
        type=str,
        help="Override of environment in config file (OPTIONAL). Values: dev, qa, prod",
    )
    parser.add_argument(
        "--is_airflow_run",
        default=True,
        action="store_true",
        help="Whether run is in airflow, to use built-loggers",
    )

    args = parser.parse_args() if args_list is None else parser.parse_args(args_list)

    assert any(
        [hasattr(datasets, args.job), hasattr(tasks, args.job)]
    ), f"No script for job '{args.job}' in datasets or tasks"

    return args


def run(args_list=None, **kwargs):
    args = parse_args(args_list)

    configs = utils.load_yaml(filepath=args.config_file)

    for folder, relative_path in configs["directories"].items():
        if folder == "logs" and args.is_airflow_run is True:
            continue

        configs["directories"][folder] = (
            ROOT_DIR / configs["directories"][relative_path]
        )

    utils.make_dirs_if_new(config_filepath=args.config_file, configs=configs)

    active_env = configs["active_env"] if args.active_env is None else args.active_env
    configs["active_env"] = active_env

    def get_job(name, logger):
        job = (
            getattr(datasets, name) if hasattr(datasets, name) else getattr(tasks, name)
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

        utils.send_notifications(
            name=name, configs=configs, ckan_url=ckan.address, **result
        )

        logger.info("Finished")

    name = args.job
    get_job(
        name,
        logger=utils.make_logger(
            name=name,
            logs_dir=configs["directories"]["logs"],
            active_env=configs["active_env"],
            is_airflow_run=args.is_airflow_run,
        ),
    )


if __name__ == "__main__":
    run()
