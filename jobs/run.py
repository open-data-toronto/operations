import os
import traceback
from pathlib import Path

import ckanapi
from utils import common as utils
import tasks
import datasets
import argparse
import schedules
import calendar

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
        help="Config file path (OPTIONAL). Defaults to root directory configs folder.",
    )
    parser.add_argument(
        "--active_env",
        nargs="?",
        type=str,
        help="Override of environment in config file (OPTIONAL). Values: dev, qa, prod",
    )
    parser.add_argument(
        "--schedule", default=False, action="store_true", help="Run schedule"
    )
    parser.add_argument(
        "--run_tasks_on_start",
        default=False,
        action="store_true",
        help="If running schedule, whether to run tasks when it initializes",
    )

    args = parser.parse_args()

    assert (
        args.schedule is False or args.job is None
    ), "Can either run in schedule mode or a single job, not both."

    assert (args.run_tasks_on_start is not True and args.job is not None) or (
        args.job is None
    ), "Cannot pass run_tasks_on_start and a job at the same time"

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
configs["active_env"] = active_env


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
        logger=utils.make_logger(
            name=name,
            logs_dir=configs["directories"]["logs"],
            active_env=configs["active_env"],
        ),
    )

elif args.schedule:
    logger = utils.make_logger(
        name="scheduler",
        logs_dir=configs["directories"]["logs"],
        active_env=configs["active_env"],
    )
    logger.debug("Running schedule")

    schedule = schedules.initialize_schedules(
        job=get_job, configs=configs, run_tasks_on_start=args.run_tasks_on_start
    )

    lines = {}
    for job in schedule.jobs:
        halves = str(job).split(",")[0].split("do")

        interval = (
            halves[0]
            .strip()
            .replace("Every", "")
            .replace("1 day", "Daily")
            .replace("1 week", f"{calendar.day_name[job.next_run.weekday()]}s")
            .strip()
        )

        name = halves[1].lower().replace("get_job(name=", "").replace("'", "").strip()

        if interval.endswith(":00"):
            interval = interval[:-3]
        if name not in lines:
            lines[name] = []

        lines[name].append(interval)

    message_lines = []
    for job_name, intervals in lines.items():
        message_lines.append(f"- *{job_name}*: {', '.join(intervals)}")

    utils.send_notifications(
        name="scheduler",
        configs=configs,
        ckan_url=configs["ckan"][active_env]["address"],
        message_type="success",
        msg="Schedule started for jobs:\n{}".format("\n".join(message_lines)),
    )

    try:
        while True:
            schedule.run_pending()
    except Exception:
        logger.error(traceback.format_exc())
