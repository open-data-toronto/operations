from utils import common as utils
import schedule

datasets = {
    "rain_gauge_locations_and_precipitation": [schedule.every().day.at("10:00")],
    "covid_19_cases_in_toronto": [schedule.every().tuesday.at("09:59")],
}

tasks = {
    "get_empty_datastore_resources": [schedule.every().day.at("09:00")],
    "update_data_quality_scores": [
        schedule.every().monday.at("16:00"),
        schedule.every().thursday.at("16:00"),
    ],
    "get_last_refreshed_gap": [
        schedule.every().monday.at("11:00"),
        schedule.every().thursday.at("11:00"),
    ],
    "sync_remote_file_times": [schedule.every(2).hours],
}


def get_schedules(job, configs, get_tasks=False, get_datasets=False):
    dataset_schedules = datasets if get_datasets else {}
    tasks_schedules = tasks if get_tasks else {}

    for job_name, intervals in {**tasks_schedules, **dataset_schedules}.items():
        logger = utils.make_logger(
            name=job_name, logs_dir=configs["directories"]["logs"]
        )

        [run.do(job, name=job_name, logger=logger) for run in intervals]

    return schedule
