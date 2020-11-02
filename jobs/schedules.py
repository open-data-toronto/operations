from utils import common as utils
import schedule

datasets = {
    "rain_gauge_locations_and_precipitation": [schedule.every().day.at("10:00")],
    "covid_19_cases_in_toronto": [schedule.every().wednesday.at("09:59")],
}

tasks = {
    "get_empty_datastore_resources": [
        schedule.every().day.at("09:00"),
        schedule.every().day.at("15:00"),
    ],
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


def initialize_schedules(job, configs, run_tasks_on_start=False):
    for job_name, intervals in {**tasks, **datasets}.items():
        logger = utils.make_logger(
            name=job_name, logs_dir=configs["directories"]["logs"]
        )

        for idx, run in enumerate(intervals):
            scheduled_job = run.do(job, name=job_name, logger=logger)
            if idx == 0 and job_name in tasks and run_tasks_on_start:
                scheduled_job.run()

    return schedule
