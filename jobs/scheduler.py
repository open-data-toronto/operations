import schedule
from utils import common as utils


def get_job_schedule(job, configs):
    def make_logger(name):
        return utils.make_logger(name=name, logs_dir=configs["directories"]["logs"])

    # TASKS
    schedule.every(2).minutes.do(
        job,
        name="sync_remote_file_times",
        logger=make_logger(name="sync_remote_file_times",),
    )

    # schedule.every(2).hours.do(
    #     job,
    #     name="sync_remote_file_times",
    #     logger=make_logger(name="sync_remote_file_times",),
    # )

    # schedule.every().day.at("09:00").do(
    #     job,
    #     name="get_empty_datastore_resources",
    #     logger=make_logger("get_empty_datastore_resources",),
    # )

    # schedule.every().monday.at("11:00").do(
    #     job,
    #     name="get_last_refreshed_gap",
    #     logger=make_logger("get_last_refreshed_gap",),
    # )

    # schedule.every().thursday.at("11:00").do(
    #     job,
    #     name="get_last_refreshed_gap",
    #     logger=make_logger("get_last_refreshed_gap",),
    # )

    # schedule.every().monday.at("16:00").do(
    #     job,
    #     name="update_data_quality_scores",
    #     logger=make_logger("update_data_quality_scores",),
    # )

    # schedule.every().thursday.at("16:00").do(
    #     job,
    #     name="update_data_quality_scores",
    #     logger=make_logger("update_data_quality_scores",),
    # )

    # DATASETS
    return schedule
