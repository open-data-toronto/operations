import calendar
import logging
import requests
from datetime import datetime, timedelta
from typing import List
from pathlib import Path

from airflow.models import Variable

# Oracle Infinity Report Params
reports = {
    "Page Views and Time Based Metrics": "a59fdb579e3a44f1a5fa84a0f6c24ea2",
    "File URL Clicks": "0bc8dea04fb34c740b0cad90077a59aa",
}


def determine_latest_period_loaded(dir_path: str):
    """Determine last loaded period.
    Returns:
        dates_loaded: Last loaded begin date
    """

    dates_loaded = [
        datetime.strptime(p.name, "%Y%m")
        for p in dir_path.iterdir()
        if p.is_file() is False
    ]

    if not dates_loaded:
        datetimenow = datetime.now().replace(microsecond=0)
        past_date_before_2yrs = datetimenow - timedelta(days=760)
        return past_date_before_2yrs

    return max(dates_loaded)


def calculate_periods_to_load(latest_loaded) -> List:
    """Calculate new periods that need to be loaded.
    Returns:
        periods_to_load: List of all new periods.
    """

    logging.info("Calculating months to load")
    periods_to_load = []

    logging.info(f"--------{latest_loaded}--------")
    begin = latest_loaded + timedelta(days=32)
    month_end_day = calendar.monthrange(begin.year, begin.month)[1]
    end = datetime(begin.year, begin.month, month_end_day)
    logging.info(f"-------{begin}--{month_end_day}--{end}--")

    while end < datetime.now():
        periods_to_load.append(
            {
                "begin": datetime.strftime(begin, "%Y/%m/1/0"),
                "end": datetime.strftime(end, "%Y/%m/%d/23"),
            }
        )

        begin = begin + timedelta(days=31)

        if begin.month == 12:
            end = datetime(begin.year, begin.month, 31)
        else:
            end = datetime(begin.year, begin.month + 1, 1) + timedelta(days=-1)

    return periods_to_load


def generate_usage_report(
    report_name: str,
    report_id: str,
    begin: str,
    end: str,
    account_id: str,
    user: str,
    password: str,
) -> requests.models.Response:
    """Generate usage report from Oracle Infinity

    Args:
        report_name (str): Name of the usage report
        report_id (str): Id of the usage report
        begin (str): Begin of the time period
        end (str): End of the time period
        account_id (str): oracle infinity account id
        user (str): oracle infinity user name
        password (str): oracle infinity user password

    Returns:
        response: requests.Response() to the HTTP request.
    """

    report_args = {
        "format": "csv",
        "timezone": "America/New_York",
        "suppressErrorCodes": "true",
        "autoDownload": "true",
        "download": "false",
    }

    qs = "&".join(["{}={}".format(k, v) for k, v in report_args.items()])
    prefix = f"https://api.oracleinfinity.io/v1/account/{account_id}/dataexport"

    call = f"{prefix}/{report_id}/data?begin={begin}&end={end}&{qs}"
    logging.info(f"Begin: {begin} | End: {end} | {report_name.upper()} | {call}")

    response = requests.get(call, auth=(user, password))
    status = response.status_code

    assert status == 200, f"Response code: {status}. Reason: {response.reason}"

    return response


def extract_new_report(
    report_name: str, periods_to_load: List[str], dest_path: List[str]
) -> List[str]:
    """Extract Reports for each period and save in folder

    Args:
        report_name (str): Name of the report
        periods_to_load (list): List of all the periods
        dest_path (str): Filepath of output files

    Returns:
        file_paths: List of output filepaths
    """

    account_id = Variable.get("oracle_infinity_account_id")
    user = Variable.get("oracle_infinity_user")
    password = Variable.get("oracle_infinity_password")

    report_id = reports[report_name]

    file_paths = []

    for period in periods_to_load:
        ym = datetime.strptime(period["end"], "%Y/%m/%d/%H").strftime("%Y%m")

        dir_path = Path(dest_path) / ym
        dir_path.mkdir(parents=False, exist_ok=True)

        fpath = dir_path / (report_name + "_" + ym + ".csv")

        file_paths.append(str(fpath))

        response = generate_usage_report(
            report_name=report_name,
            report_id=report_id,
            begin=period["begin"],
            end=period["end"],
            account_id=account_id,
            user=user,
            password=password,
        )

        logging.info(f"Report Extracted for period {ym}")
        with open(fpath, "wb") as f:
            f.write(response.content)

    return file_paths
