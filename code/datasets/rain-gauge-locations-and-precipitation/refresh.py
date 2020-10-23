import calendar
import os
import sys
import tempfile
import traceback
from datetime import datetime
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from time import sleep

import ckanapi
import pandas as pd
import requests
from dateutil import parser

sys.path.append("../..")
from utils import common

PATH = Path(os.path.abspath(__file__))
TOOLS = common.Helper(PATH)

LOGGER, CREDS, DIRS = TOOLS.get_all()
CKAN = ckanapi.RemoteCKAN(**CREDS)

PACKAGE_ID = PATH.name

APIKEY = "E8EF5292-F128-468D-BFBC-AEC1BE7AB8AC"

print({"path": PATH, "package_id": PACKAGE_ID, "apikey": APIKEY})
