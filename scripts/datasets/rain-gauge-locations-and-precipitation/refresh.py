import requests
import ckanapi
from dateutil import parser
from datetime import datetime, timedelta
import calendar
import pandas as pd
from io import BytesIO
import tempfile
from pathlib import Path
from time import sleep
import os

import os
import traceback
from pathlib import Path
import sys

sys.path.append("../..")
from utils import common

PATH = Path(os.path.abspath(__file__))
TOOLS = common.Helper(PATH)

LOGGER, CREDS, DIRS = TOOLS.get_all()
CKAN = ckanapi.RemoteCKAN(**CREDS)

PACKAGE_ID = PATH.name

APIKEY = "E8EF5292-F128-468D-BFBC-AEC1BE7AB8AC"

print({"path": PATH, "package_id": PACKAGE_ID, "apikey": APIKEY})
