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
CKAN = ckanapi.RemoteCKAN(**CREDS["ckan"])

PACKAGE_ID = PATH.name

print(CREDS)

