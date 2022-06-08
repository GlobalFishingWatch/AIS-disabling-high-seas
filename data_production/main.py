""" main.py

    DESCRIPTION: This script produces the follwing AIS-based input
    tables for Welch et al. (2022)
        1. AIS gap events
        2. AIS reception quality
        3. AIS fishing vessel presence and fishing activity
        4. AIS loitering events by carrier vessels
"""

# Modules
import argparse
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import time
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from jinja2 import Template
import concurrent.futures

# Function to verify valid dates in script arguments
def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

# Create set of dates
def daterange(date1, date2):
    for n in range(int((date2 - date1).days)+1):
        yield date1 + timedelta(n)

parser = argparse.ArgumentParser(description='AIS disabling')
parser.add_argument('--destination_dataset', type=str,
                    help='BigQuery destination dataset', default='scratch_tyler')
parser.add_argument('--pipeline_version', type=str,
                    help='AIS pipeline version', default='v20201001')
parser.add_argument('--vi_version', type=str,
                    help='vi_version', default='v20220401')
parser.add_argument('--output_version', type=str,
                    help='output_version', default='v20220606')
parser.add_argument('--event_type', type=str,
                    help='Event type (off/on/gap)', default='off')
parser.add_argument('--start_date', type=valid_date,
                    help='Start date. Format: YYYY-MM-DD', default='2021-01-04')
parser.add_argument('--end_date', type=valid_date,
                    help='End date. Format: YYYY-MM-DD', default='2021-01-06')
parser.add_argument('--test', type=bool,
                    help='Whether to simply print the rendered query', default=True)

args = parser.parse_args()
print("Final args", args)

### Inputs & Parameters
pipeline_version = args.pipeline_version
destination_dataset = args.destination_dataset
vi_version = args.vi_version
output_version = args.output_version
start_date = args.start_date
end_date = args.end_date
