import subprocess
#import os
#import pandas as pd
#import numpy as np
#import pandas_gbq
#from datetime import datetime, timedelta, date
#from dateutil.relativedelta import relativedelta
#import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from jinja2 import Template

from ais_disabling import config_ole
from ais_disabling import utils

# BigQuery client
client = bigquery.Client()

# ### Inputs & Parameters

# Input BQ datasets/tables
gfw_research = config_ole.gfw_research
gfw_research_precursors = config_ole.gfw_research_precursors
proj_dataset = config_ole.proj_dataset
destination_dataset = config_ole.destination_dataset

pipeline_version = config_ole.pipeline_version
pipeline_dataset = f'pipe_production_{pipeline_version}'
pipeline_table = config_ole.pipeline_table
segs_table = config_ole.segs_table
vi_version = config_ole.vi_version
vd_version = config_ole.vd_version

# Output tables version
output_version = config_ole.output_version
create_tables = config_ole.create_tables

# Date range
start_date = config_ole.start_date
end_date = config_ole.end_date

# list of dates to run
tp = config_ole.tp

# Tables to run. One of 'vessels', 'gaps', 'fishing'
tables_to_run = config_ole.tables_to_run

#########################################################################
# VESSEL LIST
# Produce the list of fishing vessels used for the analysis
#########################################################################
if 'vessels' in tables_to_run:
    with open('data_production/ole/fishing_vessels_ole.sql.j2') as f:
        template = Template(f.read())

    query = template.render(
        destination_dataset = config_ole.destination_dataset,
        destination_table = config_ole.fishing_vessels_table,
        vi_version = config_ole.vi_version,
        start_date = str(config_ole.start_date),
        end_date = str(config_ole.end_date)
        )

    if config_ole.test_run:
        print(query)

    if config_ole.test_run is False:
        subprocess.run("bq query".split(), input=bytes(query, "utf-8"))

#########################################################################
# GAPS
# Produce the dataset of all AIS gaps by the vessels. This uses the 
# automated gaps pipeline rather than regenerating the full dataset.
#########################################################################
if 'gaps' in tables_to_run:
    with open('data_production/ole/ais_gaps_ole.sql.j2') as f:
        template = Template(f.read())

    query = template.render(
        destination_dataset = config_ole.destination_dataset,
        destination_table = config_ole.gap_events_features_table,
        fishing_vessels_table = config_ole.fishing_vessels_table,
        start_date = str(config_ole.start_date),
        end_date = str(config_ole.end_date)
        )

    if config_ole.test_run:
        print(query)

    if config_ole.test_run is False:
        subprocess.run("bq query".split(), input=bytes(query, "utf-8"))

#########################################################################
# FISHING
# Produce the dataset of all fishing vessel presence, removing hours for 
# positions that are allocated more than 12 hours to avoid double counting
# with time spent in gaps
#########################################################################
if 'fishing' in tables_to_run:
    with open('data_production/ole/gridded_fishing_daily_ole.sql.j2') as f:
        template = Template(f.read())

    query = template.render(
        destination_dataset = config_ole.destination_dataset,
        destination_table = config_ole.fishing_table,
        fishing_vessels_table = config_ole.fishing_vessels_table,
        start_date = str(config_ole.start_date),
        end_date = str(config_ole.end_date)
        )

    if config_ole.test_run:
        print(query)

    if config_ole.test_run is False:
        subprocess.run("bq query".split(), input=bytes(query, "utf-8"))

