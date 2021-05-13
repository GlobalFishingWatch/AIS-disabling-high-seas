# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Generate AIS reception and gap events datasets
#
# This notebook is a wrapper file for producing a complete set of results and inputs for Welch et al. (2021). It contains code for the following: 
#
# 1. Generate raw AIS gap events greater than 12 hours
# 2. Generate monthly AIS reception maps
# 3. Detect suspected AIS disabling events
#
# ## Setup
#
# ### Packages

# +
# Modules
import os
import pandas as pd
import numpy as np
import pandas_gbq
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import time
from google.cloud import bigquery
from jinja2 import Template
import pyseas

# project specific functions
import utils 

# %load_ext autoreload
# %autoreload 2

# BigQuery client
# client = bigquery.Client()
# -

# ### Inputs & Parameters

# +
# Input BQ datasets/tables
gfw_research = 'gfw_research'
gfw_research_precursors = 'gfw_research_precursors'
destination_dataset = 'scratch_tyler''

pipeline_version = 'v20201001'
pipeline_table = 'pipe_{}'.format(pipeline_version)
segs_table = 'pipe_{}_segs'.format(pipeline_version)
vi_version = 'v20210301'

# Output tables version
output_version = 'v20210429'

# Date range
start_date = date(2017, 1, 1)
end_date = date(2017, 1, 31)
# -

# Generate list of dates to produce for analysis.

# Generate list of dates to run
dates_to_run = utils.daterange(start_date, end_date)
tp = []
for dt in dates_to_run:
    tp.append(dt.strftime("%Y-%m-%d"))

# ## AIS Gaps dataset
#
# Generate a dataset of AIS gaps for time range. This involves running the following query sequence (queries in the `gaps` subdirectory):
# 1. AIS off events: `ais_off_on_events.sql.j2` with `event` parameter set to `'off'`
# 2. AIS on events: `ais_off_on_events.sql.j2` with `event` parameter set to `'on'`
# 3. AIS gap events: Stitch off and on events together into gap events using `ais_gap_events.sql.j2`
#
# ### Create tables
#
# First, create empty tables for all three tables.

# Destination tables
off_events_table = 'ais_off_events_{}'.format(output_version)
on_events_table = 'ais_on_events_{}'.format(output_version)
gap_events_table = 'ais_gap_events_{}'.format(output_version)

# Create tables.

# Off events
utils.make_bq_partitioned_table(destination_dataset, off_events_table)
# On events
utils.make_bq_partitioned_table(destination_dataset, on_events_table)
# Gap events
utils.make_bq_partitioned_table(destination_dataset, gap_events_table)

# ### Off events
#
# Generate off events

# Store commands
cmds = []
for t in tp:
    cmd = utils.make_ais_events_table(pipeline_table="{}.{}".format("gfw_research", pipeline_table),
                                segs_table="{}.{}".format("gfw_research", segs_table),
                                event_type='off',
                                date = t,
                                min_gap_hours=12, 
                                precursors_dataset=destination_dataset,
                                destination_table=off_events_table)
    cmds.append(cmd)

# Run queries
utils.execute_commands_in_parallel(commands=cmds)

# ### On events
#
# Generate on events.

# Store commands
on_cmds = []
for t in tp:
    cmd = utils.make_ais_events_table(pipeline_table="{}.{}".format("gfw_research", pipeline_table),
                                segs_table="{}.{}".format("gfw_research", segs_table),
                                event_type='on',
                                date = t,
                                min_gap_hours=12, 
                                precursors_dataset=destination_dataset,
                                destination_table=on_events_table)
    on_cmds.append(cmd)

# Run queries
utils.execute_commands_in_parallel(commands=on_cmds)

# ### Gap events
#
# Combine off and on events into gap events.

latest_date = tp[-1]
gap_cmd = utils.make_ais_gap_events_table(off_events_table = off_events_table,
                                on_events_table = on_events_table,
                                date = latest_date,
                                precursors_dataset = destination_dataset,
                                destination_dataset = destination_dataset,
                                destination_table = gap_events_table)

# Run command
os.system(gap_cmd)

# ## AIS Interpolation
#
# The next step is to generate tables of interpolated vessel positions. These tables are used subsequently for the following:
# - AIS reception
# - Time lost to gaps
#
# ### Create tables
#
# First create empty date partitioned tables to store interpolated positions.

# Destination tables
ais_positions_hourly = 'ais_positions_byssvid_hourly_{}'.format(output_version)
ais_positions_hourly_fishing = 'ais_positions_byssvid_hourly_fishing_{}'.format(output_version)
gap_positions_hourly = 'gap_positions_byssvid_hourly_{}'.format(output_version)
loitering_positions_hourly = 'loitering_positions_byssvid_hourly_{}'.format(output_version)

# all positions hourly
utils.make_bq_partitioned_table(destination_dataset, ais_positions_hourly)
# fishing vessel positions hourly
utils.make_bq_partitioned_table(destination_dataset, ais_positions_hourly_fishing)
# gap positions hourly
utils.make_bq_partitioned_table(destination_dataset, gap_positions_hourly)

# ### Interpolate all vessel positions
#
# Interpolate positions for all vessels.

# Store commands
int_cmds = []
for t in tp:
    cmd = utils.make_hourly_interpolation_table(date = t,
                                                destination_dataset = destination_dataset,
                                                destination_table = ais_positions_hourly)
    int_cmds.append(cmd)

utils.execute_commands_in_parallel(int_cmds)


