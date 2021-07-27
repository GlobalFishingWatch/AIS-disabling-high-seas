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
import pyseas.maps
import pyseas.maps.rasters
import pyseas.styles
import pyseas.cm

# project specific functions
import utils 

# %load_ext autoreload
# %autoreload 2

# BigQuery client
client = bigquery.Client()
# -

# ### Inputs & Parameters

# +
# Input BQ datasets/tables
gfw_research = 'gfw_research'
gfw_research_precursors = 'gfw_research_precursors'
destination_dataset = 'scratch_tyler'

pipeline_version = 'v20201001'
pipeline_table = 'pipe_{}'.format(pipeline_version)
segs_table = 'pipe_{}_segs'.format(pipeline_version)
vi_version = 'v20210301'
vd_version = 'v20210601'

# Output tables version
output_version = 'v20210722'
create_tables = True

# Date range
start_date = date(2017,1, 1)
end_date = date(2017,1, 4)

# Min gap hours
min_gap_hours = 6
# -

# Generate list of dates to produce for analysis.

# Generate list of dates to run
dates_to_run = utils.daterange(start_date, end_date)
tp = []
for dt in dates_to_run:
    tp.append(dt.strftime("%Y-%m-%d"))

# # AIS Gaps dataset
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

# Create tables for off/on events.

if create_tables:
    # Off events
    utils.make_bq_partitioned_table(destination_dataset, off_events_table)
    # On events
    utils.make_bq_partitioned_table(destination_dataset, on_events_table)

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
                                min_gap_hours = min_gap_hours, 
                                precursors_dataset=destination_dataset,
                                destination_table=off_events_table)
    cmds.append(cmd)

# +
# test query
# test_cmd = cmds[0].split('|')[0]
# os.system(test_cmd)
# os.system(cmds[0])
# -

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
                                min_gap_hours = min_gap_hours, 
                                precursors_dataset=destination_dataset,
                                destination_table=on_events_table)
    on_cmds.append(cmd)

# +
# test query
# test_cmd = on_cmds[0].split('|')[0]
# os.system(test_cmd)
# -

# Run queries
utils.execute_commands_in_parallel(commands=on_cmds)

# ### Gap events
#
# Combine off and on events into gap events.
#
# Create gap events table, partitioning on the `gap_start` field.

if create_tables:
    gap_tbl_cmd = "bq mk --schema=gaps/ais_gap_events.json \
    --time_partitioning_field=gap_start \
    --time_partitioning_type=DAY {}.{}".format(destination_dataset, 
                                               gap_events_table)
    os.system(gap_tbl_cmd)

latest_date = tp[-1]
gap_cmd = utils.make_ais_gap_events_table(off_events_table = off_events_table,
                                on_events_table = on_events_table,
                                date = latest_date,
                                precursors_dataset = destination_dataset,
                                destination_dataset = destination_dataset,
                                destination_table = gap_events_table)

# +
# test query
# test_cmd = gap_cmd.split('|')[0]
# os.system(test_cmd)
# os.system(test_cmd)
# -

# Run command
os.system(gap_cmd)

# Update schema
gap_schema_cmd = "bq update --schema=gaps/ais_gap_events.json {}.{}".format(destination_dataset, gap_events_table)
os.system(gap_schema_cmd)

# ### Fishing vessel gaps
#
# Lastly, subset the `ais_gap_events_vYYYYMMDD` dataset to only include fishing vessels.
#
# **TODO** 

# # Loitering
# Produce datasets of loitering events and gridded loitering activity (at quarter degree) for use by the drivers of suspected disabling model.

# Destination tables
loitering_events_table = 'loitering_events_{}'.format(output_version)
gridded_loitering_table = 'gridded_loitering_{}'.format(output_version)

# ## Loitering events and gridded loitering
#
# Query all carrier loitering events between 2017-2019. This query does not exclude loitering events that have overlapping encounters under the assumption that carrier vessels having encounters could also be meeting non-broadcasting fishing vessels at the same time. 
#
# After extracting events, produce a gridded dataset of all loitering events at quarter degree resolution.

loitering_cmd = utils.make_loitering_events_table(vd_version = vd_version,
                                                  pipeline_version = pipeline_version,
                                                  destination_dataset = destination_dataset,
                                                  destination_table = loitering_events_table)

# Run query
if create_tables:
    os.system(loitering_cmd)

gridded_loitering_cmd = utils.make_gridded_loitering_table(destination_dataset = destination_dataset,
                                                           output_version = output_version,
                                                           destination_table = gridded_loitering_table)
# gridded_loitering_cmd

if create_tables:
    os.system(gridded_loitering_cmd)

# # AIS Interpolation
#
# The next step is to generate tables of interpolated vessel positions. These tables are used subsequently for the following:
# - AIS reception
# - Time lost to gaps
#
# > The original reception quality method used a slightly different interpolation [query](https://github.com/GlobalFishingWatch/ais-gaps-and-reception/blob/master/data-production/hourly_interpoloation_v20191120.sql.j2)/table (`gfw_research_precursors.ais_positions_byssvid_hourly_v20191118`) than the [query](https://github.com/GlobalFishingWatch/ais-gaps-and-reception/blob/master/data-production/pipe-interpolation/hourly_interpoloation_v20201027.sql.j2) used to estimate time lost to gaps. These approaches have been combined/streamlined into the `interpolation/hourly_interpolation_byseg.sql.j2` in this repo.
#
# ### Create tables
#
# First create empty date partitioned tables to store interpolated positions.

# +
# Destination tables
ais_positions_hourly = 'ais_positions_byssvid_hourly_{}'.format(output_version)
ais_positions_hourly_fishing = 'ais_positions_byssvid_hourly_fishing_{}'.format(output_version)
gap_positions_hourly = 'gap_positions_byssvid_hourly_{}'.format(output_version)
loitering_positions_hourly = 'loitering_positions_byssvid_hourly_{}'.format(output_version)

# By seg_id
ais_positions_hourly = 'ais_positions_byseg_hourly_{}'.format(output_version)
# -

if create_tables:
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

# ### Interpolate fishing vessel positions
# Interpolate positions for fishing vessels, including both `nnet_score` and `night_loitering` for determining when `squid_jiggers` are fishing.
#
# TODO: Update this to the same logic as for all vessels.

# +
# Store commands
# int_fishing_cmds = []
# for t in tp:
#     cmd = utils.make_hourly_fishing_interpolation_table(date = t,
#                                                 destination_dataset = destination_dataset,
#                                                 destination_table = ais_positions_hourly_fishing)
#     int_fishing_cmds.append(cmd)

# +
# utils.execute_commands_in_parallel(int_fishing_cmds)
# -

# ### Interpolate positions during AIS gap events
#
# > **Note:** Interpolating positions between gap events was originally done using the `raw_gaps_vYYYYMMDD` table, which included the gaps with additional parameters applied to them - e.g. `pos_x_hours_before`. Need to produce a version of this table or interpolate the gap events as is.

# ## AIS Reception Quality
#
# Model AIS satellite reception quality to identify regions where AIS gap events are more/less suspicious. This is produced using the following process:
#
# **1. Calculate measured reception** - Calculates measured reception quality by AIS Class as the average number of positions received by a vessel in a day per one-degree grid cell
#
# **2. Interpolate reception** - To produce global maps of reception quality (e.g. not just in cells with AIS data) use a smoothing function to interpolate reception quality. 
#
# ### Create tables

sat_reception_measured = 'sat_reception_measured_one_degree_{}'.format(output_version)
sat_reception_smoothed = 'sat_reception_smoothed_one_degree_{}'.format(output_version)

if create_tables:
    # measured reception quality
    utils.make_bq_partitioned_table(destination_dataset, sat_reception_measured)
    # smoothed reception quality
    utils.make_bq_partitioned_table(destination_dataset, sat_reception_smoothed)

# ### Measured reception quality

# Make list of month start dates for reception quality
reception_dates = pd.date_range(start_date, end_date, freq='1M') - pd.offsets.MonthBegin(1)

# Generate commands
mr_cmds = []
for r in reception_dates:
    print(str(r.date()))
    cmd = utils.make_reception_measured_table(destination_table = sat_reception_measured, 
                                        destination_dataset = destination_dataset,
                                        start_date = r, 
                                        vi_version = vi_version, 
                                        segs_table="{}.{}".format("gfw_research", segs_table),
                                        output_version = output_version)

    mr_cmds.append(cmd)

utils.execute_commands_in_parallel(mr_cmds)

# ### Smoothed reception quality
#
# Next, interpolate the measured reception quality using a radial basis function. 

for r in reception_dates:
    print(str(r.date()))
    utils.make_smooth_reception_table(start_date = r,
                                      reception_measured_table = sat_reception_measured,
                                      destination_dataset = destination_dataset,
                                      destination_table = sat_reception_smoothed)

# #### Plot reception quality

for r in reception_dates:
    print(str(r.date()))
    utils.plot_reception_quality(reception_start_date = r,
                           destination_dataset = destination_dataset,
                           reception_smoothed_table = sat_reception_smoothed)


