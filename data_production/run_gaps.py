# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
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
from google.cloud.exceptions import NotFound
from jinja2 import Template

import pyseas
import pyseas.maps
import pyseas.maps.rasters
import pyseas.styles
import pyseas.cm

# project specific functions
from ais_disabling import utils
from ais_disabling import config

# %load_ext autoreload
# %load_ext google.cloud.bigquery
# %autoreload 2

# BigQuery client
client = bigquery.Client()
# -

# ### Inputs & Parameters

# +
# Input BQ datasets/tables
gfw_research = config.gfw_research
gfw_research_precursors = config.gfw_research_precursors
proj_dataset = config.proj_dataset
destination_dataset = config.destination_dataset

pipeline_version = config.pipeline_version
pipeline_dataset = f'pipe_production_{pipeline_version}'
pipeline_table = config.pipeline_table
segs_table = config.segs_table
vi_version = config.vi_version
vd_version = config.vd_version

# Output tables version
output_version = config.output_version
create_tables = config.create_tables

# Date range
start_date = config.start_date
end_date = config.end_date

# Min gap hours
min_gap_hours = config.min_gap_hours
# -

# list of dates to run
tp = config.tp

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
off_events_table = config.off_events_table
on_events_table = config.on_events_table
gap_events_table = config.gap_events_table

# Create tables for off/on events.

# +
# Off events
try:
    client.get_table("{d}.{t}".format(d = destination_dataset, t = off_events_table))
    print("Table {d}.{t} already exists".format(d = destination_dataset, t = off_events_table))
except NotFound:
    print("Table {d}.{t} is not found.".format(d = destination_dataset, t = off_events_table))
    print("Creating table {d}.{t}.".format(d = destination_dataset, t = off_events_table))
    # create off events table if needed
    utils.make_bq_partitioned_table(destination_dataset, off_events_table)

# On events
try:
    client.get_table("{d}.{t}".format(d = destination_dataset, t = on_events_table))
    print("Table {d}.{t} already exists".format(d = destination_dataset, t = on_events_table))
except NotFound:
    print("Table {d}.{t} is not found.".format(d = destination_dataset, t = on_events_table))
    print("Creating table {d}.{t}.".format(d = destination_dataset, t = on_events_table))
    # Create on events table if needed
    utils.make_bq_partitioned_table(destination_dataset, on_events_table)
# -

# ### Off events
#
# Generate off events

# Store commands
cmds = []
for t in tp:
    cmd = utils.make_ais_events_table(
        pipeline_table="{}.{}".format(pipeline_dataset, pipeline_table),
        segs_table="{}.{}".format(pipeline_dataset, segs_table),
        event_type='off',
        date = t,
        min_gap_hours = min_gap_hours,
        precursors_dataset=destination_dataset,
        destination_table=off_events_table
    )
    cmds.append(cmd)

# test query
test_cmd = cmds[0].split('|')[0]
os.system(test_cmd)
# os.system(cmds[0])

# Run queries
utils.execute_commands_in_parallel(commands=cmds)

# ### On events
#
# Generate on events.

# Store commands
on_cmds = []
for t in tp:
    cmd = utils.make_ais_events_table(
        pipeline_table="{}.{}".format(pipeline_dataset, pipeline_table),
        segs_table="{}.{}".format(pipeline_dataset, segs_table),
        event_type='on',
        date = t,
        min_gap_hours = min_gap_hours,
        precursors_dataset=destination_dataset,
        destination_table=on_events_table
    )
    on_cmds.append(cmd)

# test query
test_cmd = on_cmds[0].split('|')[0]
os.system(test_cmd)

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
gap_cmd = utils.make_ais_gap_events_table(
    off_events_table = off_events_table,
    on_events_table = on_events_table,
    date = latest_date,
    precursors_dataset = destination_dataset,
    destination_dataset = destination_dataset,
    destination_table = gap_events_table
)

# test query
test_cmd = gap_cmd.split('|')[0]
os.system(test_cmd)
# os.system(test_cmd)

# Run command
os.system(gap_cmd)

# +
# Update schema
# gap_schema_cmd = "bq update --schema=gaps/ais_gap_events.json {}.{}".format(destination_dataset, gap_events_table)
# os.system(gap_schema_cmd)
# -

# ### Fishing vessel gaps
#
# Lastly, create the final gaps model dataset by doing the following:
#
# + subset the `ais_gap_events_vYYYYMMDD` dataset to only include fishing vessels
# + Add additional model variables not calculated at the time of the gap events creation:

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
                                                  start_date = tp[0],
                                                  end_date = tp[-1],
                                                  destination_dataset = destination_dataset,
                                                  destination_table = loitering_events_table)

# +
# test query
# print(loitering_cmd)
# test_cmd = loitering_cmd.split('|')[0]
# os.system(test_cmd)
# -

# Run query
if create_tables:
    os.system(loitering_cmd)

gridded_loitering_cmd = utils.make_gridded_loitering_table(destination_dataset = proj_dataset,
                                                           output_version = output_version,
                                                           destination_table = gridded_loitering_table,
                                                           start_date = start_date,
                                                           end_date = end_date)
gridded_loitering_cmd

if create_tables:
    os.system(gridded_loitering_cmd)

# ## Fishing
#
# Produce dataset of gridded fishing effort (at quarter degree) for use by the drivers of suspected disabling model.

# Destination tables
fishing_table = f"gridded_fishing_{output_version}"
fishing_brt_table = f"gridded_fishing_brt_{output_version}"

fishing_cmd = utils.make_gridded_fishing_table(output_version = output_version,
                                               pipeline_table="{}.{}".format(pipeline_dataset, pipeline_table),
                                               segs_table="{}.{}".format(pipeline_dataset, segs_table),
                                               start_date = start_date,
                                               end_date = end_date,
                                               vi_version = vi_version,
                                               destination_dataset = destination_dataset,
                                               destination_table = fishing_table)

# +
# test query
# test_cmd = fishing_cmd.split('|')[0]
# os.system(test_cmd)
# fishing_cmd
# -

# Run query
if create_tables:
    os.system(fishing_cmd)

# Create fishing table further aggregated by just vessel class and month for input to BRT models.

# +
fishing_brt_cmd = utils.make_gridded_fishing_brt_table(gridded_fishing_table = fishing_table,
                                                       destination_dataset = destination_dataset,
                                                       destination_table = fishing_brt_table)

#     print(fishing_brt_cmd)
# -

if create_tables:
    os.system(fishing_brt_cmd)

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
# reception_dates

# Generate commands
mr_cmds = []
for r in reception_dates:
#     print(str(r.date()))
    cmd = utils.make_reception_measured_table(destination_table = sat_reception_measured,
                                        destination_dataset = destination_dataset,
                                        start_date = r,
                                        vi_version = vi_version,
                                        segs_table="{}.{}".format("gfw_research", segs_table),
                                        output_version = output_version)

    mr_cmds.append(cmd)

utils.execute_commands_in_parallel(mr_cmds)

# +
# test query
# test_cmd = mr_cmds[0].split('|')[0]
# os.system(test_cmd)
# -

# ### Smoothed reception quality
#
# Next, interpolate the measured reception quality using a radial basis function.

for r in reception_dates:
    print(str(r.date()))
    utils.make_smooth_reception_table(start_date = r,
                                      reception_measured_table = sat_reception_measured,
                                      destination_dataset = destination_dataset,
                                      destination_table = sat_reception_smoothed)

# ### Plot reception quality
#

for r in reception_dates:
    print(str(r.date()))
    """
    Query smoothed reception data
    """
    month_reception_query = '''SELECT *
                               FROM `{d}.{t}`
                               WHERE _partitiontime = "{m}"'''.format(d = destination_dataset,
                                                                      t = sat_reception_smoothed,
                                                                      m = str(r.date())
                                                                     )
    # Query data
    month_reception = pd.read_gbq(month_reception_query, project_id='world-fishing-827', dialect='standard')

    utils.plot_reception_quality(reception_start_date = r,
                                 destination_dataset = destination_dataset,
                                 reception_smoothed_table = sat_reception_smoothed,
                                 reception_df = month_reception
                            )

# ## Final gap events table
#
# Create the final gap events table that is used as an input to the model of suspected drivers of AIS disabling. This table takes the `ais_gap_events_vYYYYMMDD` table created above and adds a handful of additional model features, including the smoothed reception quality.

gap_events_features_table = f'ais_gap_events_features_{output_version}'

# +
gap_features_cmd = utils.make_ais_gap_events_features_table(pipeline_version=pipeline_version,
                                                   vi_version=vi_version,
                                                   output_version=output_version,
                                                   start_date=str(start_date),
                                                   end_date=str(end_date),
                                                   destination_dataset=destination_dataset,
                                                   destination_table=gap_events_features_table)

# gap_features_cmd
# -

# test query
# test_cmd = gap_features_cmd.split('|')[0]
# os.system(test_cmd)
gap_features_cmd

# Run gap features query
if create_tables:
    os.system(gap_features_cmd)

# # Copy Tables
#
# If needed, copy tables to a different BigQuery dataset.

# +
# Copy destination dataset
cp_destination_dataset = 'proj_ais_gaps_catena'

# List of tables to copy
tables_to_cp = [
    off_events_table,
    on_events_table,
    gap_events_table,
    gap_events_features_table,
    loitering_events_table,
    gridded_loitering_table,
    fishing_table,
    sat_reception_measured,
    sat_reception_smoothed
]

if copy_tables:
    for t in tables_to_cp:

        print('Copying {d}.{t} \n to {cd}.{t}'.format(d = destination_dataset,
                                                  cd = cp_destination_dataset,
                                                  t = t))
        # Format query
        cp_cmd = """bq cp -n \
        {d}.{t} \
        {cd}.{t}""".format(d = destination_dataset,
                           cd = cp_destination_dataset,
                           t = t)

        # Run command
        os.system(cp_cmd)
# -
