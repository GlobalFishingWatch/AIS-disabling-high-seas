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
import utils 

# %load_ext autoreload
# %load_ext google.cloud.bigquery
# %autoreload 2

# BigQuery client
client = bigquery.Client()
# -

# ### Inputs & Parameters

# +
# Input BQ datasets/tables
gfw_research = 'gfw_research'
gfw_research_precursors = 'gfw_research_precursors'
proj_dataset = 'proj_ais_gaps_catena'
destination_dataset = 'proj_ais_gaps_catena'

pipeline_version = 'v20201001'
pipeline_dataset = f'pipe_production_{pipeline_version}'
pipeline_table = 'research_messages'
segs_table = 'research_segs'
vi_version = 'v20220401'
vd_version = 'v20220401'

# Output tables version
output_version = 'v20220606'
create_tables = True

# Date range
start_date = date(2017,1, 1)
end_date = date(2019,12, 31)

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

# Check for dates that did not complete properly and re-run.

# %%bigquery missing_on_dates
WITH
--
on_dates AS (
  SELECT
    DISTINCT _partitiontime as dates,
    'on_events' as event
  FROM `world-fishing-827.proj_ais_gaps_catena.ais_on_events_v20220606`
),
--
all_dates AS (
  SELECT
  GENERATE_DATE_ARRAY('2017-01-01', '2019-12-31', INTERVAL 1 DAY) as dates
)
--
SELECT 
  dates 
FROM all_dates, UNNEST(dates) as dates
WHERE dates NOT IN (
  SELECT DATE(dates) FROM on_dates
)

for d in missing_on_dates['dates']:
    print(str(d))
    cmd = utils.make_ais_events_table(
        pipeline_table="{}.{}".format(pipeline_dataset, pipeline_table),
        segs_table="{}.{}".format(pipeline_dataset, segs_table),
        event_type='on',
        date = str(d),
        min_gap_hours = min_gap_hours, 
        precursors_dataset=destination_dataset,
        destination_table=on_events_table
    )
    os.system(cmd)

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

# # AIS Interpolation
#
# The next step is to generate tables of interpolated vessel positions. These tables are used subsequently for the following:
# - AIS reception
# - Time lost to gaps
#
# > The original reception quality method used a slightly different interpolation [query](https://github.com/GlobalFishingWatch/ais-gaps-and-reception/blob/master/data-production/hourly_interpoloation_v20191120.sql.j2)/table (`gfw_research_precursors.ais_positions_byssvid_hourly_v20191118`) than the [query](https://github.com/GlobalFishingWatch/ais-gaps-and-reception/blob/master/data-production/pipe-interpolation/hourly_interpoloation_v20201027.sql.j2) used to estimate time lost to gaps. These approaches have been combined/streamlined into the `interpolation/hourly_interpolation_byseg.sql.j2` in this repo. This query was used to generate the interpolated positions for reception quality and is similar in form to the query used to generate interpolated fishing vessels positions (`hourly_fishing_interpolation.sql.j2`).
#
# ### Create tables
#
# First create empty date partitioned tables to store interpolated positions.

# Destination tables
ais_positions_hourly = 'ais_positions_byseg_hourly_{}'.format(output_version)
ais_positions_hourly_fishing = 'ais_positions_byseg_hourly_fishing_{}'.format(output_version)
loitering_positions_hourly = 'loitering_positions_byseg_hourly_{}'.format(output_version)

# Create tables:

# +
# all positions hourly
try:
    client.get_table("{d}.{t}".format(d = destination_dataset, t = ais_positions_hourly))
    print("Table {d}.{t} already exists".format(d = destination_dataset, t = ais_positions_hourly))
except NotFound:
    print("Table {d}.{t} is not found.".format(d = destination_dataset, t = ais_positions_hourly))
    print("Creating table {d}.{t}.".format(d = destination_dataset, t = ais_positions_hourly))
    # create table if needed
    utils.make_bq_partitioned_table(destination_dataset, ais_positions_hourly)

# fishing vessel positions
try:
    client.get_table("{d}.{t}".format(d = destination_dataset, t = ais_positions_hourly_fishing))
    print("Table {d}.{t} already exists".format(d = destination_dataset, t = ais_positions_hourly_fishing))
except NotFound:
    print("Table {d}.{t} is not found.".format(d = destination_dataset, t = ais_positions_hourly_fishing))
    print("Creating table {d}.{t}.".format(d = destination_dataset, t = ais_positions_hourly_fishing))
    # Create table if needed
    utils.make_bq_partitioned_table(destination_dataset, ais_positions_hourly_fishing)
        
# loitering positions
try:
    client.get_table("{d}.{t}".format(d = destination_dataset, t = loitering_positions_hourly))
    print("Table {d}.{t} already exists".format(d = destination_dataset, t = loitering_positions_hourly))
except NotFound:
    print("Table {d}.{t} is not found.".format(d = destination_dataset, t = loitering_positions_hourly))
    print("Creating table {d}.{t}.".format(d = destination_dataset, t = loitering_positions_hourly))
    # Create table if needed
    utils.make_bq_partitioned_table(destination_dataset, loitering_positions_hourly)
# -

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

# Store commands
int_fishing_cmds = []
for t in tp:
    cmd = utils.make_hourly_fishing_interpolation_table(date = t,
                                                destination_dataset = destination_dataset,
                                                destination_table = ais_positions_hourly_fishing)
    int_fishing_cmds.append(cmd)

# +
# test query
# test_cmd = int_fishing_cmds[0].split('|')[0]
# test_cmd
# os.system(test_cmd)
# -

# Run commands
utils.execute_commands_in_parallel(int_fishing_cmds)

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

# Plot average reception quality across the full time series

# +
"""
Query smoothed reception data
"""
month_reception_query = '''SELECT 
                           lat_bin,
                           lon_bin,
                           class,
                           AVG(positions_per_day) as positions_per_day
                           FROM `{d}.{t}`
                           WHERE _partitiontime BETWEEN "2017-01-01" 
                           AND "2019-12-01"
                           GROUP BY 1,2,3'''.format(d = destination_dataset,
                                                      t = sat_reception_smoothed)
# Query data
month_reception = pd.read_gbq(month_reception_query, project_id='world-fishing-827', dialect='standard')

utils.plot_reception_quality(reception_start_date = r,
                             destination_dataset = destination_dataset,
                             reception_smoothed_table = sat_reception_smoothed,
                             reception_df = month_reception
                        )
# -

# ### Alternate reception quality

# +
# Create tables for alternate speed reception quality
sat_reception_measured_all_speeds_tbl = 'sat_reception_measured_one_degree_all_speeds_{}'.format(output_version)
sat_reception_smoothed_all_speeds_tbl = 'sat_reception_smoothed_one_degree_all_speeds_{}'.format(output_version)

if create_tables:
    # measured reception quality
    utils.make_bq_partitioned_table(destination_dataset, sat_reception_measured_all_speeds_tbl)
    utils.make_bq_partitioned_table(destination_dataset, sat_reception_smoothed_all_speeds_tbl)
# -

# Generate commands
mr_all_speed_cmds = []
for r in reception_dates:
#     print(str(r.date()))
    cmd = utils.make_reception_measured_table(destination_table = sat_reception_measured, 
                                        destination_dataset = destination_dataset,
                                        start_date = r, 
                                        vi_version = vi_version, 
                                        segs_table="{}.{}".format("gfw_research", segs_table),
                                        output_version = output_version,
                                        include_all_speeds = "True")

    mr_all_speed_cmds.append(cmd)

# Run commands
utils.execute_commands_in_parallel(mr_all_speed_cmds)

# Now smooth the alternate reception quality maps.

for r in reception_dates[8:]:
    print(str(r.date()))
    utils.make_smooth_reception_table(start_date = r,
                                      reception_measured_table = sat_reception_measured_all_speeds_tbl,
                                      destination_dataset = destination_dataset,
                                      destination_table = sat_reception_smoothed_all_speeds_tbl)

reception_dates[8:]

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

# ## Reception quality excluding disabling events
#
# Calculate reception quality only after excluding suspected disabling events.

# +
# Create tables for alternate speed reception quality
sat_reception_measured_no_disabling_tbl = 'sat_reception_measured_one_degree_no_disabling_{}'.format(output_version)
sat_reception_smoothed_no_disabling_tbl = 'sat_reception_smoothed_one_degree_no_disabling_{}'.format(output_version)

if create_tables:
    # measured reception quality
    utils.make_bq_partitioned_table(destination_dataset, sat_reception_measured_no_disabling_tbl)
    utils.make_bq_partitioned_table(destination_dataset, sat_reception_smoothed_no_disabling_tbl)
# -

# Generate commands
mr_no_disable_cmds = []
for r in reception_dates:
#     print(str(r.date()))
    cmd = utils.make_reception_measured_table(destination_table = sat_reception_measured_no_disabling_tbl, 
                                        destination_dataset = destination_dataset,
                                        start_date = r, 
                                        vi_version = vi_version, 
                                        segs_table="{}.{}".format("gfw_research", segs_table),
                                        output_version = output_version,
                                        include_all_speeds = "False",
                                        exclude_disabling = "True")

    mr_no_disable_cmds.append(cmd)

# test query
test_cmd = mr_no_disable_cmds[0].split('|')[0]
os.system(test_cmd)

# Run commands
utils.execute_commands_in_parallel(mr_no_disable_cmds)

# Smooth reception quality that excludes gaps.

for r in reception_dates:
    print(str(r.date()))
    utils.make_smooth_reception_table(start_date = r,
                                      reception_measured_table = sat_reception_measured_no_disabling_tbl,
                                      destination_dataset = destination_dataset,
                                      destination_table = sat_reception_smoothed_no_disabling_tbl)

# # Time lost to gaps
#
# Estimate the time lost to suspected disabling
#
# ### Interpolate positions during AIS gap events

# Table for interpolated gap positions
gap_positions_hourly_table = "gap_positions_hourly_{}".format(output_version)

try:
    client.get_table("{d}.{t}".format(d = destination_dataset, t = gap_positions_hourly_table))
    print("Table {d}.{t} already exists".format(d = destination_dataset, t = gap_positions_hourly_table))
except NotFound:
    print("Table {d}.{t} is not found.".format(d = destination_dataset, t = gap_positions_hourly_table))
    print("Creating table {d}.{t}".format(d = destination_dataset, t = gap_positions_hourly_table))
    # create interpolated gap event positions table if needed
    utils.make_bq_partitioned_table(destination_dataset, gap_positions_hourly_table)

# Store commands
gap_int_cmds = []
for t in tp:
    cmd = utils.make_hourly_gap_interpolation_table(date = t,
                                                    output_version = output_version,
                                                    destination_dataset = destination_dataset,
                                                    destination_table = gap_positions_hourly_table)
    gap_int_cmds.append(cmd)

# test query
test_cmd = gap_int_cmds[0].split('|')[0]
os.system(test_cmd)

# Run commands
utils.execute_commands_in_parallel(gap_int_cmds)



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
    ais_positions_hourly,
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

# # Download data
#
# Download the following datasets for us in the model to identify drivers of suspected disabling:
# + Gap events with features
# + Loitering events
# + Gridded fishing

# Create results folder 
results_dir = "../results"
# os.mkdir(results_dir)
# Create folder for specific results version
results_version_dir = os.path.join(results_dir, "gap_inputs_{}".format(output_version))
print(results_version_dir)
os.mkdir(results_version_dir)

# %%bigquery gap_events_features_df
SELECT * 
FROM `proj_ais_gaps_catena.ais_gap_events_features_v20220606` 
WHERE gap_hours >= 12
AND gap_start < '2020-01-01'
AND gap_end < '2020-01-01'

gap_events_features_df.to_csv(f'{results_version_dir}/gap_events_features_{output_version}.zip', 
                              index = False,
                              compression = dict(method='zip', archive_name=f'gap_events_features_{output_version}.zip')
                             )

# Download loitering events:

# %%bigquery loitering_events_df
SELECT *
FROM proj_ais_gaps_catena.loitering_events_v20220301

loitering_events_df.to_csv('{d}/loitering_events_{v}.csv'.format(d = results_version_dir,
                                                                 v = output_version), index = False)

# Gridded loitering

# %%bigquery gridded_loitering_df
SELECT *
FROM proj_ais_gaps_catena.gridded_loitering_v20220301

gridded_loitering_df.to_csv('{d}/loitering_quarter_degree_{v}_2017_to_2020.csv'.format(d = results_version_dir,
                                                                                       v = output_version), index = False)

# Download gridded fishing:

# %%bigquery gridded_fishing_df
SELECT *
FROM proj_ais_gaps_catena.gridded_fishing_brt_v20220606

gridded_fishing_df.to_csv(f'{results_version_dir}/vessel_presence_quarter_degree_{output_version}_2017_to_2019.zip', 
                          index = False,
                          compression = dict(method='zip', archive_name=f'vessel_presence_quarter_degree_{output_version}_2017_to_2019.csv')
                         )


