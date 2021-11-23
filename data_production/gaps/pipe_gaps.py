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

# # AIS Gaps Pipeline
#
# **Last updated:** 2020-11-14
#
# ## Overview
#
# This notebook executes the following series of three queries to produce an events table of 6+ hour gaps in AIS signal:
#
# + `ais_off_events.sql.j2`: This query identifies the start of gaps in AIS signal longer than a certain number of hours. Results are saved to the date-partitioned table `gfw_research_precursors.ais_off_events_vYYYYMMDD`
#
#
# + `ais_on_events.sql.j2`: This query identifies the end of the gaps in AIS signal identified by the `ais_off_events.sql.j2` query. Results are saved to the table `gfw_research_precursors.ais_on_events_vYYYYMMDD`
#
#
# + `ais_gap_events.sql`: This query stitches together the AIS off and on events identified by `ais_off_events.sql.j2` and `ais_on_events.sql.j2`, respectively, and saves/updates results in the table `gfw_research.ais_gap_events_vYYYYMMDD`.

# ## Table Definitions
#
# Define the names of the BigQuery datasets and tables to create and/or query
#
# TODO: Properly version the tables (currently hardcoded below)

# +
# Datasets
## Development
gfw_research = 'scratch_jenn'
gfw_research_precursors = 'scratch_jenn'
destination_dataset = 'scratch_jenn'

# Deployment
# gfw_research = 'gfw_research'
# gfw_research_precursors = 'gfw_research_precursors'
# destination_dataset = 'gfw_research'

# Tables
pipeline_table = 'pipe_v20190502'
segs_table = 'pipe_v20190502_segs'
off_events_table = 'ais_off_events_v20201124'
on_events_table = 'ais_on_events_v20201124'
gap_events_table = 'ais_gap_events_v20201124'
# -

# ## Setup
#
# The first step in the AIS gaps pipeline is to determine the date range over which to identify AIS gaps. This includes dates that have already been processed by the pipeline but which have not been added to `gfw_research_precursors.ais_off_events_vYYYYMMDD`. 

# +
# Modules
import os
import pandas_gbq
from datetime import datetime
from google.cloud import bigquery
from jinja2 import Template

# BigQuery client
# client = bigquery.Client()
# -

# ### Functions
#
# Write function to find already processed dates.

def already_processed(dataset = "gfw_research", table = "pipe_v20190502", partition = 'date'):
    
    '''
    this checks the tables present in a dataset and returns an empty list if
    '''  
    q = '''SELECT * 
    FROM {dataset}.__TABLES__
    WHERE table_id = '{table}'
    '''.format(table=table, dataset=dataset)
    df = pandas_gbq.read_gbq(q, project_id="world-fishing-827")
    if len(df)==0:
        return []
    
    '''
    this returns a list of strings, in format YYYY-MM-DD, that already exist as 
    date in a given dataset and table
    '''    
    q = '''
    SELECT DISTINCT
    {partition} as date
    FROM {dataset}.{table}
    GROUP BY date 
    ORDER BY date
    '''.format(table=table, dataset=dataset,partition=partition)
    df = pandas_gbq.read_gbq(q, project_id="world-fishing-827")
    dt = list(df.date)    
    ap = map(lambda x: x.strftime("%Y-%m-%d"), dt)
    return list(ap)


def execute_commands_in_parallel(commands):
    '''This takes a list of commands and runs them in parallel
    Note that this assumes you can run 16 commands in parallel,
    your mileage may vary if your computer is old and slow.
    Requires having gnu parallel installed on your machine.
    '''
    with open('commands.txt', 'w') as f:
        print(commands)
        f.write("\n".join(commands))    
    
    os.system("parallel -j 16 < commands.txt")
    os.system("rm -f commands.txt")


# Find the already processed dates in `gfw_research.pipe_v20190502` and `gfw_research_precursors.ais_off_events`

# Get list of already processed pipeline dates
ap_pipe = already_processed(dataset = "gfw_research", table = pipeline_table, partition='date')
# print(len(ap_pipe))

# +
# Get list of off and on event dates processed by gaps pipeline
ap_off = already_processed(dataset = gfw_research, table = off_events_table, partition='_partitiontime')
print(len(ap_off))

ap_on = already_processed(dataset = gfw_research, table = on_events_table, partition='_partitiontime')
print(len(ap_on))
# -

# If the gap off events table doesn't exist, create the table.

# +
if(len(ap_off)==0):
    # if the length of ap_off is zero, that means the off events table doesn't have any data in it, so we must create it.
    cmd = "bq mk --time_partitioning_type=DAY {}.{}".format(gfw_research_precursors, off_events_table)
    os.system(cmd)

if(len(ap_on)==0):
    # if the length of ap_on is zero, that means the on events table doesn't have any data in it, so we must create it.
    cmd = "bq mk --time_partitioning_type=DAY {}.{}".format(gfw_research_precursors, on_events_table)
    os.system(cmd)
# -

# Get all dates to process by finding dates in `ap_pipe` not in `ap_gaps`.

# Get all values to process
# NOTE -- we don't' want to process the last day
# bedause we dan't calculate off events without a following day
tp = [t for t in ap_pipe[:-1] if t not in ap_gaps]

# +
### Used for development to run discrete time periods

# from datetime import timedelta, date

# def daterange(date1, date2):
#     for n in range(int ((date2 - date1).days)+1):
#         yield date1 + timedelta(n)

# start_dt = date(2012, 1, 1)
# end_dt = date(2012, 1, 31)
# tp = []
# for dt in daterange(start_dt, end_dt):
#     tp.append(dt.strftime("%Y-%m-%d"))
# -

# Test function on first date
if len(tp) <= 31:
    for t in tp:
        print(str(t))
else:
    print("Start:", tp[0])
    print("End:", tp[-1])
    print("Number of days:", len(tp))

# Format the off events query to validate and/or edit.

# +
# Open ais_off_on_events.sql.j2 file
with open('ais_off_on_events.sql.j2') as f:
    sql_template = Template(f.read())

# Format the query according to the desired event type, date, and gap length
output_template = sql_template.render( pipeline_table="{}.{}".format("gfw_research", pipeline_table), segs_table="{}.{}".format("gfw_research", segs_table), event='off', date="2012-01-03", min_gap_length=6)

print(output_template)


# -

# Make the off events tables.

# Off/on events function
def make_ais_events_table(pipeline_table, segs_table, event_type, date, min_gap_hours, precursors_dataset, destination_table):
    
    # Format date string without dashes for partition
    dest_table_partition = destination_table + "\$"+ date.replace("-","")
    
    # Format jinja2 command
    cmd = """jinja2 ais_off_on_events.sql.j2 \
    -D pipeline_table="{pipeline_table}" \
    -D segs_table="{segs_table}" \
    -D event="{event_type}" \
    -D date="{date}" \
    -D min_gap_length={min_gap_hours} \
    | \
    bq query --replace \
    --destination_table={precursors_dataset}.{destination_table}\
    --allow_large_results --use_legacy_sql=false --max_rows=0
    """.format(pipeline_table=pipeline_table,
               segs_table=segs_table,
               event_type=event_type,
               date=date, 
               min_gap_hours=min_gap_hours,
               precursors_dataset=precursors_dataset,
               destination_table=dest_table_partition)
    return cmd


# ## AIS Off Events
#
# Generate all off event commands.

# Store commands
cmds = []
for t in tp:
    cmd = make_ais_events_table(pipeline_table="{}.{}".format("gfw_research", pipeline_table),
                                segs_table="{}.{}".format("gfw_research", segs_table),
                                event_type='off',
                                date = t,
                                min_gap_hours=6, 
                                precursors_dataset=gfw_research_precursors,
                                destination_table=off_events_table)
    cmds.append(cmd)

# Run all commands
# print(cmds[0])
execute_commands_in_parallel(commands=cmds)

# ## AIS On Events
#
# Generate all on event commands

# Store commands
cmds = []
for t in tp:
    cmd = make_ais_events_table(pipeline_table="{}.{}".format("gfw_research", pipeline_table),
                                segs_table="{}.{}".format("gfw_research", segs_table),
                                event_type='on',
                                date = t,
                                min_gap_hours=6, 
                                precursors_dataset=gfw_research_precursors,
                                destination_table=on_events_table)
    cmds.append(cmd)

# Run all commands
# print(cmds[0])
execute_commands_in_parallel(commands=cmds)

# ## AIS Gap Events
#
# Combine the tables of AIS off events and AIS on events into a single table of completed AIS gap events.

# +
# Open ais_off_on_events.sql.j2 file
with open('ais_gap_events.sql.j2') as f:
    sql_template = Template(f.read())

# Format the query according to the desired event type, date, and gap length
output_template = sql_template.render(off_events_table="{}.{}".format(gfw_research_precursors,off_events_table),
                                      on_events_table="{}.{}".format(gfw_research_precursors,on_events_table),
                                      date="2017-01-01")

print(output_template)
# -

# Check if gap events table exists in destination dataset.

# Check if gaps table exists in destination dataset
gaps_exist = already_processed(dataset = gfw_research, table = gap_events_table, partition = 'gap_start')
print(len(gaps_exist))

# Make empty table for gap events if the table does not already exist.

# Create gaps table if it does not exist
if(len(gaps_exist)==0):
    # if the length is zero, that means the table doesn't have any data in it.
    # create the table so that we can add to it in the next step. 
    cmd = "bq mk --schema=ais_gap_events.json --time_partitioning_field=gap_start --time_partitioning_type=DAY {}.{}".format(gfw_research, gap_events_table)
    os.system(cmd)


# Define function to overwrite data in the gap events table. Gap events are partitioned by the gap_start field.

# Off/on events function
def make_ais_gap_events_table(off_events_table,
                              on_events_table,
                              date, 
                              precursors_dataset,
                              destination_dataset, 
                              destination_table):
        
    # Format jinja2 command
    cmd = """jinja2 ais_gap_events.sql.j2 \
    -D off_events_table="{precursors_dataset}.{off_events_table}" \
    -D on_events_table="{precursors_dataset}.{on_events_table}" \
    -D date="{date}" \
    | \
    bq query --replace \
    --destination_table={destination_dataset}.{destination_table}\
    --allow_large_results --use_legacy_sql=false --max_rows=0
    """.format(off_events_table=off_events_table,
               on_events_table=on_events_table,
               date=date,
               precursors_dataset=precursors_dataset,
               destination_dataset=destination_dataset,
               destination_table=destination_table)
    return cmd


# Generate query to make replacement gaps table based on the latest date.

# Store commands
cmds = []
latest_date = tp[-1]
cmd = make_ais_gap_events_table(off_events_table = off_events_table,
                                on_events_table = on_events_table,
                                date = latest_date,
                                precursors_dataset = gfw_research_precursors,
                                destination_dataset = gfw_research,
                                destination_table = gap_events_table)
cmds.append(cmd)


# Run all commands to update gap events table.

# print(len(cmds))
execute_commands_in_parallel(commands=cmds)


