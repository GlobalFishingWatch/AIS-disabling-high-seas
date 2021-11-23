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

# # Generate Likely Hourly Positions
# October 27, 2020
# David Kroodsma
#
# This table generates the most likely position for a vessel for each half hour and the number of satellite and terrestrial positions for the given hour.
#
# Updated on December 8th to include new gaps table from 12/01 (Tyler)

import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


# +
# Version of model inputs to use in input/output table names
input_version = "20201209"

# Generate the list of dates that we want to run our updates on
startdate = "2017-01-01"
enddate = "2019-12-31"

d = datetime.strptime(startdate,"%Y-%m-%d")
endtime = datetime.strptime(enddate,"%Y-%m-%d")
tp = []
while d <= endtime:
    tp.append(d.strftime("%Y%m%d"))
    d = d + timedelta(days=1)


# -

def execute_commands_in_parallel(commands):
    '''This takes a list of commands and runs them in parallel
    Note that this assumes you can run 16 commands in parallel,
    your mileage may vary if your computer is old and slow.
    Requires having gnu parallel installed on your machine.
    '''
    with open('commands.txt', 'w') as f:
        f.write("\n".join(commands))    
    os.system("parallel -j 16 < commands.txt")
    os.system("rm -f commands.txt")


# # All Vessel Position Interpolation

destination_table = 'proj_ais_gaps_catena.ais_positions_byssvid_hourly_v20201027'
# this is to be run the first time
# os.system("bq mk --time_partitioning_type=DAY "+destination_table)

commands = []
for t in tp:
    destination_table2 = destination_table + "\$"+t
    command = '''jinja2 hourly_interpoloation_v20201027.sql.j2    \
       -D YYYY_MM_DD="{YYYY_MM_DD}" \
       | \
        bq query --replace \
        --destination_table={destination_table2}\
         --allow_large_results --use_legacy_sql=false '''.format(YYYY_MM_DD = t[:4] + "-" + t[4:6] + "-" + t[6:8],
                                destination_table2=destination_table2)                                 
    commands.append(command)


execute_commands_in_parallel(commands)

# Uncomment to copy version to new name
# !bq cp proj_ais_gaps_catena.ais_positions_byssvid_hourly_v20201027 proj_ais_gaps_catena.ais_positions_byssvid_hourly_v20201209

# # Fishing Vessel Position Interpolation

destination_table = 'proj_ais_gaps_catena.ais_positions_byssvid_hourlyfishing_v20201027'
# uncomment if need to delete and do over...
# os.system("bq rm -f  "+destination_table)

# +

os.system("bq mk --time_partitioning_type=DAY "+destination_table)
# -

commands = []
for t in tp:
    destination_table2 = destination_table + "\$"+t
    command = '''jinja2 hourly_fishing_interpoloation_v20201027.sql.j2    \
       -D YYYY_MM_DD="{YYYY_MM_DD}" \
       | \
        bq query --replace \
        --destination_table={destination_table2}\
         --allow_large_results --use_legacy_sql=false --max_rows=0 '''.format(YYYY_MM_DD = t[:4] + "-" + t[4:6] + "-" + t[6:8],
                                destination_table2=destination_table2)                                 
    commands.append(command)


# +
# print(commands[-1])

# +
## Uncomment to test...
# os.system(commands[0])
# -

execute_commands_in_parallel(commands)

# Uncomment to copy version to new name
# !bq cp proj_ais_gaps_catena.ais_positions_byssvid_hourlyfishing_v20201027 proj_ais_gaps_catena.ais_positions_byssvid_hourlyfishing_v20201209

# # Gap Interpolation

destination_table = 'proj_ais_gaps_catena.gap_positions_hourly_v{}'.format(input_version)
# uncomment if need to delete and do over...
# os.system("bq rm -f  "+destination_table)
os.system("bq mk --time_partitioning_type=DAY "+destination_table)

commands = []
for t in tp:
    destination_table2 = destination_table + "\$"+t
    command = '''jinja2 hourly_gap_interpoloation_v20201031.sql.j2    \
       -D YYYY_MM_DD="{YYYY_MM_DD}" \
       -D input_version="{input_version}" \
       | \
        bq query --replace \
        --destination_table={destination_table2}\
         --allow_large_results --use_legacy_sql=false --max_rows=0'''.format(YYYY_MM_DD = t[:4] + "-" + t[4:6] + "-" + t[6:8],
                                                                             input_version = input_version,
                                                                             destination_table2=destination_table2)                                 
    commands.append(command)


# +
# Uncomment to test...
# commands[0]
# os.system(commands[0])
# -

execute_commands_in_parallel(commands)

# # Loitering Interpolation

destination_table = 'proj_ais_gaps_catena.loitering_positions_byssvid_hourly_v{}'.format(input_version)
# uncomment if need to delete and do over...
# os.system("bq rm -f  "+destination_table)
os.system("bq mk --time_partitioning_type=DAY "+destination_table)

commands = []
for t in tp:
    destination_table2 = destination_table + "\$"+t
    command = '''jinja2 hourly_loitering_interpoloation_v20201027.sql.j2    \
       -D YYYY_MM_DD="{YYYY_MM_DD}" \
       -D input_version="{input_version}" \
       | \
        bq query --replace \
        --destination_table={destination_table2}\
         --allow_large_results --use_legacy_sql=false --max_rows=0'''.format(YYYY_MM_DD = t[:4] + "-" + t[4:6] + "-" + t[6:8],
                                                                             input_version = input_version,
                                                                             destination_table2=destination_table2)                                 
    commands.append(command)


# Uncomment to test...
# commands[0]
os.system(commands[0])

execute_commands_in_parallel(commands)


