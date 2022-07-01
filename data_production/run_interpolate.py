# run_interpolate.py

import os
import subprocess
from ais_disabling import utils
from ais_disabling import config
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from jinja2 import Template

#########################################################################
# AIS INTERPOLATION
# Produce various datasets of interpolated AIS positions
#########################################################################

# Which steps to run
steps_to_run = ['loitering']

#########################################################################
# 1. Interpolate positions during AIS gap events
#########################################################################

if 'gaps' in steps_to_run:

    # Store commands
    gap_int_cmds = []
    for t in tp:
        cmd = utils.make_hourly_gap_interpolation_table(date = t,
                                                        output_version = output_version,
                                                        destination_dataset = destination_dataset,
                                                        destination_table = gap_positions_hourly_table)
        gap_int_cmds.append(cmd)

    # test query
    if config.test_run:
        test_cmd = gap_int_cmds[0].split('|')[0]
        print(test_cmd)
        os.system(test_cmd)

    if config.test_run is False:
        # Create tables if necessary
        try:
            config.client.get_table("{d}.{t}".format(d = destination_dataset, t = gap_positions_hourly_table))
            print("Table {d}.{t} already exists".format(d = destination_dataset, t = gap_positions_hourly_table))
        except NotFound:
            print("Table {d}.{t} is not found.".format(d = destination_dataset, t = gap_positions_hourly_table))
            print("Creating table {d}.{t}".format(d = destination_dataset, t = gap_positions_hourly_table))
            # create interpolated gap event positions table if needed
            utils.make_bq_partitioned_table(destination_dataset, gap_positions_hourly_table)

        # Run commands
        utils.execute_commands_in_parallel(gap_int_cmds)

#########################################################################
# 2. Interpolate AIS positions
#########################################################################

if 'ais' in steps_to_run:
    print("Running AIS interpolation")
    # Store commands
    ais_int_cmds = []
    for t in config.tp:
        cmd = utils.make_hourly_interpolation_table(
            date = t,
            pipeline_dataset = config.pipeline_dataset,
            pipeline_table = config.pipeline_table,
            destination_dataset = config.destination_dataset,
            destination_table = config.ais_positions_hourly
            )

        ais_int_cmds.append(cmd)

    # test query
    if config.test_run:
        test_cmd = ais_int_cmds[0].split('|')[0]
        print(test_cmd)
        os.system(test_cmd)

    if config.test_run is False:
        # Create tables if necessary
        try:
            utils.client.get_table(f"{config.destination_dataset}.{config.ais_positions_hourly}")
            print(f"Table {config.destination_dataset}.{config.ais_positions_hourly} already exists")
        except NotFound:
            print(f"Table {config.destination_dataset}.{config.ais_positions_hourly} is not found.")
            print(f"Creating table {config.destination_dataset}.{config.ais_positions_hourly}")
            # create interpolated gap event positions table if needed
            utils.make_bq_partitioned_table(config.destination_dataset, config.ais_positions_hourly)

        # Run commands
        utils.execute_commands_in_parallel(ais_int_cmds)

#########################################################################
# 2. Interpolate loitering positions
#########################################################################

if 'loitering' in steps_to_run:
    print("Running AIS interpolation")
    # Store commands
    loit_int_cmds = []
    for t in config.tp:
        cmd = utils.make_hourly_loitering_interpolation_table(
            date = t,
            destination_dataset = config.destination_dataset,
            destination_table = config.loitering_positions_hourly_table,
            output_version = config.output_version
            )

        loit_int_cmds.append(cmd)

    # test query
    if config.test_run:
        test_cmd = loit_int_cmds[0].split('|')[0]
        print(test_cmd)
        os.system(test_cmd)

    if config.test_run is False:
        # Create tables if necessary
        try:
            utils.client.get_table(f"{config.destination_dataset}.{config.loitering_positions_hourly_table}")
            print(f"Table {config.destination_dataset}.{config.loitering_positions_hourly_table} already exists")
        except NotFound:
            print(f"Table {config.destination_dataset}.{config.loitering_positions_hourly_table} is not found.")
            print(f"Creating table {config.destination_dataset}.{config.loitering_positions_hourly_table}")
            # create interpolated gap event positions table if needed
            utils.make_bq_partitioned_table(config.destination_dataset, config.loitering_positions_hourly_table)

        # Run commands
        utils.execute_commands_in_parallel(loit_int_cmds)
