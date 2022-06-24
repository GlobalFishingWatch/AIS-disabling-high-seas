# run_time_lost_to_gaps.py

import os
import subprocess
from ais_disabling import utils
from ais_disabling import config
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from jinja2 import Template

#########################################################################
# TIME LOST TO GAPS
# Produce datasets used to estimate the time lost to suspected disabling
#########################################################################

# Top level configuration
output_version = config.output_version
destination_dataset = config.destination_dataset
tp = config.tp

# Tables
gap_positions_hourly_table = config.gap_positions_hourly_table
raster_gaps_table = config.raster_gaps_table

# Which steps to run
steps_to_run = ['normalize_gaps']

#########################################################################
# 1. Interpolate positions during AIS gap events
#########################################################################

if 'gap_interpolation' in steps_to_run:

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
# 2. Rasterize gaps
#########################################################################

if 'raster_gaps' in steps_to_run:

    with open('data_production/time_lost_to_gaps/RasterGaps.sql.j2') as f:
        template = Template(f.read())

    query = template.render(
        pipeline_dataset = config.pipeline_dataset,
        pipeline_table = config.pipeline_table,
        segs_table = config.segs_table,
        start_date = str(config.start_date),
        end_date = str(config.end_date),
        fishing_vessels_table = config.fishing_vessels_table,
        destination_dataset = config.destination_dataset,
        output_table = config.raster_gaps_table
        )

    if config.test_run:
        print(query)

    if config.test_run is False:
        subprocess.run("bq query".split(), input=bytes(query, "utf-8"))

#########################################################################
# 3. Normalize rasterized gaps
#########################################################################

if 'normalize_gaps' in steps_to_run:

    with open('data_production/time_lost_to_gaps/NormalizeRasterGaps.sql.j2') as f:
        template = Template(f.read())

    query = template.render(
        destination_dataset = config.destination_dataset,
        raster_gaps_table = config.raster_gaps_table,
        output_table = config.raster_gaps_norm_table
        )

    if config.test_run:
        print(query)

    if config.test_run is False:
        # print(query)
        subprocess.run("bq query".split(), input=bytes(query, "utf-8"))

#########################################################################
# 4. Spatially allocate gaps
#########################################################################

#
# Allocate gaps via raster method
#
if 'allocate_gaps_raster' in steps_to_run:

    with open('data_production/time_lost_to_gaps/AllocateGapsRaster.sql.j2') as f:
        template = Template(f.read())

    query = template.render(
        destination_dataset = config.destination_dataset,
        fishing_vessels_table = config.fishing_vessels_table,
        gap_events_features_table = config.gap_events_features_table,
        raster_gaps_norm_table = config.raster_gaps_norm_table,
        start_date = str(config.start_date),
        end_date = str(config.end_date),
        output_table = config.gaps_allocated_raster_table,
        scale = 1
        )

    if config.test_run:
        print(query)

    if config.test_run is False:
        # print(query)
        subprocess.run("bq query".split(), input=bytes(query, "utf-8"))

#
# Allocate gaps via interpolation method
#
if 'allocate_gaps_interpolate' in steps_to_run:

    with open('data_production/time_lost_to_gaps/AllocateGapsInterpolate.sql.j2') as f:
        template = Template(f.read())

    query = template.render(
        destination_dataset = config.destination_dataset,
        fishing_vessels_table = config.fishing_vessels_table,
        gap_events_features_table = config.gap_events_features_table,
        gap_positions_hourly_table = config.gap_positions_hourly_table,
        start_date = str(config.start_date),
        end_date = str(config.end_date),
        output_table = config.gaps_allocated_interpolate_table,
        scale = 1
        )

    if config.test_run:
        print(query)

    if config.test_run is False:
        # print(query)
        subprocess.run("bq query".split(), input=bytes(query, "utf-8"))

#########################################################################
# 5. Spatially allocate fishing activity
#########################################################################

if 'grid_fishing' in steps_to_run:

    with open('data_production/time_lost_to_gaps/GridFishing.sql.j2') as f:
        template = Template(f.read())

    query = template.render(
        pipeline_dataset = config.pipeline_dataset,
        pipeline_table = config.pipeline_table,
        segs_table = config.segs_table,
        fishing_vessels_table = config.fishing_vessels_table,
        gap_events_features_table = config.gap_events_features_table,
        start_date = str(config.start_date),
        end_date = str(config.end_date),
        destination_dataset = config.destination_dataset,
        output_table = config.fishing_allocated_table,
        scale = 1
        )

    if config.test_run:
        print(query)

    if config.test_run is False:
        # print(query)
        subprocess.run("bq query".split(), input=bytes(query, "utf-8"))
