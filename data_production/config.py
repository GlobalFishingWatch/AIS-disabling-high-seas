# config.py
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date

###############################################
# Inputs & Parameters
###############################################

# Test run queries?
test_run = False

# BQ datasets/tables
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

# Date range of analysis
start_date = date(2017,1, 1)
end_date = date(2019,12, 31)

# Min gap hours in raw gaps table
min_gap_hours = 6

###############################################
# Dates to run
###############################################

# Create set of dates
def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

# Generate list of dates to run
dates_to_run = daterange(start_date, end_date)
tp = []
for dt in dates_to_run:
    tp.append(dt.strftime("%Y-%m-%d"))

# Dates for monthly reception quality
reception_dates = pd.date_range(start_date, end_date, freq='1M') - pd.offsets.MonthBegin(1)

###############################################
# Suspected disabling event filters
###############################################

gap_filters = f"""
WHERE gap_hours >= 12
AND (DATE(gap_start) >= '{tp[0]}' AND DATE(gap_end) <= '{tp[-1]}')
AND off_distance_from_shore_m > 1852*50
AND positions_per_day_off > 10
AND positions_12_hours_before_sat >= 14
"""

###############################################
# Destination tables
###############################################

# Vessels
fishing_vessels_table = f'fishing_vessels_{output_version}'

# Gap events
off_events_table = f'ais_off_events_{output_version}'
on_events_table = f'ais_on_events_{output_version}'
gap_events_table = f'ais_gap_events_{output_version}'
gap_events_features_table = f'ais_gap_events_features_{output_version}'

# Loitering
loitering_events_table = f'loitering_events_{output_version}'
gridded_loitering_table = f'gridded_loitering_{output_version}'

# Fishing vessel activity
fishing_table = f"gridded_fishing_{output_version}"
fishing_brt_table = f"gridded_fishing_brt_{output_version}"

# Interpolated position tables
ais_positions_hourly = f'ais_positions_byseg_hourly_{output_version}'
loitering_positions_hourly = f'loitering_positions_byseg_hourly_{output_version}'
gap_positions_hourly_table = f"gap_positions_hourly_{output_version}"

# Reception quality tables
sat_reception_measured = f'sat_reception_measured_one_degree_{output_version}'
sat_reception_smoothed = f'sat_reception_smoothed_one_degree_{output_version}'

# Time lost to gaps tables
raster_gaps_table = f'raster_gaps_{output_version}'
raster_gaps_norm_table = f'raster_gaps_norm_{output_version}'
gaps_allocated_raster_table = f'gaps_allocated_raster_{output_version}'
gaps_allocated_interpolate_table = f'gaps_allocated_interpolate_{output_version}'
fishing_allocated_table = f'fishing_activity_{output_version}'

###############################################
# Coordinate defaults for plotting functions
###############################################

# Min/Max coordinates
min_lon, min_lat, max_lon, max_lat  = -180, -90, 180, 90

# Number of lat/lon bins
inverse_delta_degrees = 1
n_lat = (max_lat - min_lat) * inverse_delta_degrees
n_lon = (max_lon - min_lon) * inverse_delta_degrees

lons = np.arange(min_lon, max_lon+1)
lats = np.arange(min_lat, max_lat+1)
