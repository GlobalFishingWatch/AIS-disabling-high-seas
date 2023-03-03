# config_ole.py
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
vi_version = 'v20230101'
vd_version = 'v20230101'

# Output tables version
output_version = 'v20230302'
create_tables = True

# Date range of analysis
start_date = date(2017, 1, 1)
end_date = date(2022, 12, 31)

# Tables to run
tables_to_run = ['fishing']

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
# Destination tables
###############################################

# Vessels
fishing_vessels_table = f'ole_fishing_vessels_{output_version}'

# Gap events
gap_events_features_table = f'ole_ais_gap_events_features_{output_version}'

# Fishing vessel activity
fishing_table = f"ole_fishing_daily_{output_version}"