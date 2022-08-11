#download_disabling_events.py

import pandas as pd
from ais_disabling import config

###########################
# Query to download events
###########################
query = f"""
SELECT
  gap_id,
  ssvid as mmsi,
  vessel_class,
  flag,
  vessel_length_m,
  vessel_tonnage_gt,
  gap_start as gap_start_timestamp,
  off_lat as gap_start_lat,
  off_lon as gap_start_lon,
  off_distance_from_shore_m as gap_start_distance_from_shore_m,
  gap_end as gap_end_timestamp,
  on_lat as gap_end_lat,
  on_lon as gap_end_lon,
  on_distance_from_shore_m as gap_end_distance_from_shore_m,
  gap_hours
FROM `{config.destination_dataset}.{config.gap_events_features_table}`
{config.gap_filters}
"""

# Download data
df = pd.read_gbq(query, project_id='world-fishing-827')

# Save to csv
df.to_csv(
    f'data/disabling_events.zip',
    index = False,
    compression = dict(method='zip', archive_name=f'disabling_events.csv')
    )
