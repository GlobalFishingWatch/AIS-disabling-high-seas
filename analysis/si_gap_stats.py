# si_gap_stats.py

######################################################################
# AIS disabling event summary statistics
# Calculate summary statistics about the AIS disabling events dataset.
######################################################################

import pandas as pd
import numpy as np

query = f"""
SELECT *
FROM `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20220606`
WHERE gap_hours >= 12
AND positions_per_day_off > 10
AND positions_12_hours_before_sat >= 14
AND off_distance_from_shore_m > 1852 * 50
AND DATE(gap_start) >= '2017-01-01'
AND DATE(gap_end) <= '2019-12-31'
"""

gap_df = pd.read_gbq(query, project_id="world-fishing-827")
gap_df.columns

# Calculate summary stats about the number of vessels, flag states, geartypes with disabling events.

n_gaps = gap_df['gap_id'].nunique()
print(f"{n_gaps} unique gap ids")

n_ssvid = gap_df['ssvid'].nunique()
print(f"{n_ssvid} unique MMSI")

n_flags = gap_df['flag'].nunique()
print(f"{n_flags} unique flag states")

median_gap_hours = gap_df['gap_hours'].median()
print(f"{median_gap_hours} median gap hours")

median_gap_distance_km = gap_df['gap_distance_m'].median()/1000
print(f"{median_gap_distance_km} median gap distance in km")
