# ---
# jupyter:
#   jupytext:
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

# # AIS disabling event summary statistics
#
# Calculate summary statistics about the AIS disabling events dataset.

# +
import pandas as pd
import numpy as np

# %load_ext google.cloud.bigquery

# +
# %%bigquery gap_df

SELECT 
*
FROM `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
WHERE gap_hours >= 12
AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
AND positions_X_hours_before_sat >= 19
AND off_distance_from_shore_m > 1852 * 50
AND on_distance_from_shore_m > 1852 * 50
AND DATE(gap_start) >= '2017-01-01' 
AND DATE(gap_end) <= '2019-12-31'
# -

gap_df.columns

# Calculate summary stats about the number of vessels, flag states, geartypes with disabling events.

gap_df['gap_id'].nunique()

n_ssvid = gap_df['ssvid'].nunique()
n_ssvid

n_flags = gap_df['flag'].nunique()
n_flags

gap_df['gap_hours'].median()

gap_df['gap_distance_m'].median()/1000


