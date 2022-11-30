# figs_disabling_case_study.py

######################################################################
# Pull data for vessel tracks and disabling events for
# disabling case study section
######################################################################

import pandas as pd
import numpy as np

from ais_disabling import config

######################################################################
# AIS tracks for the encounter between the Chitose (MMSI 563418000),
# TWN fishing vessel Full Li Hsiang (MMSI 416868000), and the
# fishing vessel's gear (MMSI 81561744)
######################################################################

chitose_query = f"""
SELECT
    ssvid,
    lat,
    lon,
    timestamp,
    nnet_score,
    CASE
     WHEN ssvid = '416868000' THEN 'fishing_vessel'
    # carrier ssvid
     WHEN ssvid = '563418000' THEN 'carrier_vessel'
    # gear ssvid
    WHEN ssvid = '81561744' THEN 'gear' END as ssvid_type
FROM `{config.pipeline_dataset}.{config.pipeline_table}`
WHERE ssvid IN(
    # fishing ssvid
    '416868000',
    # carrier ssvid
    '563418000',
    # gear ssvid
    '81561744'
    )
AND _partitiontime BETWEEN '2019-06-06' AND '2019-06-10'
"""

# Save to csv
chitose_df = pd.read_gbq(chitose_query, project_id="world-fishing-827")
chitose_df.to_csv(f'results/gap_inputs_{config.output_version}/fig_5_b_tracks.csv', index = False)

######################################################################
# Authorization data for the Chitose (MMSI 563418000) and
# TWN fishing vessel (MMSI 416868000) involved in above encounter
######################################################################

auth_query = f"""
SELECT *
FROM `world-fishing-827.vessel_identity.identity_authorization_v20220401`
WHERE ssvid IN ('416868000', '563418000')
AND source_code IN ('CCSBT','IOTC')
AND DATE(authorized_from) <= '{config.start_date}'
AND DATE(authorized_to) >= '{config.end_date}'
ORDER BY vessel_record_id, authorized_from
"""

# Save to csv
auth_df = pd.read_gbq(auth_query, project_id="world-fishing-827")
auth_df.to_csv(f'results/gap_inputs_{config.output_version}/fig_5_b_authorizations.csv', index = False)

######################################################################
# Disabling events for the Oyang 77 (440256000) in early 2019 when it was
# apprehended by Argentinian officals
######################################################################

oyang_77_query = f"""
SELECT
  gap_id,
  ssvid,
  vessel_class,
  flag,
  gap_start,
  gap_end
FROM `{config.destination_dataset}.{config.gap_events_features_table}`
{config.gap_filters}
AND ssvid = '440256000'
AND (DATE(gap_start) >= '2019-01-01' AND DATE(gap_end) <= '2019-02-10')
ORDER BY gap_start
"""

# Save to csv
oyang_77_df = pd.read_gbq(oyang_77_query, project_id='world-fishing-827')
oyang_77_df.to_csv(f'results/gap_inputs_{config.output_version}/fig_5_a_gaps.csv', index = False)

######################################################################
# Track for the Oyang 77 (440256000) in early 2019 when it was
# apprehended by Argentinian officals
######################################################################

oyang_77_track_query = f"""
SELECT
  ssvid,
  timestamp,
  lat,
  lon
FROM `pipe_production_v20201001.research_messages`
WHERE _partitiontime BETWEEN '2019-01-01' AND '2019-02-10'
AND ssvid = '440256000'
ORDER BY timestamp
"""

# Save to csv
oyang_77_track_df = pd.read_gbq(oyang_77_track_query, project_id='world-fishing-827')
oyang_77_track_df.to_csv(f'results/gap_inputs_{config.output_version}/fig_5_a_track.csv', index = False)
