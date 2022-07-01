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

chitose_df = pd.read_gbq(chitose_query, project_id="world-fishing-827")

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

auth_df = pd.read_gbq(auth_query, project_id="world-fishing-827")

######################################################################
# Disabling events for the Oyang 77 in early 2019 when it was
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
ORDER BY gap_start
"""

oyang_77_df = pd.read_gbq(oyang_77_query, project_id='world-fishing-827')
