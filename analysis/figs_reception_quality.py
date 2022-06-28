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

# # Reception Quality Figures

# +
import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import colors,colorbar, cm

import pyseas
import pyseas.maps
import pyseas.maps.rasters
import pyseas.styles
import pyseas.cm
import cmocean

from ais_disabling import utils
from ais_disabling import config

# %load_ext autoreload
# %autoreload 2
# -

# Parameters:

# +
# Parameters
destination_dataset = config.destination_dataset
output_version = config.output_version

# Reception quality tables
sat_reception_smoothed_tbl = config.sat_reception_smoothed
sat_reception_measured_tbl = config.sat_reception_measured

start_date = str(config.start_date)
end_date = str(config.end_date)
# -


# ## Smoothed reception quality

# Query smooth reception quality
sat_reception_smooth_query = f'''
SELECT
    lat_bin,
    lon_bin,
    class,
    AVG(positions_per_day) as positions_per_day
FROM `{destination_dataset}.{sat_reception_smoothed_tbl}`
WHERE DATE(_partitiontime) BETWEEN "{start_date}"
AND "{end_date}"
GROUP BY 1,2,3
'''
# Query data
sat_reception_smooth = pd.read_gbq(sat_reception_smooth_query, project_id='world-fishing-827', dialect='standard')

# +
# Plot reception quality with same color scales
utils.plot_reception_quality(
    reception_df = sat_reception_smooth,
    fig_min_value = 1,
    fig_max_value = 100
    )

# Save figure
plt.savefig(f"results/gap_figures_{output_version}/figure_si_smooth_reception.png",dpi=200, bbox_inches='tight')
# -

# ## Measured reception quality

sat_reception_measured_query = f'''
SELECT
    lat_bin,
    lon_bin,
    cls as class,
    AVG(sat_pos_per_day) as positions_per_day
FROM `{destination_dataset}.{sat_reception_measured_tbl}`
WHERE DATE(_partitiontime) BETWEEN "{start_date}"
AND "{end_date}"
GROUP BY 1,2,3
'''
# Query data
sat_reception_measured = pd.read_gbq(sat_reception_measured_query, project_id='world-fishing-827', dialect='standard')

# +
# Plot reception quality with same color scales
utils.plot_reception_quality(
    reception_df = sat_reception_measured,
    contours = False,
    mode = 'measured',
    fig_min_value = 1,
    fig_max_value = 100
    )

# Save figure
plt.savefig(f"results/gap_figures_{output_version}/figure_si_measured_reception.png",dpi=200, bbox_inches='tight')
# -

# ## Residuals

# +
# Query reception quality residuals
residuals_query = f'''
WITH
--
-- Calculate monthly residuals per grid cell
--
residuals AS (
  SELECT 
  a.lat_bin,
  a.lon_bin,
  a.cls as class,
  a.sat_pos_per_day as pos_per_day_measured,
  b.positions_per_day as pos_per_day_smooth,
  a.sat_pos_per_day - b.positions_per_day as residual,
  IF(a.sat_pos_per_day > 100, 100, sat_pos_per_day) - IF(b.positions_per_day > 100, 100, b.positions_per_day) as residual_cap
  FROM `{destination_dataset}.{sat_reception_measured_tbl}` a
  LEFT JOIN `{destination_dataset}.{sat_reception_smoothed_tbl}` b
  ON (
    a.lat_bin = b.lat_bin
    AND a.lon_bin = b.lon_bin
    AND a.cls = b.class
    AND a._partitiontime = b._partitiontime
    )
  WHERE a._partitiontime BETWEEN "{start_date}" AND "{end_date}"
  AND hours > 24
)
--
-- Return average residuals by grid cell
SELECT 
  lat_bin,
  lon_bin,
  class,
  AVG(pos_per_day_measured) - AVG(pos_per_day_smooth) as residual_old,
  AVG(residual) as residual,
  AVG(residual_cap) as residual_cap
FROM residuals
GROUP BY 1,2,3
'''

# Query data 
residuals_df = pd.read_gbq(residuals_query, project_id = 'world-fishing-827')
# -

# Plot residuals
utils.plot_reception_residuals(df = residuals_df)
plt.savefig(f"../results/gap_figures_{output_version}/figure_si_reception_residuals.png",dpi=200, bbox_inches='tight')

# ### Residual histogram

utils.plot_residual_histogram(residuals_df)
plt.savefig(f"../results/gap_figures_{output_version}/figure_si_reception_residual_histogram.png",dpi=200, facecolor=plt.rcParams['pyseas.fig.background'])
