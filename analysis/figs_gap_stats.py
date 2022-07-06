# figs_gap_stats.property

import pandas as pd
import matplotlib.pyplot as plt
from ais_disabling import utils
from ais_disabling import config

###########################################################
# Query stats about gap duration and distance
###########################################################

gap_stats_query = f'''
SELECT
  ssvid,
  vessel_class,
  off_class,
  gap_hours,
  gap_distance_m / 1000 as gap_distance_km,
  IF (
   positions_12_hours_before_sat >= 14
   AND (positions_per_day_off > 10), True,  False
  ) as likely_disabling
FROM `{config.destination_dataset}.{config.gap_events_features_table}`
WHERE gap_hours >= 12
AND (off_distance_from_shore_m > 1852*50)
AND (
    DATE(gap_start) >= "{config.start_date}"
    AND DATE(gap_end) <= "{config.end_date}"
    )
'''

gap_length = pd.read_gbq(gap_stats_query, project_id = 'world-fishing-827')

###########################################################
# Plot histograms of gaps statistics
###########################################################

ax_range = 24*30
bins = 60

fig, ((ax1, ax3), (ax2, ax4)) = plt.subplots(2,2, sharey=False, sharex=False, figsize = (10,8))

ax1.hist(gap_length['gap_hours'],
         range=(0, ax_range),
         bins=bins)

ax1.title.set_text('All AIS gap events')
ax1.set(ylabel = 'Count')

ax2.hist(gap_length.loc[gap_length['likely_disabling'] == True, 'gap_hours'],
         range=(0, ax_range),
         bins=bins)

ax2.title.set_text('Suspected disabling events')
ax2.set(xlabel = 'Duration (hours) of AIS gap\n(bin width = 12)')

# set axes range
ax_range = 1500
bins = 30

ax3.hist(gap_length['gap_distance_km'],
         range=(0, ax_range),
         bins=bins)

ax3.title.set_text('All AIS gap events')
ax3.set(ylabel = 'Count')

ax4.hist(gap_length.loc[gap_length['likely_disabling'] == True, 'gap_distance_km'],
         range=(0, ax_range),
         bins=bins)

ax4.title.set_text('Suspected disabling events')
ax4.set(xlabel = 'Distance travelled (km) during AIS gap\n(bin width = 50)')

plt.savefig(f'results/gap_figures_{config.output_version}/figure_si_gap_duration_dist_hists.png', dpi=200, facecolor=plt.rcParams['pyseas.fig.background'])
