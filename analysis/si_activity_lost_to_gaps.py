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

# # Activity Lost to Gaps 
#
# This notebook contains code that estimates time lost to AIS disabling and produces associated figures for the supplementary materials of Welch et al.

# +
import pyseas
import pyseas.maps
import pyseas.maps.rasters
import pyseas.styles
import pyseas.cm
import pandas as pd

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors,colorbar
import cartopy
from cartopy import config
import cartopy.crs as ccrs
import cmocean
# from tabulate import tabulate

# %matplotlib inline

def gbq(q): 
    return pd.read_gbq(q, project_id="world-fishing-827")


# -

# Set top-level parameters for analysis.

# +
# What scale (degrees) for the analysis of time lost to disabling
scale = 1

# Vessel info version to use
vi_version = '20210301' 

# Version of the gap model inputs to use. This should be the same for the gaps,
# vessel presence, loitering, and interpolated tables
inputs_version = '20210722' 
# -

# # Gap Activity

# +
q = f'''
#standardSQL

with 

# Fishing vessels
fishing_vessels AS (
SELECT * 
FROM `gfw_research.fishing_vessels_ssvid_v{vi_version}`
),
# Best vessel class
vessel_info AS (
SELECT
  ssvid,
  year,
  best.best_flag as flag,
  # TEMPORARY
  # Overwrite pole and line vessels as squid when registries say their squid
  IF(best.registry_net_disagreement = 'squid_jigger,pole_and_line',
     'squid_jigger', best.best_vessel_class) as vessel_class,
  best.best_length_m as vessel_length_m,
  best.best_tonnage_gt as vessel_tonnage_gt
FROM `gfw_research.vi_ssvid_byyear_v{vi_version}`
),

# Add in vessel class
all_vessel_info AS (
SELECT * 
FROM fishing_vessels
LEFT JOIN vessel_info
USING(ssvid, year)
),

real_gaps as 
(
select distinct gap_id
from 
`world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v{inputs_version}` 
WHERE gap_hours >= 12
AND positions_X_hours_before_sat >= 19
AND (off_distance_from_shore_m > 1852*50 AND on_distance_from_shore_m > 1852*50)
AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
)

select 
floor(a.lat*{scale}) lat_bin,
floor(a.lon*{scale}) lon_bin,
vessel_class,
flag,
b.distance_from_shore_m > 1852*200 is_high_seas,
d.gap_id is not null is_real_gap,
count(*) gap_hours,
sum(if(hours_to_nearest_ping<24*3.5,1,0)) gap_hours_truncated_at_week,
sum(if(gap_hours < 24,1,0)) gaps_under_24,
from proj_ais_gaps_catena.gap_positions_hourly_v{inputs_version}  a
join
all_vessel_info c
on a.ssvid = c.ssvid and extract(year from _partitiontime)=year
JOIN
  `world-fishing-827.pipe_static.distance_from_shore` b
ON
  cast( (a.lat*100) as int64) = cast( (b.lat*100) as int64)
  AND cast((a.lon*100) as int64) =cast(b.lon*100 as int64)
left join real_gaps d
using(gap_id)
where 
date(_partitiontime) between "2017-01-01" and "2019-12-31" -- 3 years of data
 -- only analyzing more than 50 nautical miles from shore
 and b.distance_from_shore_m > 1852*50
 and 
gap_hours >= 12
group by lat_bin, lon_bin, vessel_class,is_real_gap,is_high_seas, flag'''

# print(q)
df = gbq(q)
# -

# Inspect data:

df.head()

# +
# All time in gaps longer than 12 hours
gap_hours = pyseas.maps.rasters.df2raster(df, 'lon_bin', 
                                          'lat_bin', 
                                          'gap_hours',
                                          xyscale=scale, 
                                          per_km2=True)

# only gaps that are likely intentional
gap_hours_intentional = pyseas.maps.rasters.df2raster(df[df.is_real_gap], 
                                                      'lon_bin', 
                                                      'lat_bin', 
                                                      'gap_hours', 
                                                      xyscale=scale, 
                                                      per_km2=True)

# all gaps, but only spatially allocated up to 3.5 days after gap start
# or 3.5 days before the gap end (thus capping at one week of activity)
gap_hours_all_oneweek =  pyseas.maps.rasters.df2raster(df, 
                                                      'lon_bin', 
                                                      'lat_bin', 
                                                      'gap_hours_truncated_at_week', 
                                                      xyscale=scale, 
                                                      per_km2=True)

# only gaps that are likely intentional, but only spatially allocated up 
# to 3.5 days after gap start or 3.5 days before the gap end 
# (thus capping at one week of activity)
gap_hours_intentional_oneweek = pyseas.maps.rasters.df2raster(df[df.is_real_gap], 
                                                      'lon_bin', 
                                                      'lat_bin', 
                                                      'gap_hours_truncated_at_week', 
                                                      xyscale=scale, 
                                                      per_km2=True)


# +
# same exact tables as above, but restricting to only the high seas

gap_hours_hs = pyseas.maps.rasters.df2raster(df[df.is_high_seas], 'lon_bin', 
                                          'lat_bin', 
                                          'gap_hours',
                                          xyscale=scale, 
                                          per_km2=True)

gap_hours_intentional_hs = pyseas.maps.rasters.df2raster(df[(df.is_real_gap)&(df.is_high_seas)], 
                                                      'lon_bin', 
                                                      'lat_bin', 
                                                      'gap_hours', 
                                                      xyscale=scale, 
                                                      per_km2=True)

gap_hours_all_oneweek_hs =  pyseas.maps.rasters.df2raster(df[df.is_high_seas], 
                                                      'lon_bin', 
                                                      'lat_bin', 
                                                      'gap_hours_truncated_at_week', 
                                                      xyscale=scale, 
                                                      per_km2=True)

gap_hours_intentional_oneweek_hs = pyseas.maps.rasters.df2raster(df[(df.is_real_gap)&(df.is_high_seas)], 
                                                      'lon_bin', 
                                                      'lat_bin', 
                                                      'gap_hours_truncated_at_week', 
                                                      xyscale=scale, 
                                                      per_km2=True)
# +
# Make these by vessel class and flag state

gaps_by_v = {}
gaps_by_v_all = {}
gaps_by_v_7 = {}
gaps_by_v_7_all = {}

for vessel_class in ['drifting_longlines','squid_jigger','tuna_purse_seines','trawlers']:
    d = df[df.vessel_class==vessel_class]
    
    gaps_by_v[vessel_class] = pyseas.maps.rasters.df2raster(d[d.is_real_gap], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    
    gaps_by_v_all[vessel_class] = pyseas.maps.rasters.df2raster(d, 'lon_bin', 
                                          'lat_bin', 
                                          'gap_hours',
                                          xyscale=scale, 
                                          per_km2=True)


    gaps_by_v_7[vessel_class] = pyseas.maps.rasters.df2raster(d[d.is_real_gap], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours_truncated_at_week', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    gaps_by_v_7_all[vessel_class] = pyseas.maps.rasters.df2raster(d, 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours_truncated_at_week', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    
gaps_by_v_hs = {}
gaps_by_v_all_hs = {}
gaps_by_v_7_hs = {}
gaps_by_v_7_all_hs = {}

for vessel_class in ['drifting_longlines','squid_jigger','tuna_purse_seines','trawlers']:
    d = df[df.vessel_class==vessel_class]
    
    gaps_by_v_hs[vessel_class] = pyseas.maps.rasters.df2raster(d[(d.is_real_gap)&(d.is_high_seas)], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    
    gaps_by_v_all_hs[vessel_class] = pyseas.maps.rasters.df2raster(d[d.is_high_seas], 'lon_bin', 
                                          'lat_bin', 
                                          'gap_hours',
                                          xyscale=scale, 
                                          per_km2=True)


    gaps_by_v_7_hs[vessel_class] = pyseas.maps.rasters.df2raster(d[(d.is_real_gap)&(d.is_high_seas)], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours_truncated_at_week', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    gaps_by_v_7_all_hs[vessel_class] = pyseas.maps.rasters.df2raster(d[d.is_high_seas], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours_truncated_at_week', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
gaps_by_f = {}
gaps_by_f_all = {}
gaps_by_f_7 = {}
gaps_by_f_7_all = {}

for flag in ['CHN','TWN','ESP','JPN','KOR']:
    d = df[df.flag==flag]
    
    gaps_by_f[flag] = pyseas.maps.rasters.df2raster(d[d.is_real_gap], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    
    gaps_by_f_all[flag] = pyseas.maps.rasters.df2raster(d, 'lon_bin', 
                                          'lat_bin', 
                                          'gap_hours',
                                          xyscale=scale, 
                                          per_km2=True)


    gaps_by_f_7[flag] = pyseas.maps.rasters.df2raster(d[d.is_real_gap], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours_truncated_at_week', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    gaps_by_f_7_all[flag] = pyseas.maps.rasters.df2raster(d, 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours_truncated_at_week', 
                                                          xyscale=scale, 
                                                          per_km2=True)

gaps_by_f_hs = {}
gaps_by_f_all_hs = {}
gaps_by_f_7_hs = {}
gaps_by_f_7_all_hs = {}

for flag in ['CHN','TWN','ESP','JPN','KOR']:
    d = df[df.flag==flag]
    
    gaps_by_f_hs[flag] = pyseas.maps.rasters.df2raster(d[(d.is_real_gap)&(d.is_high_seas)], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    
    gaps_by_f_all_hs[flag] = pyseas.maps.rasters.df2raster(d[d.is_high_seas], 'lon_bin', 
                                          'lat_bin', 
                                          'gap_hours',
                                          xyscale=scale, 
                                          per_km2=True)


    gaps_by_f_7_hs[flag] = pyseas.maps.rasters.df2raster(d[(d.is_real_gap)&(d.is_high_seas)], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours_truncated_at_week', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    gaps_by_f_7_all_hs[flag] = pyseas.maps.rasters.df2raster(d[d.is_high_seas], 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'gap_hours_truncated_at_week', 
                                                          xyscale=scale, 
                                                          per_km2=True)
# -


# # Fishing Vessel Presence

# +
q = f'''WITH 

# Fishing vessels
fishing_vessels AS (
SELECT * 
FROM `gfw_research.fishing_vessels_ssvid_v{vi_version}`
),
--
--
-- Best vessel class
vessel_info AS (
SELECT
  ssvid,
  year,
  best.best_flag as flag,
  # Overwrite pole and line vessels as squid when registries say their squid
  IF(best.registry_net_disagreement = 'squid_jigger,pole_and_line',
     'squid_jigger', best.best_vessel_class) as vessel_class,
  best.best_length_m as vessel_length_m,
  best.best_tonnage_gt as vessel_tonnage_gt
FROM `gfw_research.vi_ssvid_byyear_v{vi_version}`
),
--
--
-- Add in vessel class
all_vessel_info AS (
SELECT * 
FROM fishing_vessels
LEFT JOIN vessel_info
USING(ssvid, year)
),
--
--
-- Get hourly positions
hourly_positions AS (
  SELECT
  floor(a.lat*{scale}) lat_bin,
  floor(a.lon*{scale}) lon_bin,
  vessel_class,
  flag,
  ifnull(gap_hours > 12, false) gap_over_12,
  distance_from_shore_m > 1852*200 is_high_seas,
  IF(vessel_class = 'squid_jigger', nnet_score, night_loitering) as fishing_score
  from proj_ais_gaps_catena.ais_positions_byseg_hourly_fishing_v{inputs_version} a
  join
  all_vessel_info c
  on a.ssvid = c.ssvid
  and year = extract(year from _partitiontime)
  JOIN
    `pipe_static.distance_from_shore` b
  ON
    cast( (a.lat*100) as int64) = cast( (b.lat*100) as int64)
    AND cast((a.lon*100) as int64)= cast(b.lon*100 as int64)
  where
  date(_partitiontime) between "2017-01-01" and "2019-12-31" -- 3 years of data
  and distance_from_shore_m > 1852*50 -- only areas more than 50 nautical miles from shore
)
--
-- Calculate gridded hours and fishing hours
SELECT 
lat_bin,
lon_bin,
gap_over_12,
vessel_class,
flag,
is_high_seas,
count(*) hours,
sum(if(fishing_score = 1,1,0)) fishing_hours
FROM hourly_positions 
group by lat_bin, lon_bin, gap_over_12, vessel_class,is_high_seas, flag'''

# df_fishing = gbq(q)
# print(q)
# +
# vessel_hours and fishing hours
# note that this includes time in gaps that are longer than 12 hours
# but shorter than 24 hours because the segmenter breaks at 24 hours
vessel_hours = pyseas.maps.rasters.df2raster(df_fishing, 'lon_bin', 'lat_bin',
                                             'hours', xyscale=scale, per_km2=True)
vessel_hours_hs = pyseas.maps.rasters.df2raster(df_fishing[df_fishing.is_high_seas], 'lon_bin', 'lat_bin',
                                             'hours', xyscale=scale, per_km2=True)
fishing_hours = pyseas.maps.rasters.df2raster(df_fishing, 'lon_bin', 'lat_bin',
                                             'fishing_hours', xyscale=scale, per_km2=True)


# only time in gaps over 12 hours
vessel_hours_over12 = pyseas.maps.rasters.df2raster( df_fishing[df_fishing.gap_over_12], 'lon_bin', 'lat_bin',
                                             'hours', xyscale=scale, per_km2=True)
# only time when time beteween positions is less than 12 hours
vessel_hours_under12 = pyseas.maps.rasters.df2raster( df_fishing[df_fishing.gap_over_12==False], 'lon_bin',
                                                     'lat_bin', 'hours', xyscale=scale, per_km2=True)
vessel_hours_under12_hs = \
     pyseas.maps.rasters.df2raster( df_fishing[(df_fishing.gap_over_12==False)&(df_fishing.is_high_seas)],
                                   'lon_bin','lat_bin', 'hours', xyscale=scale, per_km2=True)

#
# Create rasters by vessel class
#
presence_by_v = {}
# presence only in time with gaps < 12 hours, by vessel class
presence_by_v_u12 = {}
# presence only in time with gaps < 12 hours, by vessel class, only the high seas
presence_by_v_u12_hs = {}


for vessel_class in ['drifting_longlines','squid_jigger','tuna_purse_seines','trawlers']:
    d = df_fishing[df_fishing.vessel_class==vessel_class]
    
    presence_by_v[vessel_class] = pyseas.maps.rasters.df2raster(d, 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    d = d[~d.gap_over_12]
    presence_by_v_u12[vessel_class] = pyseas.maps.rasters.df2raster(d, 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    d = d[(~d.gap_over_12)&(d.is_high_seas)]
    presence_by_v_u12_hs[vessel_class] = pyseas.maps.rasters.df2raster(d, 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
#
# Rasters by flag state
#
presence_by_f = {}
presence_by_f_u12 = {}
presence_by_f_u12_hs = {}

for flag in ['CHN','TWN','ESP','JPN','KOR']:
    d = df_fishing[df_fishing.flag==flag]
    
    presence_by_f[flag] = pyseas.maps.rasters.df2raster(d, 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    d = d[~d.gap_over_12]
    presence_by_f_u12[flag] = pyseas.maps.rasters.df2raster(d, 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
    
    presence_by_f_u12_hs[flag] = pyseas.maps.rasters.df2raster(d, 
                                                          'lon_bin', 
                                                          'lat_bin', 
                                                          'hours', 
                                                          xyscale=scale, 
                                                          per_km2=True)
# -
# ## Total Time in Gaps  
#
# Plot maps of time lost to AIS gaps and disabling.

with pyseas.context(pyseas.styles.dark): 
    grid = np.copy(gap_hours_all_oneweek)
    fig_min_value = 10
    fig_max_value = 1000  
    grid[grid<fig_min_value/1000]=np.nan

    norm = colors.LogNorm(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(20, 20))
    
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(grid*1000,
                            r"Hours of gaps per 1000 km2",   
                            cmap="fishing",
                            loc='bottom',  
                            norm = norm)

    ax.set_title("Hours of Time in Gaps Longer than 12 hours\nin AIS Transmission, 2017-2019", fontsize=20)
#     plt.savefig("gaphours_all.png",dpi=200,bbox_inches='tight')

with pyseas.context(pyseas.styles.dark): 
    grid = np.copy(gap_hours_intentional_oneweek)
    fig_min_value = 10
    fig_max_value = 1000 
    
    grid[grid<fig_min_value/1000]=np.nan

    norm = colors.LogNorm(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(20, 20))
    
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(grid*1000,
                            r"Hours of gaps per 1000 km2",   
                            cmap="fishing",
                            loc='bottom',  
                            norm = norm)
    pyseas.maps.add_eezs(ax)

    ax.set_title("Hours of Activity with AIS Disabled, 2017-2019", fontsize=20)
#     plt.savefig("gaphours_intentional.png",dpi=200,bbox_inches='tight')

with pyseas.context(pyseas.styles.dark): 
    grid = np.copy(gap_hours_intentional_oneweek_hs)
    grid[grid<10/1000]=np.nan
    fig_min_value = 10
    fig_max_value = 1000 
    norm = colors.LogNorm(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(20, 20))
    
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(grid*1000,
                            r"Hours per 1000 km2",   
                            cmap="fishing",
                            loc='bottom',  
                            norm = norm) #,
#                             projection = 'global.pacific_centered')
    pyseas.maps.add_eezs(ax)

    ax.set_title("Hours of Time in Gaps Longer than 12 hours\nin AIS Transmission, High Seas, 2017-2019",
                 fontsize=20)
#     plt.savefig("../figures/gaphours_intentional_hs.png",dpi=200,bbox_inches='tight')

with pyseas.context(pyseas.styles.dark): #, plt.rc_context({'gfw.border.linewidth' : 0}):
    # note we are adding the gaps truncated at one week to the vessel activity under 12 hour gaps
    grid = np.copy(np.add(vessel_hours_under12, gap_hours_all_oneweek))*1000
    fig_min_value = 10
    fig_max_value = 10000  
    norm = colors.LogNorm(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(20, 20))
    
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(grid,
                            r"Hours per 1000 km2",   
                            cmap="presence",
                            loc='bottom',  
                            norm = norm) #,
#                             projection = 'global.pacific_centered')
    pyseas.maps.add_eezs(ax)

    ax.set_title("Fishing Vessel Presence", fontsize=20)
#     plt.savefig("PelagicLonglineGlobalPacCenterStudyPeriod.png",dpi=200,bbox_inches='tight')


# ## Figure 1
#
# The below figures are options for the main text Figure 1 global figure of AIS disabling.

with pyseas.context(pyseas.styles.dark): #, plt.rc_context({'gfw.border.linewidth' : 0}):
    grid = np.copy(gap_hours_intentional_oneweek_hs)
    grid[grid<10/1000]=np.nan
    fig_min_value = 10
    fig_max_value = 1000 
    norm = colors.LogNorm(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(20, 20))
    
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(grid*1000,
                            r"Hours per 1000 km2",   
                            cmap="fishing",
                            loc='bottom',  
                            norm = norm) #,
#                             projection = 'global.pacific_centered')
    pyseas.maps.add_eezs(ax)

    ax.set_title("Hours of High Seas Activity with AIS Disabled, 2017-2019",
                 fontsize=20)
#     plt.savefig("../figures/figure_1_gaphours_intentional_hs.png",dpi=200,bbox_inches='tight')

# ## Fraction of time lost to gaps
#
# Map the fraction of time lost to gaps for each one-degree cell.

with pyseas.context(pyseas.styles.dark): #, plt.rc_context({'gfw.border.linewidth' : 0}):
    # a bit tricky... 
    lost = np.divide(gap_hours_all_oneweek, np.add(gap_hours_all_oneweek,vessel_hours_under12 ))
    lost[vessel_hours < 24*7/(111*111)] = np.nan # make sure at least two weeks of activity
    grid = np.copy(lost)
    fig_min_value = 0
    fig_max_value = .5
    norm = colors.Normalize(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(20, 20))
    
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(grid,
                            r"Fraction Time in >12 Hour Gaps",   
                            cmap="fishing",
                            loc='bottom',  
                            norm = norm) #,
#                             projection = 'global.pacific_centered')
    pyseas.maps.add_eezs(ax)

    ax.set_title("All Gaps in AIS > 12 Hours / Total Activity", fontsize=20)
#     plt.savefig("gap_fraction_all.png",dpi=200,bbox_inches='tight')

with pyseas.context(pyseas.styles.dark): #, plt.rc_context({'gfw.border.linewidth' : 0}):
    # a bit tricky... all vessel activity divided by 
    lost = np.divide(np.subtract(gap_hours_all_oneweek, gap_hours_intentional_oneweek),
                     np.add(gap_hours_all_oneweek,vessel_hours_under12 ))
    lost[vessel_hours < 24*7/(111*111)] = np.nan # make sure at least two weeks of activity
    grid = np.copy(lost)
    fig_min_value = 0
    fig_max_value = .5
    norm = colors.Normalize(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(20, 20))
    
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(grid,
                            r"Fraction Time in >12 Intentional Gaps",   
                            cmap="fishing",
                            loc='bottom',  
                            norm = norm) #,
#                             projection = 'global.pacific_centered')
    pyseas.maps.add_eezs(ax)

    ax.set_title("Unintentional Gaps / Total Activity", fontsize=20)
#     plt.savefig("gap_fraction_unintentional.png",dpi=200,bbox_inches='tight')


with pyseas.context(pyseas.styles.dark): #, plt.rc_context({'gfw.border.linewidth' : 0}):
    # a bit tricky... all vessel activity divided by 
    lost = np.divide( gap_hours_intentional_oneweek,
                     np.add(gap_hours_all_oneweek,vessel_hours_under12 ))
    lost[vessel_hours < 24*7/(111*111)] = np.nan # make sure at least two weeks of activity
    grid = np.copy(lost)
    fig_min_value = 0
    fig_max_value = .5
    norm = colors.Normalize(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(20, 20))
    
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(grid,
                            r"Fraction Time in >12 Intentional Gaps",   
                            cmap="fishing",
                            loc='bottom',  
                            norm = norm) #,
#                             projection = 'global.pacific_centered')
    pyseas.maps.add_eezs(ax)

    ax.set_title("Fraction of Activity with AIS Disabled, 2017-2019", fontsize=20)
#     plt.savefig("gap_fraction_intentional.png",dpi=200,bbox_inches='tight')


with pyseas.context(pyseas.styles.light): #, plt.rc_context({'gfw.border.linewidth' : 0}):
    # a bit tricky... 
    lost = np.divide(gap_hours_intentional_oneweek, np.add(gap_hours_all_oneweek,vessel_hours_under12 ))
    lost[vessel_hours_hs < 24*7/(111*111)] = np.nan # make sure at least two weeks of activity    grid = np.copy(lost)
    fig_min_value = 0
    fig_max_value = .4
    norm = colors.Normalize(vmin=fig_min_value, vmax=fig_max_value)
    fig = plt.figure(figsize=(15, 15))
    
    ax, im, colorbar = pyseas.maps.plot_raster_w_colorbar(lost,
                            r"Fraction of activity lost to suspected AIS disabling",   
                            cmap="presence",
                            loc='bottom',  
                            norm = norm) #,
#                             projection = 'global.pacific_centered')
    pyseas.maps.add_eezs(ax)
    ax.set_title("Fraction of Activity with AIS Disabled, High Seas", fontsize=20)
    plt.savefig("../results/gap_figures_v{}/gap_fraction_intentional_hs.png".format(inputs_version),
                dpi=200,
                bbox_inches='tight')

# Calculate the different time lost to gaps metrics.

# +
# Total time lost to disabling on the high seas
lost_time_int_hs = np.copy(gap_hours_intentional_oneweek_hs)
# lost_time_int_hs[lost_time_int_hs<10/1000]=np.nan 

# Fraction of all activity
lost = np.divide(gap_hours_all_oneweek, np.add(gap_hours_all_oneweek,vessel_hours_under12 ))
# lost[vessel_hours < 24*7/(111*111)] = np.nan # make sure at least two weeks of activity

# Fraction unintentional
lost_unint = np.divide(np.subtract(gap_hours_all_oneweek, gap_hours_intentional_oneweek),
                     np.add(gap_hours_all_oneweek,vessel_hours_under12 ))
# lost_unint[vessel_hours < 24*7/(111*111)] = np.nan # make sure at least two weeks of activity

# Fraction intentional
lost_int = np.divide(gap_hours_intentional_oneweek, np.add(gap_hours_all_oneweek,vessel_hours_under12 ))
# lost_int[vessel_hours < 24*7/(111*111)] = np.nan # make sure at least two weeks of activity

# Fraction intentional on high seas
lost_int_hs = np.divide(gap_hours_intentional_oneweek_hs, np.add(gap_hours_all_oneweek_hs,vessel_hours_under12_hs ))
# lost_int_hs[vessel_hours_hs < 24*7/(111*111)] = np.nan # make sure at least two weeks of activity    grid = np.copy(lost)
# -


# ### Output data for Figures 1
# Output the total time and fraction of time lost to intentional gaps on the high seas as a csv.

# +
# Min/Max coordinates 
min_lon, min_lat, max_lon, max_lat  = -180, -90, 180, 90

# Number of lat/lon bins
inverse_delta_degrees = 1
n_lat = (max_lat - min_lat) * inverse_delta_degrees
n_lon = (max_lon - min_lon) * inverse_delta_degrees

lons = np.arange(min_lon, max_lon+1)
lats = np.arange(min_lat, max_lat+1)

# Empty list to store coordinates and values
fig_df = []
lost_time_df = []
lost_int_hs_df = []

# Loop over lat/lon and fill with positions per hour
for lat in range(n_lat):
    for lon in range(n_lon):
        ### Add data to list
        # Total time 
        lost_time_df.append([lats[lat], lons[lon], gap_hours_intentional_oneweek_hs[lat][lon]])
        # Fraction of time
        lost_int_hs_df.append([lats[lat], lons[lon], lost_int_hs[lat][lon]])
        # Figure df
        fig_df.append([lats[lat], 
                       lons[lon],
                       vessel_hours_under12_hs[lat][lon],
                       gap_hours_intentional_oneweek_hs[lat][lon],
                       lost_int_hs[lat][lon]])

# Convert lists to pandas dataframes
lost_time_df = pd.DataFrame(lost_time_df, columns=['lat_bin','lon_bin','hours_lost'])
lost_time_df['gap_type'] = 'intentional_high_seas'

lost_int_hs_df = pd.DataFrame(lost_int_hs_df, columns=['lat_bin','lon_bin','frac_lost'])
lost_int_hs_df['gap_type'] = 'intentional_high_seas'

fig_df = pd.DataFrame(fig_df,
                     columns=['lat_bin','lon_bin','vessel_hours','gap_hours','frac_lost'])
fig_df['gap_type'] = 'intentional_high_seas'

# Save as csv
# lost_time_df.to_csv('../data/gap_inputs_v{v}/total_time_lost_to_gaps_hs_v{v}.csv'.format(v=inputs_version))
# lost_int_hs_df.to_csv('../data/gap_inputs_v{v}/fraction_time_lost_to_gaps_hs_v{v}.csv'.format(v=inputs_version))
fig_df.to_csv('../data/gap_inputs_v{v}/figure_1_data_v{v}.csv'.format(v=inputs_version))
# -

# ## Figure 2 

# ### Output data for Figure 2 (total time by flag state and vessel class)

# +
# Min/Max coordinates 
min_lon, min_lat, max_lon, max_lat  = -180, -90, 180, 90

# Number of lat/lon bins
inverse_delta_degrees = 1
n_lat = (max_lat - min_lat) * inverse_delta_degrees
n_lon = (max_lon - min_lon) * inverse_delta_degrees

lons = np.arange(min_lon, max_lon+1)
lats = np.arange(min_lat, max_lat+1)

vessel_classes = ['drifting_longlines','squid_jigger','tuna_purse_seines','trawlers']

flags =  ['CHN','TWN','ESP','KOR']
country_names = ["China","Taiwan","Spain","South Korea"]

# Empty list to store coordinates and values
flag_df = []
vc_df = []

# Loop over lat/lon and fill with positions per hour
for flag in flags:
    for lat in range(n_lat):
        for lon in range(n_lon):
            # Pull out flag state data 
            flag_gaps = gaps_by_f_7_hs[flag]
            ### Add data to list      
            flag_df.append([lats[lat], lons[lon], flag, flag_gaps[lat][lon]])

# Loop over lat/lon and fill with positions per hour
for vessel_class in vessel_classes:
    for lat in range(n_lat):
        for lon in range(n_lon):
            # Pull out flag state data
            vc_gaps = gaps_by_v_7_hs[vessel_class]
            ### Add data to list 
            vc_df.append([lats[lat], lons[lon], vessel_class, vc_gaps[lat][lon]])

# Convert lists to pandas dataframes
flag_df = pd.DataFrame(flag_df, columns=['lat_bin','lon_bin','flag_state','hours_lost'])
flag_df['gap_type'] = 'intentional_high_seas'
flag_df.to_csv('../data/gap_inputs_v{v}/figure_2_flag_data_v{v}.csv'.format(v=inputs_version))

vc_df = pd.DataFrame(vc_df, columns=['lat_bin','lon_bin','vessel_class','hours_lost'])
vc_df['gap_type'] = 'intentional_high_seas'
vc_df.to_csv('../data/gap_inputs_v{v}/figure_2_vessel_class_data_v{v}.csv'.format(v=inputs_version))
# -

# # As Distance from Shore

q = f'''with 


# Fishing vessels
fishing_vessels AS (
SELECT * 
FROM `gfw_research.fishing_vessels_ssvid_v{vi_version}`
),
# Best vessel class
vessel_info AS (
SELECT
  ssvid,
  year,
  best.best_flag as flag,
  # TEMPORARY
  # Overwrite pole and line vessels as squid when registries say their squid
  IF(best.registry_net_disagreement = 'squid_jigger,pole_and_line',
     'squid_jigger', best.best_vessel_class) as vessel_class,
  best.best_length_m as vessel_length_m,
  best.best_tonnage_gt as vessel_tonnage_gt
FROM `gfw_research.vi_ssvid_byyear_v{vi_version}`
),
# Add in vessel class
all_vessel_info AS (
SELECT * 
FROM fishing_vessels
LEFT JOIN vessel_info
USING(ssvid, year)
),

real_gaps as 
(
select distinct gap_id
from 
`world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v{inputs_version}` 
where
 positions_X_hours_before_sat >= 19  
  and (positions_per_day_off > 5 and positions_per_day_on > 5)
 and (off_distance_from_shore_m > 1852*50 and on_distance_from_shore_m > 1852*50)
),


gaps_by_distance as (
select 
vessel_class,
floor(b.distance_from_shore_m/1000) distnace_from_shore_km,
count(*) gap_hours,
sum(if(hours_to_nearest_ping<24*3.5,1,0)) gap_hours_truncated_at_week,
sum(if(d.gap_id is not null,1,0)) real_gap_hours,
sum(if(d.gap_id is not null and hours_to_nearest_ping<24*3.5,1,0)) real_gap_hours_truncated_at_week,
-- sum(if(gap_hours < 24,1,0)) gaps_under_24,
from proj_ais_gaps_catena.gap_positions_hourly_v{inputs_version}  a
join
all_vessel_info c
on a.ssvid = c.ssvid and extract(year from _partitiontime)=year
JOIN
  `world-fishing-827.pipe_static.distance_from_shore` b
ON
  cast( (a.lat*100) as int64) = cast( (b.lat*100) as int64)
  AND cast((a.lon*100) as int64) =cast(b.lon*100 as int64)
left join real_gaps d
using(gap_id)
where 
gap_hours > 12
group by vessel_class, distnace_from_shore_km
order by distnace_from_shore_km),

vessels_by_distance as (
select 
vessel_class,
floor(b.distance_from_shore_m/1000) distnace_from_shore_km, 
sum(if(gap_hours < 12, 1,0)) hours_under12,
count(*) hours,
sum(
    CASE WHEN 
    vessel_class = 'squid_jigger' AND night_loitering = 1 THEN 1
    WHEN vessel_class != 'squid_jigger' AND nnet_score = 1 THEN 1
    ELSE 0
    END
) fishing_hours
from proj_ais_gaps_catena.ais_positions_byseg_hourly_fishing_v{inputs_version} a
join
all_vessel_info c
on a.ssvid = c.ssvid
and year = extract(year from _partitiontime)
JOIN
  `world-fishing-827.pipe_static.distance_from_shore` b
ON
  cast( (a.lat*100) as int64) = cast( (b.lat*100) as int64)
  AND cast((a.lon*100) as int64)= cast(b.lon*100 as int64)
group by vessel_class, distnace_from_shore_km)
select *,
distnace_from_shore_km/1.852 as distance_from_shore_nm
from
vessels_by_distance
full outer join
gaps_by_distance
using(vessel_class, distnace_from_shore_km)
order by distnace_from_shore_km
'''
# print(q)
df_dist_shore = gbq(q)

df_dist_shore.head()

# #  Time Lost to Gaps, Not Capping at 7 days

# +
vessel_classes = ['tuna_purse_seines','trawlers','squid_jigger','drifting_longlines']


plt.figure(figsize=(10,5))
for vessel_class in vessel_classes:
    d = df_dist_shore[df_dist_shore.vessel_class==vessel_class]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_t'] = d.real_gap_hours_truncated_at_week/(d.gap_hours + d.hours_under12)
    plt.plot(d.distance_from_shore_nm, (d.gap_hours + d.hours_under12)/24, label = vessel_class)

plt.xlim(150,300)
plt.ylim(0,4e3)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Days of Activity by Vessel Class, Distance from Shore")
plt.xlabel("Distance from Shore, Nautical MIles")
plt.legend()
plt.show()

plt.figure(figsize=(10,5))
for vessel_class in vessel_classes:
    d = df_dist_shore[df_dist_shore.vessel_class==vessel_class]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_t'] = d.real_gap_hours_truncated_at_week/(d.gap_hours + d.hours_under12)
    plt.plot(d.distance_from_shore_nm, d.real_gap_hours_truncated_at_week/24, label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,400)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Days of Activity by Vessel Class Lost to Intentional Gaps")
plt.xlabel("Distance from Shore, Nautical Miles")
plt.legend()
plt.show()

plt.figure(figsize=(10,5))
for vessel_class in vessel_classes:
    d = df_dist_shore[df_dist_shore.vessel_class==vessel_class]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    plt.plot(d.distance_from_shore_nm, 
             d.gap_hours/24 - d.real_gap_hours_truncated_at_week/24, 
             label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,800)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Days of Activity by Vessel Class Lost to Unintentional Gaps")
plt.xlabel("Distance from Shore, Nautical Miles")
plt.legend()
plt.show()



# -

#

# +
# Set colors for plots
colors = {'tuna_purse_seines': "#204280",
          'trawlers': "#ad2176",
          'squid_jigger': "#ee6256",
          'drifting_longlines': "#f8ba47"}

vessel_classes = ['tuna_purse_seines','trawlers','squid_jigger','drifting_longlines']

plt.figure(figsize=(10,5))
for vessel_class in vessel_classes:
    d = df_dist_shore[df_dist_shore.vessel_class==vessel_class]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_t'] = d.real_gap_hours_truncated_at_week/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_ut'] = (d.gap_hours - d.real_gap_hours)/(d.gap_hours + d.hours_under12)

    plt.plot(d.distance_from_shore_nm, 
             d.gaps_per_hour_ut, 
             label = vessel_class.replace("_"," "),
             color = colors[vessel_class])
#     plt.plot(d.distnace_from_shore_nm, d.hours, label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,0.3)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Fraction of Activity by Vessel Class Lost to Unintentional Gaps")
plt.xlabel("Distance from shore (nm)")
plt.legend()
plt.show()

plt.figure(figsize=(10,5))
for vessel_class in vessel_classes:
    d = df_dist_shore[df_dist_shore.vessel_class==vessel_class]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_t'] = d.real_gap_hours_truncated_at_week/(d.gap_hours + d.hours_under12)
    
    plt.plot(d.distance_from_shore_nm, 
             d.gaps_per_hour_t, 
             label = vessel_class.replace("_"," "),
             color = colors[vessel_class])
#     plt.plot(d.distnace_from_shore_nm, d.hours, label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,0.3)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Fraction of activity lost to AIS disabling by vessel class,\nduration of suspected disabling events capped at seven days")
plt.xlabel("Distance from shore (nm)")
plt.legend()
# plt.show()
plt.savefig("../results/gap_figures_v{}/figure_si_frac_lost_by_dist_geartype.png".format(inputs_version),
            dpi=300, 
            bbox_inches='tight')


plt.figure(figsize=(10,5))
for vessel_class in vessel_classes:
    d = df_dist_shore[df_dist_shore.vessel_class==vessel_class]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_t'] = d.real_gap_hours_truncated_at_week/(d.gap_hours + d.hours_under12)
    plt.plot(d.distance_from_shore_nm, 
             d.gaps_per_hour, 
             label = vessel_class.replace("_"," "),
             color = colors[vessel_class])
#     plt.plot(d.distnace_from_shore_nm, d.hours, label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,1)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Fraction of Activity by Vessel Class Lost to AIS Disabling")
plt.xlabel("Distance from shore (nm)")
plt.legend()
plt.show()
# -


# # Distance from shore by country

q = f'''with 


# Fishing vessels
fishing_vessels AS (
SELECT * 
FROM `gfw_research.fishing_vessels_ssvid_v{vi_version}`
),
# Best vessel class
vessel_info AS (
SELECT
  ssvid,
  year,
  best.best_flag as flag,
  # TEMPORARY
  # Overwrite pole and line vessels as squid when registries say their squid
  IF(best.registry_net_disagreement = 'squid_jigger,pole_and_line',
     'squid_jigger', best.best_vessel_class) as vessel_class,
  best.best_length_m as vessel_length_m,
  best.best_tonnage_gt as vessel_tonnage_gt
FROM `gfw_research.vi_ssvid_byyear_v{vi_version}`
),
# Add in vessel class
all_vessel_info AS (
SELECT * 
FROM fishing_vessels
LEFT JOIN vessel_info
USING(ssvid, year)
),

real_gaps as 
(
select distinct gap_id
from 
`world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v{inputs_version}` 
where
 positions_X_hours_before_sat >= 19  
  and (positions_per_day_off > 5 and positions_per_day_on > 5)
 and (off_distance_from_shore_m > 1852*50 and on_distance_from_shore_m > 1852*50)
),


gaps_by_distance as (
select 
flag,
floor(b.distance_from_shore_m/1000) distnace_from_shore_km,
count(*) gap_hours,
sum(if(d.gap_id is not null,1,0)) real_gap_hours,
sum(if(hours_to_nearest_ping<24*3.5,1,0)) gap_hours_truncated_at_week,
sum(if(d.gap_id is not null and hours_to_nearest_ping<24*3.5,1,0)) real_gap_hours_truncated_at_week,
-- sum(if(gap_hours < 24,1,0)) gaps_under_24,
from proj_ais_gaps_catena.gap_positions_hourly_v{inputs_version}  a
join
all_vessel_info c
on a.ssvid = c.ssvid and extract(year from _partitiontime)=year
JOIN
  `world-fishing-827.pipe_static.distance_from_shore` b
ON
  cast( (a.lat*100) as int64) = cast( (b.lat*100) as int64)
  AND cast((a.lon*100) as int64) =cast(b.lon*100 as int64)
left join real_gaps d
using(gap_id)
where 
gap_hours >= 12
group by flag, distnace_from_shore_km
order by distnace_from_shore_km),

vessels_by_distance as (
select 
flag,
floor(b.distance_from_shore_m/1000) distnace_from_shore_km, 
sum(if(gap_hours < 12, 1,0)) hours_under12,
count(*) hours,
sum(
    CASE WHEN 
    vessel_class = 'squid_jigger' AND night_loitering = 1 THEN 1
    WHEN vessel_class != 'squid_jigger' AND nnet_score = 1 THEN 1
    ELSE 0
    END
) fishing_hours
from proj_ais_gaps_catena.ais_positions_byseg_hourly_fishing_v{inputs_version} a
join
all_vessel_info c
on a.ssvid = c.ssvid
and year = extract(year from _partitiontime)
JOIN
  `world-fishing-827.pipe_static.distance_from_shore` b
ON
  cast( (a.lat*100) as int64) = cast( (b.lat*100) as int64)
  AND cast((a.lon*100) as int64)= cast(b.lon*100 as int64)
-- where
-- distance_from_shore_m > 1852*200
group by flag, distnace_from_shore_km)
select *,
distnace_from_shore_km/1.852 as distance_from_shore_nm
from
vessels_by_distance
full outer join
gaps_by_distance
using(flag, distnace_from_shore_km)
order by distance_from_shore_nm

'''
df_dist_shore_f = gbq(q)

# +
# Set colors for plots
colors = {'CHN': "#204280",
          'TWN': "#ad2176",
          'ESP': "#ee6256",
          'KOR': "#f8ba47"}

plt.figure(figsize=(10,5))
for flag in ['CHN','TWN','ESP','KOR']:
    d = df_dist_shore_f[df_dist_shore_f.flag==flag]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_t'] = d.real_gap_hours_truncated_at_week/(d.gap_hours + d.hours_under12)
    plt.plot(d.distance_from_shore_nm, 
             (d.gap_hours + d.hours_under12)/24, 
             label = flag,
            color = colors[flag])
#     plt.plot(d.distnace_from_shore_nm, d.hours, label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,4e3)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Days of Activity by flag, Distance from Shore")
plt.xlabel("Distance from shore (nm)")
plt.legend()
plt.show()

plt.figure(figsize=(10,5))
for flag in ['CHN','TWN','ESP','KOR']:
    d = df_dist_shore_f[df_dist_shore_f.flag==flag]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_t'] = d.real_gap_hours_truncated_at_week/(d.gap_hours + d.hours_under12)
    plt.plot(d.distance_from_shore_nm, 
             d.real_gap_hours_truncated_at_week/24, 
             label = flag,
            color = colors[flag])
#     plt.plot(d.distnace_from_shore_nm, d.hours, label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,400)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Days of Activity by Flag Lost to Intentional Gaps")
plt.xlabel("Distance from shore (nm)")
plt.legend()
plt.show()

plt.figure(figsize=(10,5))
for flag in ['CHN','TWN','ESP','KOR']:
    d = df_dist_shore_f[df_dist_shore_f.flag==flag]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    plt.plot(d.distance_from_shore_nm, 
             d.gap_hours/24 - d.real_gap_hours_truncated_at_week/24, 
             label = flag,
             color = colors[flag])
#     plt.plot(d.distnace_from_shore_nm, d.hours, label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,800)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Days of Activity by Vessel Class Lost to Unintentional Gaps")
plt.xlabel("Distance from shore (nm)")
plt.legend()
plt.show()



plt.figure(figsize=(10,5))
for flag in ['CHN','TWN','ESP','KOR']:
    d = df_dist_shore_f[df_dist_shore_f.flag==flag]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_t'] = d.real_gap_hours_truncated_at_week/(d.gap_hours + d.hours_under12)
    plt.plot(d.distance_from_shore_nm, 
             d.gaps_per_hour_t, 
             label = flag,
             color = colors[flag])
#     plt.plot(d.distnace_from_shore_nm, d.hours, label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,.5)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Fraction of of activity lost to AIS disabling by flag state,\nduration of suspected disabling events capped at seven days")
plt.xlabel("Distance from shore (nm)")
plt.legend()
# plt.show()
plt.savefig("../results/gap_figures_v{}/figure_si_frac_lost_by_dist_flag.png".format(inputs_version),
            dpi=300, 
            bbox_inches='tight')

plt.figure(figsize=(10,5))
for flag in ['CHN','TWN','ESP','KOR']:
    d = df_dist_shore_f[df_dist_shore_f.flag==flag]
    d = d.fillna(0)
    d['gaps_per_hour'] = d.real_gap_hours/(d.gap_hours + d.hours_under12)
    d['gaps_per_hour_t'] = d.real_gap_hours_truncated_at_week/(d.gap_hours + d.hours_under12)
    plt.plot(d.distance_from_shore_nm, 
             d.gaps_per_hour, 
             label = flag,
             color = colors[flag])
#     plt.plot(d.distnace_from_shore_nm, d.hours, label = vessel_class)
plt.xlim(150,300)
plt.ylim(0,.5)
plt.plot([200,200],[0,1e6], linestyle='dashed', color='grey', label='EEZ boundary')
plt.title("Fraction of of Activity by Flag Lost to Intentional Gaps")
plt.xlabel("Distance from shore (nm)")
plt.legend()
plt.show()
    
# -
# # Make Tables

q = f'''with # Fishing vessels
fishing_vessels AS (
SELECT * 
FROM `gfw_research.fishing_vessels_ssvid_v{vi_version}`
),
# Best vessel class
vessel_info AS (
SELECT
  ssvid,
  year,
  best.best_flag as flag,
  # Overwrite pole and line vessels as squid when registries say their squid
  IF(best.registry_net_disagreement = 'squid_jigger,pole_and_line',
     'squid_jigger', best.best_vessel_class) as vessel_class,
  best.best_length_m as vessel_length_m,
  best.best_tonnage_gt as vessel_tonnage_gt
FROM `gfw_research.vi_ssvid_byyear_v{vi_version}`
),
# Add in vessel class
all_vessel_info AS (
SELECT * 
FROM fishing_vessels
LEFT JOIN vessel_info
USING(ssvid, year) ),


gaps_table as (

select 
gap_id,
gap_hours,
b.flag as flag,
b.vessel_class vessel_class,
row_number() over (partition by gap_id order by gap_hours, rand()) row
from 
proj_ais_gaps_catena.raw_gaps_v{inputs_version} a 
left join 
all_vessel_info b
using(ssvid, year)
where a.positions_X_hours_before_sat >= 19
 and (a.positions_per_day_off > 5 and a.positions_per_day_on > 5)
and a.off_distance_from_shore_m > 1852*200
and gap_hours >= 12
)

select 
vessel_class, 
flag,
count(*) gaps, 
sum(gap_hours) gap_hours,
sum(gap_hours)/count(*) hours_per_gap
from gaps_table
--where row = 1
group by vessel_class, flag
order by gaps desc

'''
df_gap_counts = gbq(q)

df_dist_shore.head()

rows = []
vessel_classes =  ['drifting_longlines','squid_jigger','tuna_purse_seines','trawlers']
for vessel_class in vessel_classes + ['other_vessels','total']:
    
    if vessel_class == "other_vessels":
        d = df_dist_shore[~df_dist_shore.vessel_class.isin(vessel_classes)]
    elif vessel_class == 'total':
        d = df_dist_shore
    else:
        d = df_dist_shore[(df_dist_shore.vessel_class==vessel_class)]        
    d = d[(d.distance_from_shore_nm>=200)]
    d = d.fillna(0)
    hours = d.hours_under12.sum()  + d.gap_hours.sum()
    hours_t = d.hours_under12.sum() + d.gap_hours_truncated_at_week.sum()
    real_gap_hours = d.real_gap_hours.sum()
    real_gap_hours_t = d.real_gap_hours_truncated_at_week.sum()
   

    if vessel_class == "other_vessels":
        d = df_gap_counts[~df_gap_counts.vessel_class.isin(vessel_classes)]
    elif vessel_class == 'total':
        d = df_gap_counts
    else:
        d = df_gap_counts[(df_gap_counts.vessel_class==vessel_class)]  
    gaps = d.gaps.values.sum()
    
    
    gaps_per_day = gaps/hours*24


    
    if vessel_class == "other_vessels":
        d = df_fishing[~df_fishing.vessel_class.isin(vessel_classes)]
    elif vessel_class == "total":
        d = df_fishing
    else:
        d = df_fishing[(df_fishing.vessel_class==vessel_class)]      
    d = d[d.is_high_seas]
    
    
    
    days_activity_1000 = round(hours/1000/24)
    gap_days_low = round(real_gap_hours_t/24)
    gap_days_high = round(real_gap_hours/24)
    low_perc_days_lost = int(round(real_gap_hours_t/hours_t*100) )
    high_perc_days_lost = int(round(real_gap_hours/hours*100))    
    gaps_per_1000day = round(gaps_per_day*1000) 
    days_per_gap_low = round(real_gap_hours_t/gaps/24,1)
    days_per_gap_high = round(real_gap_hours/gaps/24,1)

    rows.append([vessel_class.replace("_"," "),
                 days_activity_1000,
                 gaps,
                 gaps_per_1000day,
                 f"{real_gap_hours_t}-{real_gap_hours}",
                 f"{gap_days_low}-{gap_days_high}",
                 f"{low_perc_days_lost}-{high_perc_days_lost}%",
                 f"{days_per_gap_low}-{days_per_gap_high}"])

# +
headers = ['vessel class',
          '1000s days\nof activity',
          'disabling\nevents',
          'high seas disabling events\nper 1000 days',
          'hours lost to\ndisabling events',
          'days lost to\ndisabling events',
          'fraction of time lost to\ndisabling events',
          'avg length\nof disabling\nevent, days' ]

print(tabulate(rows, headers=headers))# '1000 fhours','g/fishingday*100']))

# -

table_1_a = pd.DataFrame(rows, columns=['group','1000s_days_activity','disabling_events',
                                        'hs_disabling_events_per_1000_days','hours_lost_to_disabling',
                                        'days_lost_to_disabling',
                                        'frac_time_lost_to_disabling','avg_length_disabling_event'])
table_1_a.to_csv('../data/gap_inputs_v{v}/table_1_time_lost_to_gaps_vessel_class_v{v}.csv'.format(v=inputs_version))

# +
# the following is to print the results and then copy and paste into a spreadsheet 

print("\t".join(map(lambda x: x.replace("\n"," "),headers)))
for row in rows:
    print("\t".join(list(map(str,row))))
# -

rows = []
flags =  ['CHN','TWN','ESP','KOR']
for flag in flags + ['other_vessels','total']:
    
    if flag == "other_vessels":
        d = df_dist_shore_f[~df_dist_shore_f.flag.isin(flags)]
    elif flag == 'total':
        d = df_dist_shore_f
    else:
        d = df_dist_shore_f[(df_dist_shore_f.flag==flag)]        
    d = d[(d.distance_from_shore_nm>=200)]
    d = d.fillna(0)
    hours = d.hours_under12.sum() + d.gap_hours.sum()
    hours_t = d.hours_under12.sum() + d.gap_hours_truncated_at_week.sum()
    real_gap_hours = d.real_gap_hours.sum()
    real_gap_hours_t = d.real_gap_hours_truncated_at_week.sum()
   

    if flag == "other_vessels":
        d = df_gap_counts[~df_gap_counts.flag.isin(flags)]
    elif flag == 'total':
        d = df_gap_counts
    else:
        d = df_gap_counts[(df_gap_counts.flag==flag)]  
    gaps = d.gaps.values.sum()
    
    
    gaps_per_day = gaps/hours*24


    
    if flag == "other_vessels":
        d = df_fishing[~df_fishing.flag.isin(flags)]
    elif flag == "total":
        d = df_fishing
    else:
        d = df_fishing[(df_fishing.flag==flag)]      
    d = d[d.is_high_seas]
    
    
    
    days_activity_1000 = round(hours/1000/24)
    low_perc_days_lost = int(round(real_gap_hours_t/hours_t*100) )
    high_perc_days_lost = int(round(real_gap_hours/hours*100)    )
    gap_days_low = round(real_gap_hours_t/24)
    gap_days_high = round(real_gap_hours/24)
    gaps_per_1000day = round(gaps_per_day*1000) 
    days_per_gap_low = round(real_gap_hours_t/gaps/24,1)
    days_per_gap_high = round(real_gap_hours/gaps/24,1)

    rows.append([flag.replace("_"," "),
                 days_activity_1000,
                 gaps,
                 gaps_per_1000day,
                 f"{real_gap_hours_t}-{real_gap_hours}",
                 f"{gap_days_low}-{gap_days_high}",
                 f"{low_perc_days_lost}-{high_perc_days_lost}%",
                 f"{days_per_gap_low}-{days_per_gap_high}"])

# +

headers = ['Flag',
          '1000s days\nof activity',
          'disabling\nevents',
          'high seas disabling events\nper 1000 days',
          'hours lost to\ndisabling events', 
          'days lost to disabling',
          'frac time lost to\ndisabling events',
          'avg length\nof disabling\nevent, days' ]

print(tabulate(rows, headers=headers))# '1000 fhours','g/fishingday*100']))

# +
# the following is to print the results and then copy and paste into a spreadsheet 

print("\t".join(map(lambda x: x.replace("\n"," "),headers)))
for row in rows:
    print("\t".join(list(map(str,row))))
# -
table_1_b = pd.DataFrame(rows, columns=['group','1000s_days_activity','disabling_events',
                                        'hs_disabling_events_per_1000_days',
                                        'hours_lost_to_disabling',
                                        'days_lost_to_disabling',
                                        'frac_time_lost_to_disabling','avg_length_disabling_event'])
table_1_b.to_csv('../data/gap_inputs_v{v}/table_1_time_lost_to_gaps_flag_v{v}.csv'.format(v=inputs_version))
