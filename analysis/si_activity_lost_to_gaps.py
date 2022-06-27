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

from ais_disabling import config
from ais_disabling import utils

# %matplotlib inline
# %load_ext autoreload
# %autoreload 2

def gbq(q): 
    return pd.read_gbq(q, project_id="world-fishing-827")


# -

# Set top-level parameters for analysis.

# +
# What scale (degrees) for the analysis of time lost to disabling
scale = 1

# Vessel info version to use
vi_version = config.vi_version 

# Version of the gap model inputs to use. This should be the same for the gaps,
# vessel presence, loitering, and interpolated tables
inputs_version = config.output_version
# -

# # Gap Activity

# +
q = f'''
#standardSQL

WITH 

# Fishing vessels
all_vessel_info AS (
    SELECT * 
    FROM `{config.destination_dataset}.{config.fishing_vessels_table}`
),

real_gaps AS (
    SELECT 
        DISTINCT gap_id
    FROM 
    `{config.destination_dataset}.{config.gap_events_features_table}` 
    {config.gap_filters}
)

SELECT 
floor(a.lat*{scale}) lat_bin,
floor(a.lon*{scale}) lon_bin,
vessel_class,
flag,
b.distance_from_shore_m > 1852*200 is_high_seas,
d.gap_id is not null is_real_gap,
count(*) gap_hours,
sum(if(hours_to_nearest_ping<24*3.5,1,0)) gap_hours_truncated_at_week,
sum(if(gap_hours < 24,1,0)) gaps_under_24,
from {config.destination_dataset}.{config.gap_positions_hourly_table}  a
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
date(_partitiontime) between "{config.start_date}" and "{config.end_date}" -- 3 years of data
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
q = f'''
#standardSQL
WITH 

# Fishing vessels
all_vessel_info AS (
    SELECT * 
    FROM `{config.destination_dataset}.{config.fishing_vessels_table}`
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
  from {config.destination_dataset}.{config.ais_positions_hourly} a
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
  date(_partitiontime) between "{config.start_date}" and "{config.end_date}" -- 3 years of data
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

df_fishing = gbq(q)
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
# ## Time lost to gaps
#
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


# # As Distance from Shore

q = f'''
WITH
--
--
# Fishing vessels
all_vessel_info AS (
    SELECT * 
    FROM `{config.destination_dataset}.{config.fishing_vessels_table}`
),
--
--
real_gaps AS (
    SELECT 
        DISTINCT gap_id
    FROM 
    `{config.destination_dataset}.{config.gap_events_features_table}` 
    {config.gap_filters}
),
--
--
gaps_by_distance as (
select 
vessel_class,
floor(b.distance_from_shore_m/1000) distnace_from_shore_km,
count(*) gap_hours,
sum(if(hours_to_nearest_ping<24*3.5,1,0)) gap_hours_truncated_at_week,
sum(if(d.gap_id is not null,1,0)) real_gap_hours,
sum(if(d.gap_id is not null and hours_to_nearest_ping<24*3.5,1,0)) real_gap_hours_truncated_at_week,
-- sum(if(gap_hours < 24,1,0)) gaps_under_24,
from {config.destination_dataset}.{config.gap_positions_hourly_table}  a
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
from {config.destination_dataset}.{config.ais_positions_hourly} a
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

# # Distance from shore by country

q = f'''
WITH
--
--
# Fishing vessels
all_vessel_info AS (
    SELECT * 
    FROM `{config.destination_dataset}.{config.fishing_vessels_table}`
),
--
--
real_gaps AS (
    SELECT 
        DISTINCT gap_id
    FROM 
    `{config.destination_dataset}.{config.gap_events_features_table}` 
    {config.gap_filters}
),
--
--
gaps_by_distance as (
select 
flag,
floor(b.distance_from_shore_m/1000) distnace_from_shore_km,
count(*) gap_hours,
sum(if(d.gap_id is not null,1,0)) real_gap_hours,
sum(if(hours_to_nearest_ping<24*3.5,1,0)) gap_hours_truncated_at_week,
sum(if(d.gap_id is not null and hours_to_nearest_ping<24*3.5,1,0)) real_gap_hours_truncated_at_week,
-- sum(if(gap_hours < 24,1,0)) gaps_under_24,
from {config.destination_dataset}.{config.gap_positions_hourly_table}  a
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
from {config.destination_dataset}.{config.ais_positions_hourly} a
join
all_vessel_info c
on a.ssvid = c.ssvid
and year = extract(year from _partitiontime)
JOIN
  `world-fishing-827.pipe_static.distance_from_shore` b
ON
  cast( (a.lat*100) as int64) = cast( (b.lat*100) as int64)
  AND cast((a.lon*100) as int64)= cast(b.lon*100 as int64)
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

# # Make Tables

q = f'''with # Fishing vessels
--
--
# Fishing vessels
all_vessel_info AS (
    SELECT * 
    FROM `{config.destination_dataset}.{config.fishing_vessels_table}`
),

gaps_table as (

select 
gap_id,
gap_hours,
b.flag as flag,
b.vessel_class vessel_class,
row_number() over (partition by gap_id order by gap_hours, rand()) row
from 
{config.destination_dataset}.{config.gap_events_features_table} a 
left join 
all_vessel_info b
on(
    a.ssvid = b.ssvid
    AND EXTRACT(year from a.gap_start) = b.year
)
{config.gap_filters}
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

# +
# df_dist_shore.head()
# -

rows = []
vessel_classes =  ['drifting_longlines','squid_jigger','tuna_purse_seines','trawlers']
for vessel_class in vessel_classes + ['other_geartypes','total']:
    
    if vessel_class == "other_geartypes":
        d = df_dist_shore[~df_dist_shore.vessel_class.isin(vessel_classes)]
    elif vessel_class == 'total':
        d = df_dist_shore
    else:
        d = df_dist_shore[(df_dist_shore.vessel_class==vessel_class)]        
#     d = d[(d.distance_from_shore_nm>=200)]
    d = d[(d.distance_from_shore_nm>=50)]
    d = d.fillna(0)
    hours = d.hours_under12.sum()  + d.gap_hours.sum()
    hours_t = d.hours_under12.sum() + d.gap_hours_truncated_at_week.sum()
    real_gap_hours = d.real_gap_hours.sum()
    real_gap_hours_t = d.real_gap_hours_truncated_at_week.sum()
   

    if vessel_class == "other_geartypes":
        d = df_gap_counts[~df_gap_counts.vessel_class.isin(vessel_classes)]
    elif vessel_class == 'total':
        d = df_gap_counts
    else:
        d = df_gap_counts[(df_gap_counts.vessel_class==vessel_class)]  
    gaps = d.gaps.values.sum()
    
    
    gaps_per_day = gaps/hours*24


    
    if vessel_class == "other_geartypes":
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

# Save to CSV
table_1_a = pd.DataFrame(rows, columns=['group','1000s_days_activity','disabling_events',
                                        'hs_disabling_events_per_1000_days','hours_lost_to_disabling',
                                        'days_lost_to_disabling',
                                        'frac_time_lost_to_disabling','avg_length_disabling_event'])
table_1_a.head()

rows = []
flags =  ['CHN','TWN','ESP','KOR']
for flag in flags + ['other_flags','total']:
    
    if flag == "other_flags":
        d = df_dist_shore_f[~df_dist_shore_f.flag.isin(flags)]
    elif flag == 'total':
        d = df_dist_shore_f
    else:
        d = df_dist_shore_f[(df_dist_shore_f.flag==flag)]        
#     d = d[(d.distance_from_shore_nm>=200)]
    d = d[(d.distance_from_shore_nm>=50)]
    d = d.fillna(0)
    hours = d.hours_under12.sum() + d.gap_hours.sum()
    hours_t = d.hours_under12.sum() + d.gap_hours_truncated_at_week.sum()
    real_gap_hours = d.real_gap_hours.sum()
    real_gap_hours_t = d.real_gap_hours_truncated_at_week.sum()
   

    if flag == "other_flags":
        d = df_gap_counts[~df_gap_counts.flag.isin(flags)]
    elif flag == 'total':
        d = df_gap_counts
    else:
        d = df_gap_counts[(df_gap_counts.flag==flag)]  
    gaps = d.gaps.values.sum()
    
    
    gaps_per_day = gaps/hours*24


    
    if flag == "other_flags":
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
# Save to CSV
table_1_b = pd.DataFrame(rows, columns=['group','1000s_days_activity','disabling_events',
                                        'hs_disabling_events_per_1000_days',
                                        'hours_lost_to_disabling',
                                        'days_lost_to_disabling',
                                        'frac_time_lost_to_disabling','avg_length_disabling_event'])

table_1_b.head()
# -

# Combine tables into full Table 1

# Combine geartype and flag state tables
table_1 = pd.concat([table_1_a.iloc[0:5,[0,2,5,6,7]],table_1_b.iloc[:,[0,2,5,6,7]]], axis=0)
table_1.to_csv(f'../results/gap_inputs_{config.output_version}/table_1_time_lost_to_gaps_{config.output_version}.csv')


