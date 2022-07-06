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

# # Loitering and Gaps
#
# Are gaps closer to loitering activity than fishing activity?

# +
import pandas as pd
import os
import pyseas
import pyseas.maps.rasters
import pyseas.styles
import pyseas.cm
import pandas as pd
# %matplotlib inline
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors,colorbar
# import cartopy
# from cartopy import config
# import cartopy.crs as ccrs
# import imageio
# import io

from ais_disabling import config

def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')


# +
# Vessel info version to use
vi_version = config.vi_version 

# Version of the gap model inputs to use. This should be the same for the gaps,
# vessel presence, loitering, and interpolated tables
inputs_version = config.output_version

# +
q = f'''# Fishing vessels
WITH
--
--
all_vessel_info AS (
  SELECT * 
  FROM `{config.destination_dataset}.{config.fishing_vessels_table}`
),
--
-- Interpolated positions
interpolated AS (
  SELECT 
    *,
    EXTRACT(year from _partitiontime) as year
  FROM `{config.destination_dataset}.{config.ais_positions_hourly}`
),
--
--
fishing_vessel_locations AS (
  SELECT 
    a.ssvid,
    hour_midpoint,
    a.lat lat, 
    a.lon lon,
    CASE 
      WHEN vessel_class = 'squid_jigger' AND night_loitering > 0.5 THEN True
      WHEN vessel_class != 'squid_jigger' AND nnet_score > 0.5 THEN True
      ELSE False 
      END as is_fishing,
    vessel_class
  FROM interpolated a
  JOIN all_vessel_info b
  USING(ssvid, year)
  JOIN `world-fishing-827.pipe_static.distance_from_shore` c
      ON cast( (a.lat*100) as int64) = cast( (c.lat*100) as int64)
      AND cast((a.lon*100) as int64) =cast(c.lon*100 as int64)
  WHERE c.distance_from_shore_m > 1852 * {config.min_distance_from_shore_m}
),
--
--
dist_to_loitering as (
  SELECT
    a.ssvid ssvid,
    hour_midpoint,
    vessel_class,
    is_fishing,
    st_distance(st_geogpoint(a.lon,a.lat), 
                st_geogpoint(b.lon,b.lat)
                )/1852 as nm_to_loitering
  FROM fishing_vessel_locations a
  LEFT JOIN `{config.destination_dataset}.{config.loitering_positions_hourly_table}` b
  USING( hour_midpoint )
),
--
--
with_row_number AS (
  SELECT 
    *, 
    row_number() over (
        partition by ssvid, hour_midpoint 
        order by nm_to_loitering, rand()
        ) as row
from dist_to_loitering 
)
--
--
SELECT
    floor(nm_to_loitering) as nm_to_loitering,
    is_fishing,
    vessel_class,
    count(*) number
FROM with_row_number
WHERE row = 1
GROUP BY nm_to_loitering, is_fishing, vessel_class
ORDER BY nm_to_loitering

'''

# print(q)
df_fishing = pd.read_gbq(q, project_id = "world-fishing-827")
# -

q = f'''# Fishing vessels
WITH
--
--
all_vessel_info AS (
  SELECT * 
  FROM `{config.destination_dataset}.{config.fishing_vessels_table}`
),
--
--
real_gaps as (
select gap_id 
FROM `{config.destination_dataset}.{config.gap_events_features_table}`
{config.gap_filters}
),
--
--
gap_locations as (
SELECT 
    a.ssvid,
    gap_id,
    hour_midpoint,
    a.lat lat, 
    a.lon lon,
    off_lat,
    off_lon,
    on_lat,
    on_lon,
    vessel_class,
    gap_start,
    gap_end
FROM `{config.destination_dataset}.{config.gap_positions_hourly_table}`  a
join real_gaps 
using(gap_id)
join all_vessel_info b
on a.ssvid = b.ssvid
and b.year = extract(year from _partitiontime)
JOIN
  `world-fishing-827.pipe_static.distance_from_shore` c
ON
  cast( (a.lat*100) as int64) = cast( (c.lat*100) as int64)
  AND cast((a.lon*100) as int64) =cast(c.lon*100 as int64)
where c.distance_from_shore_m > 1852*{config.min_distance_from_shore_m}
),
--
--
dist_to_loitering as (
  SELECT 
    gap_id,
    a.ssvid ssvid,
    hour_midpoint,
    vessel_class,
    st_distance(st_geogpoint(a.lon,a.lat), 
              st_geogpoint(b.lon,b.lat))/1852 
              nm_to_loitering,
    b.lat as loit_lat,
    b.lon as loit_lon,
    a.lat lat,
    a.lon lon,
    off_lat,
    off_lon,
    on_lat,
    on_lon,
    gap_start,
    gap_end
FROM gap_locations a
LEFT JOIN
`{config.destination_dataset}.{config.loitering_positions_hourly_table}` b
USING( hour_midpoint )
),
--
--
with_row_number as (
  select 
    *, 
    row_number() over (
        partition by gap_id 
        order by nm_to_loitering, rand()
        ) as row
FROM dist_to_loitering 
)
--
--
SELECT
    * except(row),
    st_distance(st_geogpoint(loit_lon, loit_lat), st_geogpoint(off_lon, off_lat))/1852 nm_to_gap_start,
     timestamp_diff(hour_midpoint, gap_start, second) / 3600 as hours_to_gap_start,

safe_divide(st_distance(st_geogpoint(loit_lon, loit_lat), st_geogpoint(off_lon, off_lat))/1852 
           , ( timestamp_diff(hour_midpoint, gap_start, second) / 3600) ) as knots_to_gap_start,

st_distance(st_geogpoint(loit_lon, loit_lat), st_geogpoint(on_lon, on_lat))/1852 nm_to_gap_end,
 timestamp_diff(gap_end,hour_midpoint, second) / 3600 as hours_to_gap_end,
 
safe_divide(st_distance(st_geogpoint(loit_lon, loit_lat), st_geogpoint(on_lon, on_lat))/1852 
            , ( timestamp_diff(gap_end,hour_midpoint, second) / 3600)) as knots_to_gap_end,
FROM with_row_number
WHERE row = 1
'''
# print(q)
df_loit = gbq(q)

df_loit.head()

# +
x = [i for i in range(100)]

for vessel_class in ['drifting_longlines','squid_jigger','tuna_purse_seines','trawlers']:
    
    d = df_loit
    d = d[(d.vessel_class==vessel_class)]
    num_gaps = len(d)
    y = [len(d[d.nm_to_loitering<=i])/num_gaps for i in x] 
    plt.plot(np.array(x),y, label = "suspected\ndisabling")
    

    d = df_fishing
    d = d[(d.vessel_class==vessel_class)]
    tot_activity = d.number.sum()
    y2 = [d[d.nm_to_loitering<=i].number.sum()/tot_activity for i in x] 
    plt.plot(np.array(x),y2, label = "presence")
    
    d = d[(d.vessel_class==vessel_class)&(d.is_fishing)]
    tot_fishing = d.number.sum()
    y3 = [d[d.nm_to_loitering<=i].number.sum()/tot_fishing for i in x] 
    plt.plot(np.array(x),y3, label = "fishing")
    
    plt.ylim(0,1)
    plt.legend()
#     plt.title(f"Fraction of AIS disabling events by {vessel_class}\nwithin given distance from loitering events")
    plt.xlabel("Distance (nautical miles)")
    plt.ylabel("Fraction")

    if vessel_class == 'squid_jigger':
        plt.savefig(f'../results/gap_figures_{config.output_version}/figure_si_gaps_near_loitering_squid_jigger.png', dpi=200,bbox_inches='tight')
    
    print(vessel_class)
    plt.show()
# -

for vessel_class in ['drifting_longlines','squid_jigger','tuna_purse_seines','trawlers']:
    d = df_loit
    d = d[(d.vessel_class==vessel_class)]
    x = [i for i in range(20)]
    y = [len(d[(d.knots_to_gap_end<i) & (d.knots_to_gap_start<i)])/len(d) for i in x]
    plt.title("fraction of gaps that could reach closest loitering under given speed")
    plt.plot(x,y, label = vessel_class)
    plt.xlabel("knots")
plt.legend()


