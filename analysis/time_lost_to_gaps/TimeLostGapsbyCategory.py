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

# # Time Lost to Gaps
#
# How much time is lost to having AIS off on the high seas? The challenge, of course, is that we don't know where vessels are traveling.

#
# # How much time is lost by gap category?
#

# +
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import pandas as pd
import pyseas.maps as psm
import pyseas.contrib as psc
import pyseas.cm
# %matplotlib inline
from datetime import datetime, timedelta
import proplot as pplt
import warnings
warnings.filterwarnings("ignore")



def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')


# -

# # How Much Time is Lost by Distance?

# +
q = '''with gap_table as

(SELECT ssvid, 
  off_lat, off_lon, on_lat, on_lon, gap_hours,gap_distance_m/1000 as gap_distance_km,
  st_geogpoint(off_lon, off_lat) as gap_start_point,
  st_geogpoint(on_lon, on_lat) as gap_end_point,
   (positions_per_day_off > 5 AND positions_per_day_on > 5)
    AND positions_X_hours_before_sat >= 19 is_real_gap,
    gap_hours/24 > 14 over_two_weeks
 -- for spatial allocation, require start or end to be larger than 50 nautical miles
 -- to avoid counting gaps that are in port  
 
 from `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
WHERE gap_hours >= 12
    AND (off_distance_from_shore_m > 1852*50 AND on_distance_from_shore_m > 1852*50)
    AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
 

 )
 
 
 select floor(gap_distance_km) gap_distance_km, sum(gap_hours)/24 gap_days from gap_table 
 group by gap_distance_km order by gap_distance_km'''

dfd = gbq(q)
# -

dfd['gap_days_cumsum'] = dfd.gap_days.cumsum()

plt.plot(dfd.gap_distance_km, dfd.gap_days_cumsum/dfd.gap_days.sum())
plt.xlim(0,2000)
plt.title("fraction of gap hours lost as function of gap distance")
plt.xlabel("gap distance, km")
plt.ylabel("fraction of time in gaps below this distance")





# # How much by day?

# +
q = '''create temp function map_label(label string) 
as (
  case when label ="drifting_longlines" then "drifting_longlines"
  when label ="purse_seines" then "purse_seines"
  when label ="other_purse_seines" then "purse_seines"
  when label ="tuna_purse_seines" then "purse_seines"
  when label ="cargo_or_tanker" then "cargo_or_tanker"
  when label ="cargo" then "cargo_or_tanker"
  when label ="tanker" then "cargo_or_tanker"
  when label ="squid_jigger" then "squid_jigger"
  when label ="tug" then "tug"
  when label = "trawlers" then "trawlers"
  else "other" end
);

with vessel_info as 
(
select map_label(best.best_vessel_class) vessel_class, ssvid
from `world-fishing-827.gfw_research.vi_ssvid_v20210913` 
where on_fishing_list_best and activity.overlap_hours_multinames <24
),

real_gaps as 
(
select * from `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
WHERE gap_hours >= 12
    AND (off_distance_from_shore_m > 1852*50 AND on_distance_from_shore_m > 1852*50)
    and ((off_distance_from_shore_m > 1852*200 or on_distance_from_shore_m > 1852*200))
    AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
    AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
    AND positions_X_hours_before_sat >= 19
)



SELECT 
b.vessel_class,
round(gap_hours/24) gap_days,
count(*) number,
sum(gap_hours/24) days_of_gaps
FROM real_gaps a
join
vessel_info b
using(ssvid)    
group by vessel_class, gap_days
order by vessel_class, gap_days'''

df = gbq(q)
# -

df.head()



for v in df.vessel_class.unique():

    d = df[df.vessel_class == v]
#     d['days_of_gaps_sum'] = d.days_of_gaps.cumsum()
    d['number_sum'] = d.number.cumsum()
    plt.plot(d.gap_days, d.number_sum/d.number.sum(), label = v)
plt.legend()
plt.xlim(0,60)
plt.ylim(0,1.03)
plt.title("Fraction of Gaps Below a Given Number of Days")
plt.xlabel("Days of Gap")

# +
vessel_classes = ['drifting_longlines', 'purse_seines', 'squid_jigger','trawlers']

rows = []
for i in range(1,30):
    row = [i] + [1-round(df[(df.vessel_class == v) & \
                    (df.gap_days >=i)].number.sum()\
                 /df[(df.vessel_class == v)].number.sum(),2) for v in vessel_classes] +\
         [round(1-df[df.gap_days >=i].number.sum()/df.number.sum(),2)]
    rows.append(row)
# -

from tabulate import tabulate
print("FRACTION OF GAPS STARTING OR ENDING ON HIGH SEAS UNDER A GIVEN DISTANCE")
print(tabulate(rows,headers=['days']+vessel_classes+["all"]))

# +
vessel_classes = ['drifting_longlines', 'purse_seines', 'squid_jigger','trawlers']

rows = []
for i in range(1,30):
    row = [i] + [1-round(df[(df.vessel_class == v) & \
                    (df.gap_days >=i)].days_of_gaps.sum()\
                 /df[(df.vessel_class == v)].days_of_gaps.sum(),2) for v in vessel_classes] +\
         [round(1-df[df.gap_days >=i].days_of_gaps.sum()/df.days_of_gaps.sum(),2)]
    rows.append(row)
# -

from tabulate import tabulate
print("FRACTION OF TIME IN GAPS STARTING OR ENDING ON HIGH SEAS UNDER A GIVEN DISTANCE")
print(tabulate(rows,headers=['days']+vessel_classes+["all"]))






