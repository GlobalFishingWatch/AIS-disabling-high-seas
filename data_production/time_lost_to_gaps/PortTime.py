# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # How likely is it that vessels are in port in long gaps?

# +
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import matplotlib as mpl
import pandas as pd
import seaborn as sns

# %matplotlib inline

mpl.rcParams["axes.spines.right"] = False
mpl.rcParams["axes.spines.top"] = False
plt.rcParams["figure.facecolor"] = "white"
plt.rcParams["axes.facecolor"] = "white"
plt.rc("legend", fontsize="12")
plt.rc("legend", frameon=False)


def gbq(q):
    return pd.read_gbq(q, project_id="world-fishing-827")
# +


q = '''with 

voyages as (
SELECT * FROM `world-fishing-827.pipe_production_v20201001.proto_voyages_c4` 
WHERE DATE(trip_end) between "2019-01-01" 
and "2019-12-31"
and DATE(trip_start) between "2017-01-01" and "2019-12-31"

),

high_seas_fishing_vessels as 
(
select 
ssvid,
year,
e.fishing_hours,
best.best_vessel_class as vessel_class
from `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210913` 
cross join unnest(activity.eez) e
where e.value is null
and on_fishing_list_best
and e.fishing_hours > 24*4
-- and year = 2019
and activity.overlap_hours_multinames = 0
),

voyages_hs_vessels as (select * from voyages
join
high_seas_fishing_vessels
using(ssvid)),

high_seas_fishing as 
(select ssvid, timestamp, hours, nnet_score, extract(year from _partitiontime) year
from `world-fishing-827.gfw_research.pipe_v20201001` 
where distance_from_shore_m > 1852*200
and date(_partitiontime) between "2017-01-01" and "2019-12-31"
)

select 
ssvid, trip_start, trip_end, vessel_class,
timestamp_diff(trip_end, trip_start, day) days,
sum(if(nnet_score>.5,hours,0)) fishing_high_seas, sum(hours) fishing
from 
voyages_hs_vessels
join
high_seas_fishing
using(ssvid,year)
where timestamp between trip_start and trip_end
group by ssvid, trip_start, trip_end,vessel_class
having fishing > 24
order by days
'''

df = gbq(q)
# -


sns.histplot(df.days)
plt.title(f"{df.days.mean():.0f} mean voyage, {df.days.median():.0f} median")

sns.histplot(df.days)
plt.title(f"{df.days.mean():.0f} mean voyage, {df.days.median():.0f} median")


df['days_cumsum'] = df.days.cumsum()/df.days.sum()
plt.plot(df.days, df.days_cumsum)

df[(df.days_cumsum>.4999)&(df.days_cumsum<.5001)]

# ### Take Away:
#  - the median voyage is four weeks, and the mean voyage is 44 days
#  - over half the time is spent in voyages over 67 days
#  



# +
# uncomment only if you want to see by vessel class

# for vessel_class in df.vessel_class.unique():
#     d = df[df.vessel_class==vessel_class]
#     sns.histplot(d.days)
#     plt.title(f"{vessel_class}, {d.days.mean():.0f} mean voyage, {d.days.median():.0f} median")
#     plt.show()
# -

# ## How much of the year are high seas vessels at port?
#
# This anlaysis was not used, but shows the average number of days vessels spend at sea that fish in the high seas

# +
q = '''
with gapiness as (SELECT 
  ssvid, 
  sum(gap_hours)/365/3/24 gap_hours_fraction
 -- for spatial allocation, require start or end to be larger than 50 nautical miles
 -- to avoid counting gaps that are in port  
 
 from `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
 left join
 `world-fishing-827.gfw_research.vi_ssvid_v20210301` 
 using(ssvid)
WHERE gap_hours >= 12
    AND (off_distance_from_shore_m > 1852*50 AND on_distance_from_shore_m > 1852*50)
    AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
    
    group by ssvid
    
    order by gap_hours_fraction desc)


select ssvid,
eez.fishing_hours fishing_hours,
best.best_vessel_class as vessel_class,
avg(activity.active_hours)/24 days,
avg(activity.active_hours)/24/365 frac_of_year
from `world-fishing-827.gfw_research.vi_ssvid_byyear_v20210301`
cross join unnest(activity.eez) as eez

where year = 2019
and eez.value is null
and eez.fishing_hours > 24*2 -- fished for at least two days in the high seas
and on_fishing_list_best
and activity.overlap_hours_multinames = 0 
and ssvid not in 
(select ssvid from gapiness where gap_hours_fraction > .05) -- gap fraction is under 5%
group by ssvid, vessel_class, fishing_hours
order by vessel_class, days '''

df = gbq(q)
# -

# how many unique vessels?
len(df)

d = df[(df.days<366)]
d.days.mean()/365

for v in ["drifting_longlines",
          "squid_jigger",
         "tuna_purse_seines",
         "trawlers"]:
    d = df[(df.vessel_class==v)&(df.days<366)]
    plt.hist(d.days)
    plt.title(f"{v}, {d.days.mean():.0f} days on average, {d.days.mean()/365:.2f} fraction")
    print(v)
    plt.show()

# ## Take Aways:
#  - About 2/3 of the time these vessels are at sea, and 1/3 in port
#  - But it is not nomrally distributed -- the most active vessels are at sea much more time
#  - I wouldn't necessarily use this to help understand how much vessels are in port




