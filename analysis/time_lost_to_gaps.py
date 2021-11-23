# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: rad
#     language: python
#     name: rad
# ---

# %% [markdown]
# # How Much Time is lost To Gaps?
#
# Calculate the total amount of time that is lost to gaps in the high seas. We will calculate it using the raster method and using the interpolation method.
#
#

# %%
import pandas as pd

def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')


# %%
query_template = '''
with 

good_enough_reception as 
(select lat_bin, lon_bin
from `world-fishing-827.proj_ais_gaps_catena.sat_reception_smoothed_one_degree_v20210722` 
where year = 2019
and month = 1 
and class = "A"
and positions_per_day > 5),


fishing_activity as
(
select 
sum(hours_in_gaps_under_12) hours,
vessel_class
from proj_ais_gaps_catena.fishing_activity_v20211109
join
good_enough_reception
on floor(lat_index) = lat_bin
and floor(lon_index) = lon_bin
where over_50nm
group by vessel_class),

total_fishing as (
select sum(hours) hours,
"all vessels" as vessel_class
from fishing_activity),

fishing_activity_by_class as 
(
select * from fishing_activity
union all
select * from total_fishing
),


total_gaps as 
(
select 
sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
sum(gap_hours) gap_hours,
sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
"all vessels" as vessel_class
from 
{gridded_gap_table}
join
good_enough_reception
on floor(lat_index) = lat_bin
and floor(lon_index) = lon_bin
where over_50nm
),

gaps_by_class as (
select 
sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
sum(gap_hours) gap_hours,
sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
vessel_class
from 
{gridded_gap_table}
where 
over_50nm 
group by vessel_class
union all
select * from total_gaps)


select *,
-- hours here is time when the time betwee positions is less
-- than 12 hours
real_gap_hours,
round(real_gap_hours/(gap_hours + hours),3) frac_gaps,
round(real_gap_hours_2w/(gap_hours_2w + hours),3) frac_gaps_2w,
round(real_gap_hours_4w/(gap_hours_4w + hours),3) frac_gaps_4w

from gaps_by_class 
join fishing_activity_by_class
using(vessel_class)
order by hours
'''

# %% [markdown]
# ## Interpolation

# %%
q = query_template.format(gridded_gap_table = 'proj_ais_gaps_catena.gaps_allocated_interpolate_v20211109')
dfi = gbq(q)

# %%
print("Fraction of time lost by class - 2 week versus all time, using interpolation method")
for index,row in dfi.iterrows():
    print(f"{100*row.frac_gaps_2w:.1f}-{100*row.frac_gaps:.1f}% - {row.vessel_class}")

# %% [markdown]
# ## Raster method

# %%
q = query_template.format(gridded_gap_table = 'proj_ais_gaps_catena.gaps_allocated_raster_v20211109')
dfr = gbq(q)

# %%
print("Fraction of time lost by class - 2 week versus all time, using raster method")
for index,row in dfr.iterrows():
    print(f"{100*row.frac_gaps_2w:.1f}-{100*row.frac_gaps:.1f}% - {row.vessel_class}")

# %%
