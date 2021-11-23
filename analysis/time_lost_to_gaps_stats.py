# -*- coding: utf-8 -*-
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
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # How Much Time is lost To Gaps?
#
# Calculate the total number of events and the amount of time that is lost to gaps in the high seas. We will calculate it using the raster method and using the interpolation method.

# %%
import pandas as pd

def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')


# %% [markdown]
# ## Interpolation method
#
# ### By vessel class

# %%
gaps_table = 'proj_ais_gaps_catena.ais_gap_events_features_v20210722'
activity_table = 'proj_ais_gaps_catena.fishing_activity_v20211109'
interpolate_table = 'proj_ais_gaps_catena.gaps_allocated_interpolate_v20211109'
raster_table = 'proj_ais_gaps_catena.gaps_allocated_raster_v20211109'

# %%
query_template = '''
WITH
-----------------------------------------------------------------------
-- Gridded activity by fishing vessels active beyond 50 nautical miles.
-- Note David renamed tuna_purse_seines to purse_seines in the 
-- fishing_activity_v table
-----------------------------------------------------------------------
fishing_vessel_activity AS(
    SELECT 
    IF(vessel_class IN ('trawlers','drifting_longlines','purse_seines','squid_jigger'), vessel_class, 'other_geartypes') as vessel_class,
    IF(flag IN ('CHN','TWN','ESP','KOR'), flag, 'other_flags') as flag,
    hours_in_gaps_under_12
    FROM `{activity_table}`
    WHERE over_50nm
),
--------------------------------------------------------------------------
-- Summarize activity by vessel class for fishing vessels on the high seas
--------------------------------------------------------------------------
fishing_vessel_activity_by_class AS (
SELECT 
ROUND(SUM(hours_in_gaps_under_12)) hours,
vessel_class as vessel_group
FROM fishing_vessel_activity
GROUP BY vessel_group
),
--------------------------------------------------------------------------
-- Summarize activity by vessel class for fishing vessels on the high seas
--------------------------------------------------------------------------
fishing_vessel_activity_by_flag AS (
SELECT 
ROUND(SUM(hours_in_gaps_under_12)) hours,
flag as vessel_group
FROM fishing_vessel_activity 
GROUP BY flag
),
--------------------------------------------------------------------------
-- Summarize activity for all fishing vessels on the high seas
--------------------------------------------------------------------------
total_fishing AS (
SELECT 
SUM(hours) hours,
"all vessels" as vessel_group
FROM fishing_vessel_activity_by_class
),
--------------------------------------------------------------------------
-- Combine activity by vessel class with total activity of fishing vessels
--------------------------------------------------------------------------
total_fishing_vessel_activity_by_class as (
SELECT * FROM fishing_vessel_activity_by_class 
UNION ALL
SELECT * FROM total_fishing
),
--------------------------------------------------------------------------
-- Get all gaps that start in the high seas
--------------------------------------------------------------------------
all_disabling_events AS (
    SELECT
        gap_id,
        -- adjust vessel classes to match aggregation David did in gridded activity tables...
        CASE 
            WHEN vessel_class IN ('trawlers','drifting_longlines','squid_jigger') THEN vessel_class
            WHEN vessel_class = 'tuna_purse_seines' THEN 'purse_seines'
            ELSE 'other_geartypes'
            END as vessel_class,
        -- IF(vessel_class IN ('trawlers','drifting_longlines','tuna_purse_seines','squid_jigger'), vessel_class, 'other_geartypes') as vessel_class,
        IF(flag IN ('CHN','TWN','ESP','KOR'), flag, 'other_flags') as flag,
        gap_hours
    FROM `{gaps_table}`
    WHERE gap_hours >= 12
    AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
    AND positions_X_hours_before_sat >= 19
    AND off_distance_from_shore_m > 1852 * 50
    AND on_distance_from_shore_m > 1852 * 50
    AND DATE(gap_start) >= '2017-01-01' 
    AND DATE(gap_end) <= '2019-12-31'
),
-------------------------------------------------------------------------------
-- Summarize the number of disabling events by vessel class and for all vessels
-------------------------------------------------------------------------------
gaps_summarized_by_class AS (
    SELECT 
    vessel_class as vessel_group,
    COUNT(*) as disabling_events
    FROM all_disabling_events 
    GROUP BY 1
    
    UNION ALL 
    
    SELECT 
    'all vessels' as vessel_group,
    COUNT(*) as disabling_events
    FROM all_disabling_events 
),
--------------------------------------------------------------------------
-- Summarize the number of disabling events by flag
--------------------------------------------------------------------------
gaps_summarized_by_flag AS (
    SELECT 
    flag as vessel_group,
    COUNT(*) as disabling_events
    FROM all_disabling_events 
    GROUP BY 1
),
--------------------------------------------------------------------------
-- Pull the gridded gaps
--------------------------------------------------------------------------
gaps AS (
    SELECT 
    * EXCEPT(vessel_class, flag),
    IF(vessel_class IN ('trawlers','drifting_longlines','purse_seines','squid_jigger'), vessel_class, 'other_geartypes') as vessel_class,
    IF(flag IN ('CHN','TWN','ESP','KOR'), flag, 'other_flags') as flag
    FROM {gridded_gap_table}
    WHERE over_50nm
),
--------------------------------------------------------------------------
-- Summarize the time spent in gaps for all vessels
--------------------------------------------------------------------------
total_gaps AS (
    select 
    sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
    sum(if(is_real_gap, gap_hours, 0)) / 24 real_gap_days,
    round(avg(if(is_real_gap, gap_hours / 24, NULL)), 2) avg_real_gap_days,
    sum(gap_hours) gap_hours,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) / 24 real_gap_days_2w,
    round(avg(if(is_real_gap and not over_two_weeks, gap_hours / 24, NULL)), 2) avg_real_gap_days_2w,
    sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
    sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
    sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
    "all vessels" as vessel_group
    from gaps
),
------------------------------------------------------------------------------------
-- Summarize the time spent in gaps by vessel class and join with all vessel summary
------------------------------------------------------------------------------------
gaps_by_class as (
    select 
    sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
    sum(if(is_real_gap, gap_hours, 0)) / 24 real_gap_days,
    round(avg(if(is_real_gap, gap_hours / 24, NULL)), 2) avg_real_gap_days,
    sum(gap_hours) gap_hours,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) / 24 real_gap_days_2w,
    round(avg(if(is_real_gap and not over_two_weeks, gap_hours / 24, NULL)), 2) avg_real_gap_days_2w,
    sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
    sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
    sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
    vessel_class as vessel_group
    from gaps 
    group by vessel_class
    union all
    select * from total_gaps
),
------------------------------------------------------------------------------------
-- Summarize the time spent in gaps by vessel class and join with all vessel summary
------------------------------------------------------------------------------------
gaps_by_flag as (
    select 
    sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
    sum(if(is_real_gap, gap_hours, 0)) / 24 real_gap_days,
    round(avg(if(is_real_gap, gap_hours / 24, NULL)), 2) avg_real_gap_days,
    sum(gap_hours) gap_hours,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) / 24 real_gap_days_2w,
    round(avg(if(is_real_gap and not over_two_weeks, gap_hours / 24, NULL)), 2) avg_real_gap_days_2w,
    sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
    sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
    sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
    flag as vessel_group
    from gaps
    group by flag
),
--------------------------------------------------------------------------
-- Calculate fraction of time in gaps by class
--------------------------------------------------------------------------
summary_by_class AS (
    select 
    *,
    -- hours here is time when the time betwee positions is less than 12 hours
    round(real_gap_hours/(gap_hours + hours),3) frac_gaps,
    round(real_gap_hours_2w/(gap_hours_2w + hours),3) frac_gaps_2w,
    round(real_gap_hours_4w/(gap_hours_4w + hours),3) frac_gaps_4w
    from gaps_by_class 
    join total_fishing_vessel_activity_by_class
    using(vessel_group)
    join gaps_summarized_by_class  
    using(vessel_group)
),
--------------------------------------------------------------------------
-- Calculate fraction of time in gaps by flag
--------------------------------------------------------------------------
summary_by_flag AS (
    select 
    *,
    -- hours here is time when the time betwee positions is less than 12 hours
    round(real_gap_hours/(gap_hours + hours),3) frac_gaps,
    round(real_gap_hours_2w/(gap_hours_2w + hours),3) frac_gaps_2w,
    round(real_gap_hours_4w/(gap_hours_4w + hours),3) frac_gaps_4w
    from gaps_by_flag 
    join fishing_vessel_activity_by_flag
    using(vessel_group)
    join gaps_summarized_by_flag  
    using(vessel_group)
)

SELECT * FROM summary_by_class 
UNION ALL 
SELECT * FROM summary_by_flag'''

# %%
q = query_template.format(activity_table=activity_table,
                          gaps_table=gaps_table,
                          gridded_gap_table=interpolate_table,
                         )
dfi = gbq(q)

# %%
print("Fraction of time lost by class - 2 week versus all time, using interpolation method")
for index,row in dfi.iterrows():
    print(f"{100*row.frac_gaps_2w:.1f}-{100*row.frac_gaps:.1f}% - {row.vessel_group}")

# %%
dfi[['vessel_group', 
    'disabling_events', 
    'real_gap_days_2w', 
    'real_gap_days', 
    'frac_gaps_2w', 
    'frac_gaps',
    'avg_real_gap_days_2w',
    'avg_real_gap_days']]

# %% [markdown]
# ## Now with raster method

# %%
q = query_template.format(activity_table=activity_table,
                          gaps_table=gaps_table,
                          gridded_gap_table=raster_table,
                         )
dfr = gbq(q)

# %%
print("Fraction of time lost by class - 2 week versus all time, using raster method")
for index,row in dfr.iterrows():
    print(f"{100*row.frac_gaps_2w:.1f}-{100*row.frac_gaps:.1f}% - {row.vessel_group}")

# %%
dfr[['vessel_group', 
    'disabling_events', 
    'real_gap_days_2w', 
    'real_gap_days', 
    'frac_gaps_2w', 
    'frac_gaps',
    'avg_real_gap_days_2w',
    'avg_real_gap_days']]

# %%
# what fraction of time lost to gaps is by non-other vessel classes?
other_2wf = (
    dfr[dfr.vessel_group == "other_geartypes"].real_gap_hours_2w.values[0]
    / dfr[dfr.vessel_group != "all vessels"].real_gap_hours_2w.sum()
)
other_4wf = (
    dfr[dfr.vessel_group == "other_geartypes"].real_gap_hours_4w.values[0]
    / dfr[dfr.vessel_group != "all vessels"].real_gap_hours_4w.sum()
)
other = (
    dfr[dfr.vessel_group == "other_geartypes"].real_gap_hours.values[0]
    / dfr[dfr.vessel_group != "all vessels"].real_gap_hours.sum()
)
print(other_2wf, other_4wf, other)

# %%
# what fraction of time lost to gaps is by non-other?
other_2wf = (
    dfr[dfr.vessel_group == "other_flags"].real_gap_hours_2w.values[0]
    / dfr[dfr.vessel_group != "all vessels"].real_gap_hours_2w.sum()
)
other_4wf = (
    dfr[dfr.vessel_group == "other_flags"].real_gap_hours_4w.values[0]
    / dfr[dfr.vessel_group != "all vessels"].real_gap_hours_4w.sum()
)
other = (
    dfr[dfr.vessel_group == "other_flags"].real_gap_hours.values[0]
    / dfr[dfr.vessel_group != "all vessels"].real_gap_hours.sum()
)
print(other_2wf, other_4wf, other)

# %%

# %% [markdown]
# ---
# ---
# ---

# %%
q = query_template.format(activity_table=activity_table,
                          gaps_table=gaps_table,
                          gridded_gap_table=interpolate_table,
                         )
dfi = gbq(q)

# %%
dfi[['vessel_group', 
    'disabling_events', 
    'real_gap_days_2w', 
    'real_gap_days', 
    'frac_gaps_2w', 
    'frac_gaps',
    'avg_real_gap_days_2w',
    'avg_real_gap_days']]

# %%

# %%
q = query_template.format(activity_table=activity_table,
                          gaps_table=gaps_table,
                          gridded_gap_table=raster_table,
                         )
dfr = gbq(q)

# %%
dfr[['vessel_group', 
    'disabling_events', 
    'real_gap_days_2w', 
    'real_gap_days', 
    'frac_gaps_2w', 
    'frac_gaps',
    'avg_real_gap_days_2w',
    'avg_real_gap_days']]

# %%
