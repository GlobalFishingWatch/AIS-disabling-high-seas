# -*- coding: utf-8 -*-
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

# # How Much Time is lost To Gaps?
#
# This notebook calculates the statistics on the time lost to AIS disabling in Welch et al. (2022). It produced tables 1 and S3 in the paper, as well as calculating the fraction of time lost to disabling among the top flag states, geartypes, and in disabling hotspots. It also calculates the fraction of fishing vessels in the study area with disabling events.
#
# Calculate the total number of events and the amount of time that is lost to gaps in the high seas. We will calculate it using the raster method and using the interpolation method.

# +
import pandas as pd
import numpy as np

from ais_disabling import config

# %matplotlib inline
# %load_ext autoreload
# %autoreload 2

def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')


# -

# ## Time lost to gaps query

query_template = '''
WITH
--
-- Get areas above reception threshold
--
good_enough_reception as (
    SELECT
    lat_bin,
    lon_bin
    FROM `{d}.{sat_table}`
    WHERE class = "A"
    GROUP BY lat_bin, lon_bin
    HAVING AVG(positions_per_day) > {min_positions}
),
--
-- Gridded activity by fishing vessels active beyond 50 nautical miles. Note David renamed tuna_purse_seines
-- to purse_seines in the fishing_activity table
--
fishing_activity AS(
    SELECT
    IF(vessel_class = 'other', 'other_geartypes', vessel_class) as vessel_class,
    IF(flag IN ('CHN','TWN','ESP','USA'), flag, 'other_flags') as flag,
    hours_in_gaps_under_12
    FROM `{d}.{gridded_fishing_table}`
    JOIN good_enough_reception
    ON floor(lat_index) = lat_bin
    AND floor(lon_index) = lon_bin
),
--
-- Summarize activity by vessel class for fishing vessels on the high seas
--
fishing_activity_by_class AS (
    SELECT
    ROUND(SUM(hours_in_gaps_under_12)) hours,
    vessel_class as vessel_group
    FROM fishing_activity
    GROUP BY vessel_group
),
--
-- Summarize activity by vessel class for fishing vessels on the high seas
--
fishing_activity_by_flag AS (
    SELECT
    ROUND(SUM(hours_in_gaps_under_12)) hours,
    flag as vessel_group
    FROM fishing_activity
    GROUP BY flag
),
--
-- Summarize activity for all fishing vessels on the high seas
--
total_fishing AS (
    SELECT
    SUM(hours) hours,
    "all vessels" as vessel_group
    FROM fishing_activity_by_class
),
--
-- Combine activity by vessel class with total activity of fishing vessels
--
total_fishing_activity_by_class as (
    SELECT * FROM fishing_activity_by_class
    UNION ALL
    SELECT * FROM total_fishing
),
--
-- Get all disabling events that start in the study area
--
all_disabling_events AS (
    SELECT
        gap_id,    
        IF(vessel_class = 'other', 'other_geartypes', vessel_class) as vessel_class,
        IF(flag IN ('CHN','TWN','ESP','USA'), flag, 'other_flags') as flag,
        gap_hours,
        -- Flag disabling events over certain durations to calculate average duration later
        gap_hours > 14*24 over_two_weeks,
        gap_hours > 7*24*4 over_four_weeks,
    FROM `{d}.{gap_table}`
    {gap_filters}
),
--
-- Summarize the number of disabling events by vessel class and for all vessels
--
gaps_summarized_by_class AS (
    SELECT
    vessel_class as vessel_group,
    COUNT(*) as disabling_events,
    SUM(IF(NOT over_two_weeks, 1, 0)) disabling_events_2w,
    SUM(IF(NOT over_four_weeks, 1, 0)) disabling_events_4w
    FROM all_disabling_events
    GROUP BY 1

    UNION ALL

    SELECT
    'all vessels' as vessel_group,
    COUNT(*) as disabling_events,
    SUM(IF(NOT over_two_weeks, 1, 0)) disabling_events_2w,
    SUM(IF(NOT over_four_weeks, 1, 0)) disabling_events_4w
    FROM all_disabling_events
),
--
-- Summarize the number of disabling events by flag
--
gaps_summarized_by_flag AS (
    SELECT
    flag as vessel_group,
    COUNT(*) as disabling_events,
    SUM(IF(NOT over_two_weeks, 1, 0)) disabling_events_2w,
    SUM(IF(NOT over_four_weeks, 1, 0)) disabling_events_4w
    FROM all_disabling_events
    GROUP BY 1
),
--
--
--
gaps AS (
    SELECT
    * EXCEPT(vessel_class, flag),
    IF(vessel_class = 'other', 'other_geartypes', vessel_class) as vessel_class,
    IF(flag IN ('CHN','TWN','ESP','USA'), flag, 'other_flags') as flag
    FROM `{d}.{gridded_gap_table}`
    JOIN good_enough_reception
    ON floor(lat_index) = lat_bin
    AND floor(lon_index) = lon_bin
    WHERE over_50_nm
),
--
-- Summarize the time spent in gaps for all vessels
--
total_gaps AS (
    select
    sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
    round(sum(if(is_real_gap, gap_hours, 0)) / 24) real_gap_days,
    sum(gap_hours) gap_hours,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
    round(sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) / 24) real_gap_days_2w,
    sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
    sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
    sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
    "all vessels" as vessel_group
    from gaps
),
--
-- Summarize the time spent in gaps by vessel class and join with all vessel summary
--
gaps_by_class as (
    select
    sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
    round(sum(if(is_real_gap, gap_hours, 0)) / 24) real_gap_days,
    sum(gap_hours) gap_hours,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
    round(sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) / 24) real_gap_days_2w,
    sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
    sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
    sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
    vessel_class as vessel_group
    from gaps
    group by vessel_class
    union all
    select * from total_gaps
),
--
-- Summarize the time spent in gaps by vessel class and join with all vessel summary
--
gaps_by_flag as (
    select
    sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
    round(sum(if(is_real_gap, gap_hours, 0)) / 24) real_gap_days,
    sum(gap_hours) gap_hours,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
    round(sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) / 24) real_gap_days_2w,
    sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
    sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
    sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
    flag as vessel_group
    from gaps
    group by flag
),
--
-- Calculate fraction of time in gaps by class
--
summary_by_class AS (
    select
    *,
    -- hours here is time when the time betwee positions is less than 12 hours
    round(real_gap_hours/(gap_hours + hours),3) frac_gaps,
    round(real_gap_hours_2w/(gap_hours_2w + hours),3) frac_gaps_2w,
    round(real_gap_hours_4w/(gap_hours_4w + hours),3) frac_gaps_4w,
    -- average gap days
    round(real_gap_hours_2w / gaps_summarized_by_class.disabling_events_2w / 24, 2) as avg_gap_days_2w,
    round(real_gap_hours / gaps_summarized_by_class.disabling_events / 24, 2) as avg_gap_days
    from gaps_by_class
    join total_fishing_activity_by_class
    using(vessel_group)
    join gaps_summarized_by_class
    using(vessel_group)
),
--
-- Calculate fraction of time in gaps by flag
--
summary_by_flag AS (
    select
    *,
    -- hours here is time when the time betwee positions is less than 12 hours
    round(real_gap_hours/(gap_hours + hours),3) frac_gaps,
    round(real_gap_hours_2w/(gap_hours_2w + hours),3) frac_gaps_2w,
    round(real_gap_hours_4w/(gap_hours_4w + hours),3) frac_gaps_4w,
    -- average gap days, where gap hours are the subset of the disabling time spent >50nm
    round(real_gap_hours_2w / gaps_summarized_by_flag.disabling_events_2w / 24, 2) as avg_gap_days_2w,
    round(real_gap_hours / gaps_summarized_by_flag.disabling_events / 24, 2) as avg_gap_days
    from gaps_by_flag
    join fishing_activity_by_flag
    using(vessel_group)
    join gaps_summarized_by_flag
    using(vessel_group)
)

SELECT * FROM summary_by_class
UNION ALL
SELECT * FROM summary_by_flag
'''

# ## Interpolation method results

q = query_template.format(
    d = config.destination_dataset,
    sat_table = config.sat_reception_smoothed,
    min_positions = config.min_positions_per_day,
    gap_table = config.gap_events_features_table,
    gap_filters = config.gap_filters,
    gridded_fishing_table = config.fishing_allocated_table,
    gridded_gap_table = config.gaps_allocated_interpolate_table
 )
print(q)
# dfi = gbq(q)

print("Fraction of time lost by class - 2 week versus all time, using interpolation method")
for index,row in dfi.iterrows():
    print(f"{100*row.frac_gaps_2w:.1f}-{100*row.frac_gaps:.1f}% - {row.vessel_group}")

# ### Save Table 1

# +
gear_groups = ['drifting_longlines','squid_jigger','tuna_purse_seines','trawlers','other_geartypes']
flag_groups = ['CHN','TWN','ESP','USA','other_flags']
vessel_groups = gear_groups + flag_groups + ['all vessels']

rows = []
for vessel_group in vessel_groups:
    r = dfi[dfi['vessel_group'] == vessel_group]
    rows.append([
        vessel_group.replace("_", " "),
        r.disabling_events.sum(),
        f"{round(r.real_gap_days_2w.sum())} - {round(r.real_gap_days.sum())}",
        f"{round(r.frac_gaps_2w.sum()*100, 1)} - {round(r.frac_gaps.sum()*100, 1)}%",
        f"{round(r.avg_gap_days_2w.sum(), 1)} - {round(r.avg_gap_days.sum(), 1)}",
    ])

table_1 = pd.DataFrame(rows, 
                       columns = [
                         'Vessel group',
                         'AIS disabling events',
                         'Time (days) lost to AIS disabling events',
                         'Fraction of time lost to AIS disabling events',
                         'Average duration (days)'
                         ])

# Reorder table
table_1.set_index('Vessel group', inplace = True)

table_1.reindex([
    'drifting longlines',
    'squid jigger',
    'tuna purse seines',
    'trawlers',
    'other geartypes',
    'CHN',
    'TWN',
    'ESP',
    'KOR',
    'other flags',
    'all vessels'
])

# Save to CSV
# table_1
table_1.to_csv(f'../../results/gap_inputs_{config.output_version}/table_1_time_lost_to_gaps_{config.output_version}.csv')
# -

# Calculate the percent of disabling events by top flags and geartypes.

# +
top_flag_events = dfi[dfi['vessel_group'].isin(['USA','ESP','CHN','TWN'])]['disabling_events'].sum()
all_events = dfi[dfi['vessel_group'] == 'all vessels']['disabling_events'].sum()

print(f"The top flag states (USA, ESP, CHN, and TWN) represent {round(top_flag_events / all_events * 100, 1)}% of disabling events")

# +
top_gear_events = dfi[dfi['vessel_group'].isin(['drifting_longlines',
                                                'tuna_purse_seines',
                                                'trawlers',
                                                'squid_jigger'])]['disabling_events'].sum()

print(f"The top geartypes represent {round(top_gear_events / all_events * 100, 1)}% of disabling events")
# -

# ## Raster method results

q = query_template.format(
    d = config.destination_dataset,
    sat_table = config.sat_reception_smoothed,
    min_positions = config.min_positions_per_day,
    gap_table = config.gap_events_features_table,
    gap_filters = config.gap_filters,
    gridded_fishing_table = config.fishing_allocated_table,
    gridded_gap_table = config.gaps_allocated_raster_table
 )
dfr = gbq(q)
# print(q)

print("Fraction of time lost by class - 2 week versus all time, using raster method")
for index,row in dfr.iterrows():
    print(f"{100*row.frac_gaps_2w:.1f}-{100*row.frac_gaps:.1f}% - {row.vessel_group}")

# ### Save Table S3
#
# Produce the supplementary table comparing the interpolation vs raster methods.

# +
rows = []
for vessel_group in vessel_groups:
    i = dfi[dfi['vessel_group'] == vessel_group]
    r = dfr[dfr['vessel_group'] == vessel_group]
    rows.append([
        vessel_group.replace("_", " "),
        r.disabling_events.sum(),
        f"{round(i.real_gap_days.sum())} ({round(r.real_gap_days.sum())})",
        f"{round(i.frac_gaps.sum()*100, 1)} ({round(r.frac_gaps.sum()*100, 1)})",
        f"{round(i.avg_gap_days.sum(), 1)} ({round(r.avg_gap_days.sum(), 1)})",
    ])

table_s = pd.DataFrame(rows, 
                       columns = [
                         'Vessel group',
                         'AIS disabling events',
                         'Time (days) lost to AIS disabling events',
                         'Fraction of time lost to AIS disabling events',
                         'Average duration (days)'
                         ])

# Reorder table
table_s.set_index('Vessel group', inplace = True)

table_s.reindex([
    'drifting longlines',
    'squid jigger',
    'tuna purse seines',
    'trawlers',
    'other geartypes',
    'CHN',
    'TWN',
    'ESP',
    'USA',
    'other flags',
    'all vessels'
])

# Save to CSV
table_s.to_csv(f'../../results/gap_inputs_{config.output_version}/table_s3_interp_vs_raster_{config.output_version}.csv')
# -

# ### Calculate fraction of time by other geartypes and flag states

# +
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

# print(other_2wf, other_4wf, other)
print(f"Other geartypes made up {round(other_2wf*100, 1)}-{round(other*100, 1)}% time lost to disabling")
print(f"Main geartypes made up {round((1-other_2wf)*100, 1)}-{round((1-other)*100, 1)}% time lost to disabling")
# -

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
# print(other_2wf, other_4wf, other)
print(f"Other flags made up {round(other_2wf*100, 1)}-{round(other*100, 1)}% time lost to disabling")
print(f"Main flags made up {round((1-other_2wf)*100, 1)}-{round((1-other)*100, 1)}% time lost to disabling")

# ## Time lost in areas of interest
#
# Figure 1 of the paper highlights the following three areas with a high fraction of time lost to AIS disabling:
#
# Coordinates ordered as (lon_min, lon_max, lat_min, lat_max)
#
# **Argentine EEZ**: bbox_ARG = (-65, -55, -50, -35)
#
# **NW pacific**: bbox_NWP = (143, 175, 38, 52)
#
# **West Africa**: bbox_WA = (-23, 15, -8, 23)

query_template = '''
WITH
--
-- Get areas above reception threshold
--
good_enough_reception as (
    SELECT
    lat_bin,
    lon_bin
    FROM `{d}.{sat_table}`
    WHERE class = "A"
    GROUP BY lat_bin, lon_bin
    HAVING AVG(positions_per_day) > {min_positions}
),
--
-- Gridded activity by fishing vessels active beyond 50 nautical miles. Note: fishing activity
-- table is already filtered to >50nm
--
fishing_activity AS(
    SELECT
    lat_bin,
    lon_bin,
    hours_in_gaps_under_12
    FROM {d}.{gridded_fishing_table}
    JOIN good_enough_reception
    ON floor(lat_index) = lat_bin
    AND floor(lon_index) = lon_bin
    #WHERE over_50nm # fishing table now filters to 50nm
),
--
-- Label cells in the bounding box AOIS
--
fishing_activity_aoi AS (
    SELECT
    *,
    CASE
        -- Argentina
        WHEN (
            lat_bin BETWEEN -50 AND -35
            AND lon_bin BETWEEN -65 AND -55
        ) THEN 'ARG'
        -- NW Pacific
        WHEN (
            lat_bin BETWEEN 38 AND 52
            AND lon_bin BETWEEN 143 AND 175
        ) THEN 'NWPACIFIC'
        -- West Africa
        WHEN (
            lat_bin BETWEEN -8 AND 23
            AND lon_bin BETWEEN -23 AND 15
        ) THEN 'WESTAFRICA'
        -- Alaska
        WHEN (
            lat_bin BETWEEN 50 AND 70
            AND lon_bin BETWEEN -180 AND -150
        ) THEN 'ALASKA'
        -- West Pacific
        WHEN (
          lat_bin BETWEEN  -5.3 and 8.2
          AND lon_bin BETWEEN 139.5 and 180 
        ) THEN 'WEST_PACIFIC'
        -- Indian ocean
        WHEN (
          lat_bin BETWEEN   -10.8 and 9.9
          AND lon_bin BETWEEN 42.3 and 68.9 
        ) THEN 'INDIAN'
        ELSE 'OTHER'
        END as region
    FROM fishing_activity
),
--
-- Summarize activity for all fishing vessels on the high seas
--
total_fishing AS (
    SELECT
    region,
    ROUND(SUM(hours_in_gaps_under_12)) hours
    FROM fishing_activity_aoi
    GROUP BY 1
),
--
-- Get all gaps in good enough reception and >50 nm from shore
--
gaps AS (
    SELECT
    *
    FROM {d}.{gridded_gaps_table}
    JOIN good_enough_reception
    ON floor(lat_index) = lat_bin
    AND floor(lon_index) = lon_bin
    WHERE over_50_nm
),
--
-- Assign gaps to AOIs
--
gaps_aoi AS (
    SELECT
    *,
    CASE
    -- Argentina
    WHEN (
        lat_bin BETWEEN -50 AND -35
        AND lon_bin BETWEEN -65 AND -55
    ) THEN 'ARG'
    -- NW Pacific
    WHEN (
        lat_bin BETWEEN 38 AND 52
        AND lon_bin BETWEEN 143 AND 175
    ) THEN 'NWPACIFIC'
    -- West Africa
    WHEN (
        lat_bin BETWEEN -8 AND 23
        AND lon_bin BETWEEN -23 AND 15
    ) THEN 'WESTAFRICA'
    -- Alaska
    WHEN (
        lat_bin BETWEEN 50 AND 70
        AND lon_bin BETWEEN -180 AND -150
    ) THEN 'ALASKA'
    -- West Pacific
    WHEN (
      lat_bin BETWEEN  -5.3 and 8.2
      AND lon_bin BETWEEN 139.5 and 180 
    ) THEN 'WEST_PACIFIC'
    -- Indian ocean
    WHEN (
      lat_bin BETWEEN   -10.8 and 9.9
      AND lon_bin BETWEEN 42.3 and 68.9 
    ) THEN 'INDIAN'
    ELSE 'OTHER'
    END as region
    FROM gaps
),
--
-- Summarize the time spent in gaps for all vessels
--
total_gaps AS (
    select
    region,
    sum(gap_hours) gap_hours,
    sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
    sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
    sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w
    from gaps_aoi
    GROUP BY 1
),
--
-- Calculate total time and fraction of time lost to gaps by region
--
frac_by_aoi AS (
    SELECT
    region,
    -- Calculate fraction of time lost to gaps in each region
    -- and the fraction of all time lost to gaps
    round(real_gap_hours_2w/(gap_hours_2w + hours),3) frac_gaps_2w,
    round(real_gap_hours_2w/total_real_gap_hours_2w,3) frac_all_gaps_2w,
    round(real_gap_hours/(gap_hours + hours),3) frac_gaps,
    round(real_gap_hours/total_real_gap_hours,3) frac_all_gaps,
    FROM (
        SELECT
        *,
        -- Summarize gap hours and real gap hours across all regions
        SUM(hours) OVER () as total_hours,
        SUM(gap_hours_2w) OVER () as total_gap_hours_2w,
        SUM(real_gap_hours_2w) OVER () as total_real_gap_hours_2w,
        SUM(gap_hours) OVER () as total_gap_hours,
        SUM(real_gap_hours) OVER () as total_real_gap_hours
        FROM total_gaps
        JOIN total_fishing
        USING(region)
    )
)
-- Return final results
SELECT * FROM frac_by_aoi
'''

q = query_template.format(
    d = config.destination_dataset,
    sat_table = config.sat_reception_smoothed,
    min_positions = config.min_positions_per_day,
    gridded_fishing_table = config.fishing_allocated_table,
    gridded_gaps_table = config.gaps_allocated_raster_table
 )
# dfr = gbq(q)
print(q)

print(f"{dfr.loc[dfr['region'] != 'OTHER', ['frac_all_gaps_2w']].sum()} \
- {dfr.loc[dfr['region'] != 'OTHER', ['frac_all_gaps_2w']].sum()}% of time lost to disabling in Argentina/NW Pacific/W Africa/Alaska")

dfr[['region','frac_all_gaps_2w','frac_all_gaps']]

# ## Fraction of fishing vessels with disabling events
#
# Calculate the fraction of fishing vessels active in the study region had suspected disabling events.

ssvid_query_template = f'''
WITH
--
-- Get all gaps that start in the study area
--
all_disabling_ssvid AS (
    SELECT
    COUNT(DISTINCT ssvid) as ssvid_with_gaps
    FROM `{config.destination_dataset}.{config.gap_events_features_table}`
    {config.gap_filters}
),
--
-- Get areas above reception threshold
--
good_enough_reception as (
    SELECT
    lat_bin,
    lon_bin
    FROM {config.destination_dataset}.{config.sat_reception_smoothed}
    WHERE class = "A"
    GROUP BY lat_bin, lon_bin
    HAVING AVG(positions_per_day) > {config.min_positions_per_day}
),
--
-- Fishing vessels operating outside 50nm in good enough reception
--
all_fishing_ssvid AS (
    SELECT DISTINCT
    ssvid,
    EXTRACT(YEAR from timestamp) as year
    FROM `{config.pipeline_dataset}.{config.pipeline_table}`
    JOIN good_enough_reception
    ON floor(lat_bin) = FLOOR(lat)
    AND floor(lon_bin) = FLOOR(lon)
    WHERE _partitiontime BETWEEN '{config.start_date}' AND '{config.end_date}'
    AND distance_from_shore_m > 1852 * {config.min_distance_from_shore_m}
    -- Noise filter to only include vessels with good segments
    AND seg_id IN (
        SELECT seg_id
        FROM `{config.pipeline_dataset}.{config.segs_table}`
        WHERE good_seg
        AND NOT overlapping_and_short
        )
),
--
-- Join with fishing vessel list. Version used in gaps dataset creation.
--
all_fishing_ssvid_filtered AS (
    SELECT DISTINCT
    ssvid
    FROM all_fishing_ssvid
    JOIN `{config.destination_dataset}.{config.fishing_vessels_table}`
    USING(ssvid, year)
)
--
-- Join counts of MMSI and calculate fraction
--
SELECT
*,
round(ssvid_with_gaps / all_fishing_ssvid, 3) frac_ssvid_with_disabling
FROM all_disabling_ssvid
CROSS JOIN (
    SELECT
    COUNT(*) as all_fishing_ssvid
    FROM all_fishing_ssvid_filtered
) '''

# +
# Run query (expensive, commented out by default)
# print(ssvid_query_template)
# ssvid_df = gbq(ssvid_query_template)

# +
# ssvid_df
# -


