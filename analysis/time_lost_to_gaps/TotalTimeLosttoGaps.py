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

# # How Much Time is lost To Gaps?
#
# Calculate the total number of events and the amount of time that is lost to gaps in the high seas. We will calculate it using the raster method and using the interpolation method.

# +
import pandas as pd

def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')


# -

# ## Interpolation method
#
# ### By vessel class

query_template = '''
WITH
--
-- Get areas above reception threshold
--
good_enough_reception as (
    SELECT
    lat_bin,
    lon_bin
    FROM `world-fishing-827.proj_ais_gaps_catena.sat_reception_smoothed_one_degree_v20210722`
    WHERE class = "A"
    GROUP BY lat_bin, lon_bin
    HAVING AVG(positions_per_day) > 5
),
--
-- Gridded activity by fishing vessels active beyond 50 nautical miles. Note David renamed tuna_purse_seines
-- to purse_seines in the fishing_activity table
--
fishing_activity AS(
    SELECT
    IF(vessel_class IN ('trawlers','drifting_longlines','purse_seines','squid_jigger'), vessel_class, 'other_geartypes') as vessel_class,
    IF(flag IN ('CHN','TWN','ESP','KOR'), flag, 'other_flags') as flag,
    hours_in_gaps_under_12
    FROM {gridded_fishing_table}
    JOIN good_enough_reception
    ON floor(lat_index) = lat_bin
    AND floor(lon_index) = lon_bin
    WHERE over_50nm
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
-- Get all gaps that start in the high seas
--
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
        gap_hours,
        -- Flag disabling events over certain durations to calculate average duration later
        gap_hours > 14*24 over_two_weeks,
        gap_hours > 7*24*4 over_four_weeks,
    FROM `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
    WHERE gap_hours >= 12
    AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
    AND positions_X_hours_before_sat >= 19
    AND off_distance_from_shore_m > 1852 * 50
    AND on_distance_from_shore_m > 1852 * 50
    AND DATE(gap_start) >= '2017-01-01'
    AND DATE(gap_end) <= '2019-12-31'
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
    IF(vessel_class IN ('trawlers','drifting_longlines','purse_seines','squid_jigger'), vessel_class, 'other_geartypes') as vessel_class,
    IF(flag IN ('CHN','TWN','ESP','KOR'), flag, 'other_flags') as flag
    FROM {gridded_gap_table}
    JOIN good_enough_reception
    ON floor(lat_index) = lat_bin
    AND floor(lon_index) = lon_bin
    WHERE over_50nm
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

q = query_template.format(gridded_fishing_table = 'proj_ais_gaps_catena.fishing_activity_v20211203',
                          gridded_gap_table = 'proj_ais_gaps_catena.gaps_allocated_interpolate_v20211203')
print(q)
# dfi = gbq(q)

print("Fraction of time lost by class - 2 week versus all time, using interpolation method")
for index,row in dfi.iterrows():
    print(f"{100*row.frac_gaps_2w:.1f}-{100*row.frac_gaps:.1f}% - {row.vessel_class}")

dfi[['vessel_class',
    'disabling_events',
    'real_gap_days_2w',
    'real_gap_days',
    'frac_gaps_2w',
    'frac_gaps',
    'avg_gap_days_2w',
    'avg_gap_days']]

# Calculate the percent of disabling events by top flags and geartypes.

# +
top_flag_events = dfi[dfi['vessel_group'].isin(['KOR','ESP','CHN','TWN'])]['disabling_events'].sum()
all_events = dfi[dfi['vessel_group'] == 'all vessels']['disabling_events'].sum()

top_flag_events / all_events * 100
# -

top_gear_events = dfi[dfi['vessel_group'].isin(['drifting_longlines',
                                                'purse_seines',
                                                'trawlers',
                                                'squid_jigger'])]['disabling_events'].sum()
top_gear_events / all_events * 100

# ## Now with raster method

q = query_template.format(gridded_fishing_table = 'proj_ais_gaps_catena.fishing_activity_v20211203',
                          gridded_gap_table = 'proj_ais_gaps_catena.gaps_allocated_raster_v20211203')
dfr = gbq(q)

print("Fraction of time lost by class - 2 week versus all time, using raster method")
for index,row in dfr.iterrows():
    print(f"{100*row.frac_gaps_2w:.1f}-{100*row.frac_gaps:.1f}% - {row.vessel_class}")

dfr[['vessel_group',
    'disabling_events',
#     'real_gap_days_2w',
    'real_gap_days',
#     'frac_gaps_2w',
    'frac_gaps',
#     'avg_gap_days_2w',
    'avg_gap_days']]

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
    FROM `world-fishing-827.proj_ais_gaps_catena.sat_reception_smoothed_one_degree_v20210722`
    WHERE class = "A"
    GROUP BY lat_bin, lon_bin
    HAVING AVG(positions_per_day) > 5
),
--
-- Gridded activity by fishing vessels active beyond 50 nautical miles. Note David renamed tuna_purse_seines
-- to purse_seines in the fishing_activity table
--
fishing_activity AS(
    SELECT
    lat_bin,
    lon_bin,
    hours_in_gaps_under_12
    FROM {gridded_fishing_table}
    JOIN good_enough_reception
    ON floor(lat_index) = lat_bin
    AND floor(lon_index) = lon_bin
    WHERE over_50nm
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
    FROM {gridded_gaps_table}
    JOIN good_enough_reception
    ON floor(lat_index) = lat_bin
    AND floor(lon_index) = lon_bin
    WHERE over_50nm
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

# ## Fraction of fishing vessels with disabling events
#
# Calculate the fraction of fishing vessels active in the study region had suspected disabling events.

ssvid_query_template = '''
WITH
--
-- Get all gaps that start in the study area
--
all_disabling_ssvid AS (
    SELECT
    -- COUNT(DISTINCT gap_id) as events,
    COUNT(DISTINCT ssvid) as ssvid_with_gaps
    FROM `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
    WHERE gap_hours >= 12
    AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
    AND positions_X_hours_before_sat >= 19
    AND off_distance_from_shore_m > 1852 * 50
    AND on_distance_from_shore_m > 1852 * 50
    AND DATE(gap_start) >= '2017-01-01'
    AND DATE(gap_end) <= '2019-12-31'
),
--
-- Get areas above reception threshold
--
good_enough_reception as (
    SELECT
    lat_bin,
    lon_bin
    FROM `world-fishing-827.proj_ais_gaps_catena.sat_reception_smoothed_one_degree_v20210722`
    WHERE class = "A"
    GROUP BY lat_bin, lon_bin
    HAVING AVG(positions_per_day) > 5
),
--
-- Fishing vessels operating outside 50nm in good enough reception
--
all_fishing_ssvid AS (
    SELECT DISTINCT
    ssvid,
    EXTRACT(YEAR from timestamp) as year
    FROM `world-fishing-827.gfw_research.pipe_v20201001_fishing`
    JOIN good_enough_reception
    ON floor(lat_bin) = FLOOR(lat)
    AND floor(lon_bin) = FLOOR(lon)
    WHERE _partitiontime BETWEEN '2017-01-01' AND '2019-12-31'
    AND distance_from_shore_m > 1852 * 50
    -- Noise filter to only include vessels with good segments
    AND seg_id IN (
        SELECT seg_id
        FROM `world-fishing-827.gfw_research.pipe_v20201001_segs`
        WHERE good_seg
        AND positions > 10)
),
--
-- Join with fishing vessel list. Version used in gaps dataset creation.
--
all_fishing_ssvid_filtered AS (
    SELECT DISTINCT
    ssvid
    FROM all_fishing_ssvid
    JOIN `world-fishing-827.gfw_research.fishing_vessels_ssvid_v20210301`
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
# ssvid_df = gbq(ssvid_query_template)
