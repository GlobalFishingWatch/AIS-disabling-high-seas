################################################################################
## Project: Catena AIS Gaps
## Script purpose: Query to generate gaps dataset
## By: Tyler Clavelle 
## Date: 2019-12-16
################################################################################

# Function to generate raw gaps dataset
generate_gaps <- function(min_date = "2017-01-01",
                          max_date = "2017-01-31",
                          gaps_table = "proj_ais_gaps_catena.ais_gap_events_v20201124",
                          vi_version = NA,
                          run_query = F,
                          print_query = T,
                          destination_dataset = "proj_ais_gaps_catena",
                          table_version = NA,
                          bq_con = con) {
  
  
# Gaps query 
gaps_query <- glue("
##########################################################
/*
QUERY TO PRODUCE GAPS DATASET FOR NOAA/UCSC AIS GAP EVENTS
PROJECT
  This query filters the gaps data in proj_ais_gaps_catena.ais_gap_events,
  joins it with the smoothed satellite reception data in
  proj_ais_gaps_catena.sat_reception_one_degree_vYYYYMMDD,
  and calculates metrics used in gaps classification
  (i.e. positions_X_hours_before_sat).
*/
##########################################################
# Declare date range
CREATE TEMP FUNCTION start_date() AS (DATE('{min_date}'));
CREATE TEMP FUNCTION end_date() AS (DATE('{max_date}'));
WITH
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
# Filter gap events to 2017 and later
gaps AS (
SELECT 
  * EXCEPT(gap_start_lat, gap_start_lon, gap_end_lat, gap_end_lon, gap_start_class, gap_end_class, gap_start_receiver_type, gap_end_receiver_type, gap_start_distance_from_shore_m, gap_end_distance_from_shore_m, gap_start_rfmo, positions_12_hours_before, positions_12_hours_before_sat, positions_12_hours_before_ter),
  gap_start_lat as off_lat,
  gap_start_lon as off_lon,
  gap_end_lat as on_lat,
  gap_end_lon as on_lon,
  gap_start_class as off_class,
  gap_end_class as on_class,
  gap_start_receiver_type as off_receiver_type,
  gap_end_receiver_type as on_receiver_type,
  gap_start_distance_from_shore_m as off_distance_from_shore_m,
  gap_end_distance_from_shore_m as on_distance_from_shore_m,
  gap_start_rfmo as rfmo,
  floor(gap_start_lat) as off_lat_bin, # lat bin for joining with reception
  floor(gap_start_lon) as off_lon_bin, # lon bin for joining with reception
  floor(gap_end_lat) as on_lat_bin, # lat bin for joining with reception
  floor(gap_end_lon) as on_lon_bin, # lon bin for joining with reception
  EXTRACT(year from gap_start) as year,
  EXTRACT(month from gap_start) as month,
  positions_12_hours_before,
  positions_12_hours_before_sat,
  positions_12_hours_before_ter
FROM `{gaps_table}` 
WHERE DATE(gap_start) >= start_date()
AND DATE(gap_start) <= end_date()
# NOTE: this automatically excludes open gaps since gap_end is NULL in those cases
# but we include is_closed = 1 check anyways. To include open gaps, remove
# the is_closed = 1 check and revise this line to (DATE(gap_end) <= end_date() OR gap_end IS NULL).
AND DATE(gap_end) <= end_date()
AND gap_start_class IS NOT NULL
AND is_closed = 1
),
# Filter gaps to non-noise fishing vessels
gaps_fishing_vessels AS (
SELECT *
FROM gaps
INNER JOIN all_vessel_info
USING (ssvid, year)
),
# Monthly satellite reception at on degree. Limit to 0
sat_reception AS (
SELECT 
* EXCEPT(positions_per_day),
IF(positions_per_day < 0, 0, positions_per_day) as positions_per_day
FROM `proj_ais_gaps_catena.sat_reception_one_degree_v20200806`
),
-- Reception at the lat/lon where the gap starts
off_quality AS (
SELECT
  gap_id,
  gap_start,
  gap_end,
  a.positions_per_day AS positions_per_day_off
FROM
  sat_reception a
JOIN
  gaps_fishing_vessels b
ON
  a.lat_bin = b.off_lat_bin
  AND a.lon_bin = b.off_lon_bin
  AND a.year = b.year
  AND a.month = b.month
  AND a.class = b.off_class),
-- Reception at the lat/lon where the gap ends
on_quality AS (
SELECT
  gap_id,
  gap_start,
  gap_end,
  a.positions_per_day AS positions_per_day_on
FROM
  sat_reception as a
JOIN
  gaps_fishing_vessels b
ON
  a.lat_bin = b.on_lat_bin
  AND a.lon_bin = b.on_lon_bin
  AND a.year = b.year
  AND a.month = b.month
  AND a.class = b.on_class),
# Join gaps with off reception
gaps_reception AS(
SELECT 
  *
  # Exclude reception related variables
  EXCEPT(off_lat_bin, off_lon_bin, on_lat_bin, on_lon_bin)
FROM gaps_fishing_vessels
LEFT JOIN
off_quality
USING(gap_id, gap_start, gap_end)
LEFT JOIN on_quality
USING(gap_id, gap_start, gap_end)
),
-- Position messages for all SSVID in the gaps table,
-- filtered to only good segments.
legit_position_messages AS (
SELECT
* EXCEPT(year, month, lat_bin, lon_bin, class)
FROM (
  SELECT
    ssvid,
    timestamp,
    receiver_type,
    floor(lat) as lat_bin,
    floor(lon) as lon_bin,
    EXTRACT(year from timestamp) as year,
    EXTRACT(month from timestamp) as month,
    # Identify Class A and B messages
    (CASE
        WHEN type IN ('AIS.1', 'AIS.2', 'AIS.3') THEN 'A'
        WHEN type IN ('AIS.18','AIS.19') THEN 'B'
        ELSE NULL
     END
   ) as class
  FROM
    `gfw_research.pipe_v20190502`
  WHERE
    DATE(date) BETWEEN 
      # Get position messages further back in time to account for the X hours lookback 
      DATE(TIMESTAMP_SUB(TIMESTAMP(start_date()), INTERVAL 30 day)) 
      AND DATE(TIMESTAMP_ADD(TIMESTAMP(end_date()), INTERVAL 30 day)) 
    AND ssvid IN (SELECT ssvid FROM gaps)
    AND seg_id IN (
    SELECT
      seg_id
    FROM
      `gfw_research.pipe_v20190502_segs`
    WHERE
      good_seg
      AND NOT overlapping_and_short ) 
  )
LEFT JOIN sat_reception
USING(lat_bin, lon_bin, year, month, class)
),
-- Number of position messages in the
-- 12 hours after gap end
twelve_hours_after AS (
SELECT
  gap_id,
  gap_start,
  gap_end,
  COUNT(*) positions_12_hours_after,
  SUM(IF(receiver_type = 'terrestrial', 1,0)) as positions_12_hours_after_ter,
  SUM(IF(receiver_type = 'satellite', 1,0)) as positions_12_hours_after_sat
FROM
  gaps_reception
JOIN
  legit_position_messages
USING
  (ssvid)
WHERE
  timestamp BETWEEN gap_end AND TIMESTAMP_ADD(gap_end, INTERVAL 12 hour)
GROUP BY
  gap_id,
  gap_start,
  gap_end),
-- Number of position messages in the
-- X hours after gap end where X is
-- the length of the gap
X_hours_before AS (
SELECT
  gap_id,
  gap_start,
  gap_end,
  COUNT(*) positions_X_hours_before,
  SUM(IF(receiver_type = 'terrestrial', 1,0)) as positions_X_hours_before_ter,
  SUM(IF(receiver_type = 'satellite', 1,0)) as positions_X_hours_before_sat
FROM
  gaps_reception
JOIN
  legit_position_messages
USING
  (ssvid)
WHERE
  timestamp BETWEEN TIMESTAMP_SUB(gap_start, INTERVAL CAST(gap_hours AS int64) hour) AND gap_start
GROUP BY
  gap_id,
  gap_start,
  gap_end),
-- Number of position messages in the
-- X hours after gap end where X is
-- the length of the gap
X_hours_after AS (
SELECT
  gap_id,
  gap_start,
  gap_end,
  COUNT(*) positions_X_hours_after,
  SUM(IF(receiver_type = 'terrestrial', 1,0)) as positions_X_hours_after_ter,
  SUM(IF(receiver_type = 'satellite', 1,0)) as positions_X_hours_after_sat
FROM
  gaps_reception
JOIN
  legit_position_messages
USING
  (ssvid)
WHERE
  timestamp BETWEEN gap_end AND TIMESTAMP_ADD(gap_end, INTERVAL CAST(gap_hours AS int64) hour)
GROUP BY
  gap_id,
  gap_start,
  gap_end),
-- Join together with the gaps.
-- Must using all three of gap_id, gap_start, and gap_end
-- to properly join due to duplicate gap_ids with
-- same gap_start but different gap_end.
gaps_features AS (
SELECT
  *
FROM
  gaps_reception
  LEFT JOIN twelve_hours_after USING (gap_id, gap_start, gap_end)
  LEFT JOIN X_hours_before USING (gap_id, gap_start, gap_end)
  LEFT JOIN X_hours_after USING (gap_id, gap_start, gap_end)
),
# Return final gaps data
final_gaps AS (
SELECT
  ssvid,
  gap_id,
  off_class,
  on_class,
  off_receiver_type,
  on_receiver_type,
  vessel_class,
  vessel_length_m,
  vessel_tonnage_gt,
  flag,
  rfmo,
  year,
  off_lat,
  off_lon,
  on_lat,
  on_lon,
  off_distance_from_shore_m,
  on_distance_from_shore_m,
  gap_start,
  gap_end,
  gap_hours,
  gap_distance_m,
  gap_implied_speed_knots,
  positions_per_day_on,
  positions_per_day_off,
  IFNULL(positions_12_hours_before, 0) as positions_12_hours_before,
  IFNULL(positions_12_hours_before_ter, 0) as positions_12_hours_before_ter,
  IFNULL(positions_12_hours_before_sat, 0) as positions_12_hours_before_sat,
  IFNULL(positions_12_hours_after, 0) as positions_12_hours_after,
  IFNULL(positions_12_hours_after_ter, 0) as positions_12_hours_after_ter,
  IFNULL(positions_12_hours_after_sat, 0) as positions_12_hours_after_sat,
  IFNULL(positions_X_hours_before, 0) as positions_X_hours_before,
  IFNULL(positions_X_hours_before_ter, 0) as positions_X_hours_before_ter,
  IFNULL(positions_X_hours_before_sat, 0) as positions_X_hours_before_sat,
  IFNULL(positions_X_hours_after, 0) as positions_X_hours_after,
  IFNULL(positions_X_hours_after_ter, 0) as positions_X_hours_after_ter,
  IFNULL(positions_X_hours_after_sat, 0) as positions_X_hours_after_sat
FROM gaps_features
)
SELECT
*
FROM
final_gaps
")

      if(run_query){
        
        results <- tryCatch(
          {
            print('Running query...')
            
            # Create BigQuery table object
            bq_gaps <- bigrquery::bq_table(
              project = "world-fishing-827",
              dataset = glue::glue("{destination_dataset}"),
              table = glue::glue("raw_gaps_v{table_version}")
            )
            
            # Run query and save to BQ
            results <- bigrquery::bq_project_query(x = "world-fishing-827",
                                                   query = gaps_query,
                                                   destination_table = bq_gaps,
                                                   write_disposition = 'WRITE_TRUNCATE')
          },
          
          error = function(cond){
          message("There appears to be an error with this query")
          message("Here is the original error")
          message(cond)
          # Choose a return value in case of error
          stop('Cancelled Query', call. = FALSE)
          }
        )
      } 
      
      if(print_query) { return(gaps_query) }
}