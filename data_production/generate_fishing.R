################################################################################
## Project: Catena AIS Gaps
## Script purpose: Query to generate vessel presence and fishing activity layer
## By: Tyler Clavelle 
## Date: 2020-03-25
################################################################################

generate_fishing <- function(min_date = '2017-01-01',
                             max_date = '2017-01-31',
                             vi_version = NA,
                             run_query = F,
                             print_query = T,
                             destination_dataset = "proj_ais_gaps_catena",
                             table_version = NA,
                             bq_con = con) {
  # Query
  fishing_q <- glue("
  /*
  Generate aggregate vessel presence, fishing activity, and 
  number of fishing MMSI present per 0.25 degree grid cell
  over full time period of analysis.
  */
  
  WITH

# Positions for fishing vessels, binned to 0.25 degrees
positions AS (
SELECT
  ssvid,
  (CASE
        WHEN type IN ('AIS.1', 'AIS.2', 'AIS.3') THEN 'A'
        WHEN type IN ('AIS.18','AIS.19') THEN 'B'
        ELSE NULL
     END
   ) as class,
  EXTRACT(date from date) as date, 
  EXTRACT(year from date) as year,
  FLOOR(lat * 4) AS lat_bin,
  FLOOR(lon * 4) AS lon_bin,
  hours,
  IF(nnet_score2 > 0.5, hours, 0) as fishing_hours
FROM `gfw_research.pipe_v20190502_fishing`
WHERE date >= '{min_date}'
AND date <= '{max_date}'
AND seg_id IN (
  SELECT seg_id 
  FROM `gfw_research.pipe_v20190502_segs` 
  WHERE good_seg
  AND NOT overlapping_and_short)
),

# Subset to active, non-noise fishing positions
fishing_positions AS (
SELECT 
* EXCEPT(best_flag, best_vessel_class)
FROM positions
INNER JOIN `gfw_research.fishing_vessels_ssvid_v{vi_version}` 
USING (ssvid, year)
),

# Add vessel info
fishing_info AS (
SELECT
*
FROM fishing_positions
LEFT JOIN (
  SELECT
  ssvid,
  year,
  best.best_flag as flag,
  # TEMPORARY
  # Overwrite pole and line vessels as squid when registries say their squid
  IF(best.registry_net_disagreement = 'squid_jigger,pole_and_line',
     'squid_jigger', best.best_vessel_class) as vessel_class,
  FROM `gfw_research.vi_ssvid_byyear_v{vi_version}`
 )
USING(ssvid, year)
),

# Summarize by day
fishing_days AS (
SELECT
  ssvid,
  vessel_class,
  flag,
  date,
  year,
  lat_bin,
  lon_bin,
  class,
  SUM(hours) as hours,
  SUM(fishing_hours) as fishing_hours,
  IF(SUM(hours) > 0, 1, 0) as vessel_day,
  IF(SUM(fishing_hours) > 1, 1, 0) as fishing_day
FROM fishing_info
GROUP BY ssvid, vessel_class, flag, date, year, lat_bin, lon_bin, class
),

# Aggregate all data by 0.25 degree bin 
all_fishing_binned AS (
SELECT
  CAST(lat_bin / 4 + 0.125 as NUMERIC) lat_bin,
  CAST(lon_bin / 4 + 0.125 as NUMERIC) lon_bin,
  vessel_class,
  flag,
  class,
  SUM(hours) as hours,
  SUM(fishing_hours) as fishing_hours,
  COUNT(DISTINCT ssvid) as mmsi_present,
  SUM(vessel_day) as vessel_days,
  SUM(fishing_day) as fishing_days
FROM fishing_days
GROUP BY lat_bin, lon_bin, vessel_class, flag, class
)

# Return binned data
select * from all_fishing_binned  
order by lat_bin, lon_bin, vessel_class, flag, class
")

if(run_query){
  
  results <- tryCatch(
    {
      print('Running query...')
      
      # Generate table version
      # tbl_v <- stringr::str_remove_all(lubridate::today(), pattern = '-')
      tbl_date <- lubridate::year(min_date)
      tbl_end_date <- lubridate::year(max_date)
      
      # Create BigQuery table object
      bq_fish <- bigrquery::bq_table(
        project = "world-fishing-827",
        dataset = glue::glue("{destination_dataset}"),
        table = glue::glue("vessel_presence_v{table_version}_from_{tbl_date}_to_{tbl_end_date}")
      )
      
      # Run query and save to BQ
      results <- bigrquery::bq_project_query(x = "world-fishing-827",
                                             query = fishing_q,
                                             destination_table = bq_fish,
                                             write_disposition = 'WRITE_TRUNCATE')
    },
    
    error = function(cond){
      message("There appear to be an error with this query")
      message("Here is the original error")
      message(cond)
      # Choose a return value in case of error
      stop('Cancelled Query', call. = FALSE)
    }
  )
} 

if(print_query) { return(fishing_q) }
}