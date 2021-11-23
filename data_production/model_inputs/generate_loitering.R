# ###############################################################################
# # Project: Catena AIS Gaps
# # Script purpose: Generate gridded loitering events 
# # By: Tyler Clavelle
# # Date: 2019-12-18
# ###############################################################################


# Loitering events function -----------------------------------------------

generate_loitering_events <- function(min_date = '2017-01-01',
                                      max_date = '2017-01-31',
                                      vi_version = NA,
                                      run_query = F,
                                      print_query = T,
                                      destination_dataset = "proj_ais_gaps_catena",
                                      table_version = NA,
                                      bq_con = con) {
  
  # Query to generate loitering events for period of interest
  loiter_q <- glue::glue("
  #standardsql
  WITH 
  
  #
  # Get list of carrier vessels from the vessel database who
  # were active between 2017-2019
  #
  carrier_vessels AS (
  SELECT 
   identity.ssvid AS carrier_ssvid
  FROM
  `world-fishing-827.vessel_database.all_vessels_v{vi_version}`
  LEFT JOIN UNNEST(registry)
  LEFT JOIN UNNEST(activity)
  LEFT JOIN UNNEST(feature.geartype) as feature_gear
  WHERE is_carrier AND
  confidence >= 3
  AND
  identity.ssvid NOT IN ('111111111','0','888888888','416202700')
  AND
  DATE(first_timestamp) <= '{max_date}'
  AND DATE(last_timestamp) >= '{min_date}'
  GROUP BY 1),
  
  #
  # Get all loitering events for carrier vessels between 2017-2019
  # Minimum loitering time >= 4 hours, avg speed <= 2 knots, and 
  # average distance from shore >= 20 nm
  #
  loitering_events AS (
  SELECT
    ssvid,
    loitering_start_timestamp,
    loitering_end_timestamp,
    loitering_hours,
    tot_distance_nm,
    avg_speed_knots,
    avg_distance_from_shore_nm,
    start_lon,
    start_lat,
    end_lon,
    end_lat
  FROM
    `gfw_research.loitering_events_v20200205`
  WHERE
  ssvid IN (SELECT carrier_ssvid FROM carrier_vessels) AND
  loitering_start_timestamp >= timestamp('{min_date}') AND 
  loitering_end_timestamp <= timestamp('{max_date}') AND
  loitering_hours >= 4 AND
  avg_speed_knots <= 2 AND
  avg_distance_from_shore_nm > 20 AND
  seg_id IN (
    SELECT
      seg_id
    FROM
      `gfw_research.pipe_v20190502_segs`
    WHERE
      good_seg
      AND NOT overlapping_and_short )),
  
  #
  # Remove duplicates in loitering events
  #
  loitering_dedup AS (
  SELECT DISTINCT
  *
  FROM loitering_events
  )
  
  SELECT * FROM loitering_dedup")
  
  if(run_query){
    
    results <- tryCatch(
      {
        print('Running query...')
        
        # Create BigQuery table object
        bq_loiter <- bigrquery::bq_table(
          project = "world-fishing-827",
          dataset = glue::glue("{destination_dataset}"),
          table = glue('loitering_events_v{table_version}')
        )
        
        # Run query and save to BQ
        results <- bigrquery::bq_project_query(x = "world-fishing-827",
                                               query = loiter_q,
                                               destination_table = bq_loiter,
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
  
  if(print_query) { return(loiter_q) }
}


# Gridded loitering function -------------------------------------------------

generate_gridded_loitering <- function(
  run_query = F,
  print_query = T,
  destination_dataset = "proj_ais_gaps_catena",
  table_version = NA,
  bq_con = con) {

# Query to generate loitering events for period of interest
loiter_q <- glue::glue("
  #standardsql
  WITH 
  
  #
  # Get all loitering events for carrier vessels between 2017-2019
  # Minimum loitering time >= 4 hours, avg speed <= 2 knots, and 
  # average distance from shore >= 20 nm
  #
  loitering_events AS (
  SELECT *
  FROM {destination_dataset}.loitering_events_v{table_version}),
  
  #
  # Bin distinct loitering events to quarter degree
  #
  loitering_binned AS (
  SELECT
    ssvid,
    FLOOR(start_lat * 4) lat_bin,
    FLOOR(start_lon * 4) lon_bin,
    loitering_hours
  FROM
    loitering_events
  )
  
  #
  # Aggregate by lat/lon bins and summarize events
  #
  SELECT
    CAST(lat_bin / 4 + 0.125 AS NUMERIC) AS lat_bin,
    CAST(lon_bin / 4 + 0.125 AS NUMERIC) AS lon_bin,
    SUM(loitering_hours) AS loitering_hours,
    COUNT(*) AS loitering_events
  FROM
    loitering_binned
  GROUP BY
    lat_bin,
    lon_bin")

if(run_query){
  
  results <- tryCatch(
    {
      print('Running query...')
      
      # Create BigQuery table object
      bq_loiter <- bigrquery::bq_table(
        project = "world-fishing-827",
        dataset = glue::glue("{destination_dataset}"),
        table = glue('gridded_loitering_v{table_version}')
      )
      
      # Run query and save to BQ
      results <- bigrquery::bq_project_query(x = "world-fishing-827",
                                             query = loiter_q,
                                             destination_table = bq_loiter,
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

if(print_query) { return(loiter_q) }
}

# # Map loitering hours
# loiter_df %>% 
#   gfw_project_raster(fill = 'loitering_hours', output_crs = gfw_projections('Equal Earth')$proj) %>% 
#   gfw_map(theme = 'light') +
#   geom_raster(aes(x = lon_bin, y = lat_bin, fill = loitering_hours)) +
#   scale_fill_gradientn(colors = gfw_palette('map_presence_light'),
#                        trans = 'log10',
#                        labels = scales::comma,
#                        na.value = NA) +
#   coord_sf(crs = gfw_projections('Equal Earth')$proj) +
#   labs(title = glue::glue('Loitering hours between 2017-2019'),
#        fill = 'Loitering hours')
#
# # Save figure
# ggsave(filename = here::here('figures', glue::glue('loitering_hours_2017_to_2019.png')),
#        width = 6,
#        height = 4)
#
# # Save loitering data
# write_csv(loiter_df, 
#           path = here::here('data', glue::glue('loitering_2017_to_2019_v20200904.csv')))
