################################################################################
## Project: AIS Gaps Catena
## Script purpose: Wrapper script to produce input datasets for gap drivers model
## By: Tyler Clavelle
## Date: 2020-12-02
################################################################################

# Setup -------------------------------------------------------------------

# Packages
library(tidyverse)
library(lubridate)
library(bigrquery)
library(glue)
library(fishwatchr)
library(patchwork)

# Source functions
source(here::here('data-production', 'generate_gaps.R'))
source(here::here('data-production', 'generate_fishing.R'))
source(here::here('data-production', 'generate_loitering.R'))

# BigQuery connection
con <- DBI::dbConnect(bigrquery::bigquery(), project = "world-fishing-827", use_legacy_sql = FALSE)

# Parameters
tbl_v <- '20201209' # Version of model inputs to produce
min_date <- "2017-01-01" # First date of analysis
max_date <- "2019-12-31" # Last date of analysis
tbl_date <- lubridate::year(min_date) # First year of analysis
tbl_end_date <- lubridate::year(max_date) # Last year of analysis
vi_version <- "20201209" # Vessel info version to use
destination_dataset <- "proj_ais_gaps_catena" # Destination dataset to save query results
gaps_v <- "20201124" # Raw gap events table to use 
gaps_table <- glue("{destination_dataset}.ais_gap_events_v{gaps_v}") # Location of raw gap events table to use

# Create directory in data folder to save results
results_dir <- here::here("data", glue("gap_inputs_v{tbl_v}"))
dir.create(results_dir, recursive = T)

# Run Queries -------------------------------------------------------------

# Run query to produce new gaps table and save in BigQuery
generate_gaps(min_date = min_date,
              max_date = max_date,
              gaps_table = gaps_table,
              vi_version = vi_version,
              run_query = TRUE, # Change to TRUE when running query
              print_query = FALSE,
              destination_dataset = destination_dataset,
              table_version = tbl_v,
              bq_con = con)

# Run query to produce gridded fishing vessel presence table and save in BigQuery
generate_fishing(min_date = min_date,
                 max_date = max_date,
                 vi_version = vi_version,
                 run_query = TRUE, # Change to TRUE when running query
                 print_query = FALSE,
                 destination_dataset = destination_dataset,
                 table_version = tbl_v,
                 bq_con = con)

# Run query to produce loitering events table and save in BigQuery
generate_loitering_events(min_date = min_date,
                   max_date = max_date,
                   run_query = TRUE, # Change to TRUE when running query
                   print_query = FALSE,
                   destination_dataset = destination_dataset,
                   vi_version = "20201201",
                   table_version = tbl_v,
                   bq_con = con)

# Run query to produce loitering events table and save in BigQuery
generate_gridded_loitering(run_query = TRUE, # Change to TRUE when running query
                          print_query = FALSE,
                          destination_dataset = destination_dataset,
                          table_version = tbl_v,
                          bq_con = con)

# Download and save results -------------------------------

# Gaps
gaps_q <- glue::glue("
  SELECT * 
  FROM `proj_ais_gaps_catena.raw_gaps_v{tbl_v}` 
  WHERE gap_hours >= 12.0"
)
gaps_df <- DBI::dbGetQuery(con, gaps_q)

# Vessel Presence
presence_q <- glue::glue("SELECT * FROM `proj_ais_gaps_catena.vessel_presence_v{tbl_v}_from_{tbl_date}_to_{tbl_end_date}`")
presence_df <- DBI::dbGetQuery(con, presence_q)

# Loitering
loitering_q <- glue::glue("SELECT * FROM `proj_ais_gaps_catena.gridded_loitering_v{tbl_v}`")
loitering_df <- DBI::dbGetQuery(con, loitering_q)

# Save results ------------------------------------------------------------

# Save gaps results
write_csv(gaps_df, glue::glue('{results_dir}/gaps_v{tbl_v}_12hr_{tbl_date}_to_{tbl_end_date}.csv.gz'))

# Save vessel presence results
write_csv(presence_df, glue::glue('{results_dir}/vessel_presence_quarter_degree_v{tbl_v}_{tbl_date}_to_{tbl_end_date}.csv.gz'))

# Save loitering results
write_csv(loitering_df, glue::glue('{results_dir}/loitering_quarter_degree_v{tbl_v}_{tbl_date}_to_{tbl_end_date}.csv.gz'))


# Gap Summary Statistics --------------------------------------------------

# Summarize suspected disabling events
gap_events_q <- glue::glue("
  SELECT * 
  FROM `proj_ais_gaps_catena.raw_gaps_v{tbl_v}` 
  WHERE positions_X_hours_before_sat >= 25  
  AND gap_hours >= 12
  AND (positions_per_day_off > 5 and positions_per_day_on > 5)
  AND  (off_distance_from_shore_m > 1852*200)"
)
gaps_events_df <- DBI::dbGetQuery(con, gap_events_q)

# Median gap duration and distance
median(gaps_events_df$gap_hours, na.rm = T)
median(gaps_events_df$gap_distance_m / 1000, na.rm = T)
n_distinct(gaps_events_df$ssvid)
n_distinct(gaps_events_df$flag)

# Histograms of gap duration and distance
# Distance
p1 <- gaps_events_df %>% 
  mutate(gap_distance_m = ifelse(gap_distance_m > 4e6, 4e6, gap_distance_m)) %>%
  ggplot() +
  geom_histogram(aes(x = gap_distance_m / 1000),
                 binwidth = 50) +
  scale_x_continuous(breaks = c(0,1000,2000,3000,4000),
                     labels = c('0','1000','2000','3000','4000+')) +
  labs(x = 'Distance travelled during suspected\nAIS disabling event\n(kilometers; bin width = 50)',
       y = 'Count') +
  theme_gfw()

# Duration
p2 <- gaps_events_df %>% 
  mutate(gap_hours = ifelse(gap_hours > 24*7*4, 24*7*4, gap_hours)) %>% 
  ggplot() +
  geom_histogram(aes(x = gap_hours), binwidth = 12) +
  scale_x_continuous(breaks = c(0,100,200,300,400, 500,600,672),
                     labels = c('0','100','200','300','400','500','600','672+')) +
  labs(x = 'Duration of suspected AIS disabling event\n(hours; bin width = 12)',
       y = 'Count') +
  theme_gfw()

# Combine plots
p1 + p2 + plot_layout(ncol = 1)

# Save
ggsave(filename = here::here("figures","gap_histograms.png"),
       width = 6,
       height = 6,
       dpi = 300)