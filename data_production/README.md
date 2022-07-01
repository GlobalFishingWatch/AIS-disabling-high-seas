
## Scripts

**`run_gaps.py`**: Main script to produce GFW datasets on AIS gap events, reception quality, and gridded fishing and loitering activity.

**`run_labeled_gaps.py`**: Creates a table in BigQuery for the gaps that are in GFW and Exact Earth along with their classification as a true or false gap.

**`utils.py`**: Utility functions to support `run_gaps.py`

## Queries

Global Fishing Watch (GFW) stores the AIS position data in Google BigQuery. The following SQL queries were used to generate AIS gap and reception quality datasets from the AIS data, as well as the fishing and loitering behavioral driver features used by the BRT model. **Note**: For data license reasons, GFW cannot make the AIS position tables used in these queries publicly available.

All queries are formatted with the [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) templating engine.  

**fishing/**:
  - `fishing_gridded.sql.j2`

**gaps/**:
  - `ais_off_on_events.sql.j2`
  - `ais_gap_events.sql.j2`
  - `ais_gap_events_features.sql.j2`

**interpolation/**:
  - `hourly_interpolation_byseg.sql.j2`
  - `hourly_fishing_interpolation_byseg.sql.j2`
  - `hourly_gap_interpolation.sql.j2`

**loitering/**:
  - `loitering_events.sql.j2`
  - `loitering_events_gridded.sql.j2`

**reception/**:
  - `reception_measured.sql.j2`
