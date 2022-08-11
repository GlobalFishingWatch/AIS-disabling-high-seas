## Scripts

Code that produces intermediate and output datasets used in this analysis are organized into the following python scripts:

**`run_gaps.py`**: Main script to produce GFW datasets on AIS gap events, reception quality, and gridded fishing and loitering activity.

**`run_labeled_gaps.py`**: Creates a table in BigQuery for the gaps that are in GFW and Exact Earth along with their classification as a true or false gap.

**`run_interpolate.py`**: Creates tables of interpolated AIS positions for use in reception quality estimation and analyses of the time lost to disabling.

**`run_time_lost_to_gaps.py`**: Spatially allocates time associated with AIS gap events using two methods - linear interpolation and rasterized probability.

**`run_vessels.py`**: Creates the list of fishing vessels (MMSI) used in the analysis. This is an updated version of the list described in [Kroodsma et al. (2018)](https://doi.org/10.1126/science.aao5646)

## Queries

Global Fishing Watch (GFW) stores AIS position data in Google BigQuery. Numerous SQL queries were used to generate the AIS-based datasets used in this analysis and these queries are organized by topic in the folders listed below. **Note**: For data license reasons, GFW cannot make the raw AIS position tables used in these queries publicly available.

All queries are formatted with the [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) templating engine.  

```
- data_production/
  - fishing/
  - gaps/
  - interpolation/
  - labeled_gaps/
  - loitering/
  - reception/
  - time_lost_to_gaps/
```
