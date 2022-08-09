# Data

This folder contains the final dataset of 55,368 AIS disabling events presented in Welch et al. (2022). The dataset is stored as a `.zip` archive (`disabling_events.zip`) containing a `.csv` file of the same name.

### Schema

The `ais_disabling.csv` file contains the following fields:

- `gap_id`: Unique id of the AIS disabling event
- `mmsi`: Maritime Mobile Service Identity (MMSI) number of the vessel. MMSI is the unique identifier in AIS data.
- `vessel_class`: Geartype of the vessel. Grouped into five categories - trawlers, drifting longlines, squid jiggers, tuna purse seines, and other.
- `flag`: Flag state (ISO3) of the vessel.
- `vessel_length_m`: Vessel length (meters)
- `vessel_tonnage_gt`: Vessel tonnage (gross tons)
- `gap_start_timestamp`: Time (UTC) at the start of the AIS disabling event
- `gap_start_lat`: Latitude of the vessel at the start of the AIS disabling event
- `gap_start_lon`: Longitude of the vessel at the start of the AIS disabling event
- `gap_start_distance_from_shore_m`: Distance from shore (meters) of the vessel at the start of the AIS disabling event
- `gap_end_timestamp`: Time (UTC) at the end of the AIS disabling event
- `gap_end_lat`: Latitude of the vessel at the end of the AIS disabling event
- `gap_end_lon`: Longitude of the vessel at the end of the AIS disabling event
- `gap_end_distance_from_shore_m`: Distance from shore (meters) of the vessel at the end of the AIS disabling event
- `gap_hours`: Duration (hours) of the AIS disabling event.
