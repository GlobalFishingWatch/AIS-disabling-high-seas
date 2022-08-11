# Hotspots of unseen fishing vessels

This repo contains code supporting the publication "Hotspots of unseen fishing vessels" by Welch et al. (2022)

[![DOI](https://zenodo.org/badge/339832616.svg)](https://zenodo.org/badge/latestdoi/339832616)

## Overview
Automatic Identification System (AIS) data are a powerful tool for tracking and monitoring fishing vessels. However, AIS devices can be intentionally disabled, reducing the efficacy of AIS as a monitoring tool. The purpose of this repository is to archive the scripts used in Welch et al. (2022) to 1. identify intentional disabling events, 2. estimate activity obscured by intentional disabling events, and 3. fit and validate boosted regression tree models to identify the drivers of intentional AIS disabling events.

**Code authors:** Heather Welch (UCSC/NOAA), Tyler Clavelle (GFW), Jennifer Van Osdel (GFW), David Kroodsma (GFW), Tim Hochberg (GFW).

### Repository structure

```
- AIS-disabling-high-seas/
  - ais_disabling/
  - analysis/
  - boosted_regression_trees/
    - functions/
    - scripts_that_call_functions/
  - data/
  - data_production/
    - fishing/
    - gaps/
    - interpolation/
    - labeled_gaps/
    - loitering/
    - reception/
    - time_lost_to_gaps/
    - vessels/
  - model_selection/
    - labeled_dataset/
    - model_selection/
```

## Code

The code for this analysis is divided into multiple subdirectories to isolate data production, modeling, and analysis code. **Note**: The raw AIS data inputs to this analysis can not be made public due to data licensing restrictions and, as a result, code cannot be run externally.

[ais_disabling/](ais_disabling/): Python package containing utility functions and analysis configurations.

[analysis/](analysis/): Scripts supporting the analysis and figure production of the AIS disabling model and events, reception quality, and time lost to AIS disabling.

[boosted_regression_trees/](boosted_regression_trees/): Code pertaining to the boosted regression tree models.

[data/](data/): Final dataset of AIS disabling events (`disabling_events.zip`).

[data_production/](data_production/): Code and queries to produce datasets of AIS gaps, reception quality, gridded vessel activity (fishing and loitering) and time lost to AIS disabling from GFW AIS data.   

[model_selection/](model_selection/): Code pertaining to the development of a labeled training dataset of AIS disabling events and the model selection process for choosing an AIS disabling model.

### Analysis pipeline:

1. Generate AIS-based input datasets, including naive gap events, reception quality, and vessel activity (fishing and loitering)
2. Create labeled training dataset of AIS disabling events
3. AIS disabling model selection
4. Boosted regression tree modeling
5. Additional analyses.

## Data

The [data/](data/) folder of this repo contains the final dataset of AIS disabling events (`disabling_events.zip`) presented in Welch et al. (2022). Additionally, data will be made available on the Global Fishing Watch Data Download Portal (https://globalfishingwatch.org/data-download/).

## Relevant papers

1. Boerder, Kristina, Nathan A. Miller, and Boris Worm. "Global hot spots of transshipment of fish catch at sea." Science advances 4.7 (2018): eaat7159.  
2. Cimino, Megan A., et al. "Towards a fishing pressure prediction system for a western Pacific EEZ." Scientific reports 9.1 (2019): 1-10.  
3. Kroodsma, David A., et al. "Tracking the global footprint of fisheries." Science 359.6378 (2018): 904-908.  
4. McDonald, Gavin G., et al. "Satellites can reveal global extent of forced labor in the world’s fishing fleet." Proceedings of the National Academy of Sciences 118.3 (2021).  
5. White, Timothy D., et al. "Predicted hotspots of overlap between highly migratory fishes and industrial fishing fleets in the northeast Pacific." Science advances 5.3 (2019): eaau3761.
