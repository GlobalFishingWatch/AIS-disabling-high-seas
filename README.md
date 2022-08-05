# Hotspots of unseen fishing vessels

Code to support the publication Welch, H. et al. "Hotspots of unseen fishing vessels"

[![DOI](https://zenodo.org/badge/339832616.svg)](https://zenodo.org/badge/latestdoi/339832616)

## Overview
Automatic Identification System (AIS) data are a powerful tool for tracking and monitoring fishing vessels. However, AIS devices can be intentionally disabled, reducing the efficacy of AIS as a monitoring tool. The purpose of this repository is to archive the scripts used in Welch et al. 2022 in order to 1. identify intentional disabling events, 2. estimate activity obscured by intentional disabling events, and 3. fit and validate boosted regression tree models to identify the drivers of intentional AIS disabling events.

**Code authors:** Heather Welch (UCSC/NOAA), Tyler Clavelle (GFW), Jennifer Van Osdel (GFW), David Kroodsma (GFW).

### Repository structure

```
- AIS-disabling-high-seas/
  - analysis/
  - data/
    - environmental_and_behavioural_drivers/
  - data_production/
    - fishing/
    - gaps/
    - interpolation/
    - loitering/
    - reception/
  - machine_learning/
    - functions/
    - scripts_that_call_functions/
  - model_selection/
    - labeled_dataset/
    - model_selection/
```

## Data

The input data (when possible) and results of this analysis are available on Global Fishing Watch's data download portal here: [https://globalfishingwatch.org/data-download/](https://globalfishingwatch.org/data-download/)

## Code

The code for this analysis is divided into multiple subdirectories to isolate data production, modeling, and analysis code.

[analysis/](analysis/README.md): Scripts supporting the analysis of the AIS disabling model and events, reception quality, and time lost to AIS disabling.

[data/](): Input data to the BRT models

[data_production/](data_production/README.md): Code and queries to produce datasets of AIS gaps, reception quality, and gridded fishing and loitering activity from GFW AIS data. **Note**: The raw AIS data inputs to this analysis can not be made public due to data licensing restrictions and so the code cannot be run externally.  

[boosted_regression_trees/](boosted_regression_trees/README.md): Code pertaining to the boosted regression tree models.

[model_selection/](model_selection/README.md): Code pertaining to the development of a labeled training dataset of AIS disabling events and the model selection process for choosing an AIS disabling model.

### Analysis pipeline:

1. Generate AIS-based input datasets, including gap events, reception quality, and fishing vessel activity and loitering activity
2. Create labeled training dataset of AIS disabling events
3. AIS disabling model selection
4. Boosted regression tree modeling
5. Additional analyses.

## Relevant papers

1. Boerder, Kristina, Nathan A. Miller, and Boris Worm. "Global hot spots of transshipment of fish catch at sea." Science advances 4.7 (2018): eaat7159.  
2. Cimino, Megan A., et al. "Towards a fishing pressure prediction system for a western Pacific EEZ." Scientific reports 9.1 (2019): 1-10.  
3. Kroodsma, David A., et al. "Tracking the global footprint of fisheries." Science 359.6378 (2018): 904-908.  
4. McDonald, Gavin G., et al. "Satellites can reveal global extent of forced labor in the world’s fishing fleet." Proceedings of the National Academy of Sciences 118.3 (2021).  
5. White, Timothy D., et al. "Predicted hotspots of overlap between highly migratory fishes and industrial fishing fleets in the northeast Pacific." Science advances 5.3 (2019): eaau3761.
