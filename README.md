Code to support the publication Welch, H. et al. "Global Automatic Identification System disabling on the high seas"  
Placeholder for zendo DOI. 

Automatic Information System (AIS) data are a powerful tool for tracking and monitoring fishing vessels. However, AIS devices can be intentionally disabled, reducing the efficacy of AIS as a monitoring tool. The purpose of this reposity is to archive the scripts used in Welch et al. 20xx to 1. identify intentional disabling events, 2. estimate activity obscured by intentional disabling events, and 3. fit and validate boosted regression tree models to identify the drivers of intentional AIS disabligne vents.

**Code authors:** Heather Welch (UCSC/NOAA), Tyler Clavelle (GFW), Jennifer Van Osdel (GFW), David Kroodsma (GFW). Add your names!  

**Relevant papers:**. 

1. Boerder, Kristina, Nathan A. Miller, and Boris Worm. "Global hot spots of transshipment of fish catch at sea." Science advances 4.7 (2018): eaat7159.  
2. Cimino, Megan A., et al. "Towards a fishing pressure prediction system for a western Pacific EEZ." Scientific reports 9.1 (2019): 1-10.  
3. Kroodsma, David A., et al. "Tracking the global footprint of fisheries." Science 359.6378 (2018): 904-908.  
4. McDonald, Gavin G., et al. "Satellites can reveal global extent of forced labor in the world’s fishing fleet." Proceedings of the National Academy of Sciences 118.3 (2021).  
5. White, Timothy D., et al. "Predicted hotspots of overlap between highly migratory fishes and industrial fishing fleets in the northeast Pacific." Science advances 5.3 (2019): eaau3761.  

**Data description:**
1. Intentional AIS disabling events. Are these going to be public? If so please provide file name and description and add to repository. If not please provide some availability statement like available by request to GFW.  
2. Fisheries activity.  Are these going to be public? If so please provide file name and description and add to repository. If not please provide some availability statement like available by request to GFW. 
3. Environmental and behavioural drivers. Global rasters at .25 degrees resolution. Raster metadata is available in the supplementary materials.  

**Analysis pipeline:**
## Machine learning. 
**Functions:**  
1. load_libraries.R - loads and installs are required R packages. 
2. filter_down_your_data.R - filters data to find 1:1 ratio of presences to pseudo-absences.  
3. extracto_functions.R - extracts behavioral and environmental drivers to presences and pseudo-absences.  
4. BRTs_ModelEvaluation_fcns - contains a series of functions to calculate performance metrics on fitted BRTs. 

**Scripts that call functions:**
1. process_raw_data_code.R - applies filters to disabling dataset to identify intentional events, develops pseudo absences for both disabling and fisheries activity. 
2. extract_code.R - extracts environmental and behavioral drivers to disabling and fishing activity datasets. 
3. fit_code.R - fits boosted regression tree models for all and vessel class specific disabling and fishing activity. 
4. mod_eval_code.R - evaluates model performance using a series of evaluation metrics. 
5. nearest_EEZ_code.R - calculates the % of fishing activity and the % of disabling events within 100 kms of every EEZ. 
