 
1. `load_libraries.R` - loads and installs are required R packages.
2. `filter_down_your_data.R` - filters data to find 1:1 ratio of presences to pseudo-absences.  
3. `extracto_functions.R` - extracts behavioral and environmental drivers to presences and pseudo-absences.  
4. BRTs_ModelEvaluation_fcns - contains a series of functions to calculate performance metrics on fitted BRTs.

##### `scripts_that_call_functions/``
1. `process_raw_data_code.R` - applies filters to disabling dataset to identify intentional events, develops pseudo absences for both disabling and fisheries activity.
2. `extract_code.R` - extracts environmental and behavioral drivers to disabling and fishing activity datasets.
3. `fit_code.R` - fits boosted regression tree models for all and vessel class specific disabling and fishing activity.
4. `mod_eval_code.R` - evaluates model performance using a series of evaluation metrics.
5. `nearest_EEZ_code.R` - calculates the % of fishing activity and the % of disabling events within 100 kms of every EEZ.
