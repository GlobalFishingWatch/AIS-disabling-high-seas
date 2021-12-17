# Description
This folder contains all of the information necessary to independently replicate the model selection results.

# Files

### Train-Test Split
* *gaps_test_set.csv* : All of the gaps in the test set, with attributes.

* *gaps_training_set.csv* : All of the gaps in the training set, with attributes.

### Cross Validation Splits
* *cv_gss_splits.json* : Contains the division of gaps into training and test for every single model run in the cross validation. This information is given by MMSI (or SSVID) as the gaps were grouped by MMSI so all gaps from a given MMSi were either entirely in the training OR the test set for each run.

### Cross Validation Results
* *cv_results.json* : The cross validation results for each of the ten runs. This includes fit times, score times, and scores.

### Fitted Models
* *final_model.json* : The final selected model fitted on the entirely of the training set represented by *gaps_training_set.csv*. This includes all of the class attributes such as the X and y data, the optimal parameters, and the optimal F0.5 Score.

* *model_<model_name>.json* : Each of the tested models was fitted on the 70% training set represented by *gaps_training_set.csv* for the creation of certain figures. These models were each saved out to their own model file with all of the class attributes.

### Static Version of Cross Validation Notebook
* *model_selection_v20210816.html* : A static HTML export of the model selection Jupyter notebook run that created the results saved in this folder, for reference.