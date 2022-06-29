
## Running model selection
From the model_selection folder, run
`python model_selection_clean --lowest_rec 10 [--run_double True]`

Remove --run_double True to only run single threshold models. If --run_double is set to True, one run will take around 4.5 hours on a quad-core machine.

The saved model trained for the paper using `python model_selection_clean --lowest_rec 10 --run_double True` can be found in `models/model_final_v20220606.zip`. In order to run the figures script in `analysis/`, you will need to unzip this file and either rename the resulting folder to `models_10ppd` or edit the `figs_model_selection.py` to point to a specific folder. 

## Running sensitivity analysis
From model_selection folder, run
`bash sensitivity_analysis/run_models.bash`
to run the single threshold models for receptions 0 through 60 by steps of 5.

Then you can open up the notebook in `sensitivity_analysis/` to redo the figures.



