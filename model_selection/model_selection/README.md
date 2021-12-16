# Description

This folder contains the code to run model selection for a model to predict if a gap is intentional disabling.  
To run the model selection, run the `model_selection.ipynb` notebook, which can be created from `model_selection.py` using the `jupytext` library.  
Please note, the code currently requires access to the Global Fishing Watch BigQuery project. However, all model outputs necessary to replicate the model selection process independently of the code in this fodler are described below in the **Saved Model** section.

# Files

### Model Selection
* *threshold_models.py* : The custom model classes that extend scikit-learn classes.

* *model_selection.py* : A jupytext file to be open as a Jupyter notebook and run to reproduce the model results. A static version is saved in *models_v20210816/*.


### Figures and Statistics for the Paper
* *publication_images.py* : A jupytext file to be open as a Jupyter notebook and run to reproduce the images for the paper. A static version of the models used int he paper are saved in *models_v20210816/*.


### Saved Model
* *models_v20210816/* : The model results used for the paper. See the README.md provided in that folder for more information.


# Nomenclature Note
Based on reviewer feedback, `X` was changed to `t` throughout the paper submission to conform to standards in the field but was kept as `X` in the code base except when used to produce figures for the paper. So `thb` references the same model as `Xhb`, etc.

