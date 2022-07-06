import os
import pandas as pd
import numpy as np
from random import seed, randint
from sklearn.model_selection import cross_validate

MODELS_BASE_FOLDER = "../model_selection/models"


def get_models_folder(lowest_rec):
    if not os.path.exists(MODELS_BASE_FOLDER):
        os.makedirs(MODELS_BASE_FOLDER)

    models_folder = f"{MODELS_BASE_FOLDER}/models_{lowest_rec}ppd"
    if not os.path.exists(models_folder):
        os.makedirs(models_folder)
    return models_folder

def get_gaps(gaps_table):
    q = f"""
    SELECT
    *
    FROM
    `{gaps_table}`
    WHERE
    (off_distance_from_shore_m > 50*1852) 
    """

    return pd.read_gbq(q, project_id="world-fishing-827", dialect="standard")

# Separate out training and test sets on 70-30 split, grouping by MMSI
# Since the gaps are grouped on MMSI, meaning that all of the gaps for an MMSI must either be fully in the training set OR the test set to prevent data leakage, getting an exact 70-30 split is not possible. The `test_size` parameter has been set to 0.22 after some experimentation as this gave the desired 70-30 split with `random_state` set to 5.
def create_training_set(df_gaps, cv_type):
    train_idx, test_idx = next(
        cv_type(test_size=0.22, n_splits=2, random_state=5).split(
            df_gaps, groups=df_gaps.ssvid.to_numpy()
        )
    )
    return df_gaps.iloc[train_idx], df_gaps.iloc[test_idx]

def generate_kfolds(cv_type, n_splits, num_repeats):
    seed(15)
    random_states = [randint(0, 100) for i in range(0, num_repeats)]
    return [
        cv_type(n_splits=n_splits, test_size=0.1, random_state=random_states[i])
        for i in range(0, num_repeats)
    ]

def cross_validate_threshold_model(X, y, model_class, num_repeats, gss_list, return_estimator, scorer, groups, lowest_rec):

    cv_results = []
    for i in range(0, num_repeats):
        cv_results.append(
            cross_validate(
                model_class(lowest_rec=lowest_rec),
                X,
                y,
                cv=gss_list[i],
                return_estimator=return_estimator,
                scoring=scorer,
                groups=groups,
                n_jobs=-1,
            )
        )
    return cv_results

def results_to_json(cv_results, decimals=8):
    """Converts the cross validation results to a valid JSON object,
        changing numpy array objects to python list objects.
        Rounds to 8 decimal points by default for reduced file size.

    Parameters
    ----------
    cv_results : dict
        The results from running `cross_validate()`.
    decimales : int, default=8
        Number of significant demical points to round the values
        in the result arrays to.

    Returns
    ----------
    The cross validation results in valid JSON format.
    """
    cv_results_json = []
    for result in cv_results:

        result_json = {}
        for key in result.keys():
            result_json[key] = np.around(result[key], decimals=decimals).tolist()
        cv_results_json.append(result_json)

    return cv_results_json

def fit_threshold_model(X, y, model_class, model_name, lowest_rec):
    model = model_class(model_name=model_name, lowest_rec=lowest_rec)
    model.fit(X, y)
    return model
