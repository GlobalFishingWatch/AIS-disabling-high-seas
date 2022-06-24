import argparse
import os
import json
import time
import pandas as pd
import sklearn.metrics as skm

from sklearn.model_selection import GroupShuffleSplit
from ais_disabling.threshold_models import SingleThresholdClassifier, DoubleThresholdClassifier
from ais_disabling.model_utils import (
    get_models_folder,
    get_gaps,
    create_training_set,
    generate_kfolds,
    cross_validate_threshold_model,
    results_to_json,
    fit_threshold_model
)

from ais_disabling.config import proj_dataset, gap_events_labeled_table

GAPS_LABELED = f"{proj_dataset}.{gap_events_labeled_table}"

DATA_FOLDER = "data"
SAVED_GAPS = f"{DATA_FOLDER}/labeled_gaps.csv"
LOAD_SAVED_GAPS = True


def run_model_selection(models_folder, lowest_rec, run_double=False):

    # Grab labeled gaps
    if LOAD_SAVED_GAPS and os.path.exists(SAVED_GAPS):
        df_gaps = pd.read_csv(SAVED_GAPS)
    else:
        df_gaps = get_gaps(GAPS_LABELED)
        df_gaps.to_csv(SAVED_GAPS)

    # Filter gaps based on off reception and distance from shore
    df_gaps = df_gaps[df_gaps.positions_per_day_off > lowest_rec].copy().reset_index(drop=True)

    print(f"Model selection is being run on {df_gaps.shape[0]} gaps.")
    assert(df_gaps.shape[0] > 0)

    ### Run Repeated KFold Cross Validation using GroupShuffleSplit to select best model

    ## TRAINING/TEST SET
    df_gaps_train, df_gaps_test = create_training_set(df_gaps, GroupShuffleSplit)
    print(
        f"The proportion of gaps used for testing and final performance metrics is actually {df_gaps_test.shape[0]/df_gaps_train.shape[0]:0.2f} due to the grouping"
    )

    # Save for reproducibility.
    df_gaps_train.to_csv(f"{models_folder}/gaps_training_set.csv")
    df_gaps_test.to_csv(f"{models_folder}/gaps_test_set.csv")

    ## MODEL SETUP
    ## Generate the Kfolds using GroupShuffleSplit.
    ## We want them to be the same for every model.
    ## We want num_repeats of them to be able to do Repeated KFold.
    ## The random_state seeds are set for reproducibility
    ## and are generated by their own random generator with
    ## a set seed.
    N_SPLITS = 10
    NUM_REPEATS = 10

    # Seed the random number generator and choose random_state values.
    gss_list = generate_kfolds(GroupShuffleSplit, N_SPLITS, NUM_REPEATS)

    ## Specify the groups that will be used for generating the folds.
    groups = df_gaps_train.ssvid.to_numpy()

    ## Create a custom scorer based on F0.5 Score
    fhalf_scorer = skm.make_scorer(skm.fbeta_score, beta=0.5, average="binary")

    ## Set whether the cross validation should return the estimator instances.
    ## Set True for debugging and investigation purposes.
    ## Otherwise set False to speed up execution.
    return_estimator = False

    y = df_gaps_train.actual_gap_class.to_numpy()

    ## 12hb model
    X = df_gaps_train[["positions_12_hours_before_sat"]].to_numpy()
    cv_results_12hb = cross_validate_threshold_model(X, y, SingleThresholdClassifier, NUM_REPEATS, gss_list, return_estimator, fhalf_scorer, groups, lowest_rec)

    ## 18hb model
    X = df_gaps_train[["positions_18_hours_before_sat"]].to_numpy()
    cv_results_18hb = cross_validate_threshold_model(X, y, SingleThresholdClassifier, NUM_REPEATS, gss_list, return_estimator, fhalf_scorer, groups, lowest_rec)

    ## 24hb model
    X = df_gaps_train[["positions_24_hours_before_sat"]].to_numpy()
    cv_results_24hb = cross_validate_threshold_model(X, y, SingleThresholdClassifier, NUM_REPEATS, gss_list, return_estimator, fhalf_scorer, groups, lowest_rec)

    ## rec_only model
    X = df_gaps_train[["positions_per_day_off"]].to_numpy()
    cv_results_rec_only = cross_validate_threshold_model(X, y, SingleThresholdClassifier, NUM_REPEATS, gss_list, return_estimator, fhalf_scorer, groups, lowest_rec)
    
    results_json = {
        "cv_results_12hb": results_to_json(cv_results_12hb),
        "cv_results_18hb": results_to_json(cv_results_18hb),
        "cv_results_24hb": results_to_json(cv_results_24hb),
        "cv_results_rec_only": results_to_json(cv_results_rec_only),
    }

    if run_double:
        start_time = time.time()

        # rec_12hb model
        X = df_gaps_train[
            ["positions_per_day_off", "positions_12_hours_before_sat"]
        ].to_numpy()
        cv_results_rec_12hb = cross_validate_threshold_model(X, y, DoubleThresholdClassifier, NUM_REPEATS, gss_list, return_estimator, fhalf_scorer, groups, lowest_rec)
        results_json['cv_results_rec_12hb'] = results_to_json(cv_results_rec_12hb)

        # rec_18hb model
        X = df_gaps_train[
            ["positions_per_day_off", "positions_18_hours_before_sat"]
        ].to_numpy()
        cv_results_rec_18hb = cross_validate_threshold_model(X, y, DoubleThresholdClassifier, NUM_REPEATS, gss_list, return_estimator, fhalf_scorer, groups, lowest_rec)
        results_json['cv_results_rec_18hb'] = results_to_json(cv_results_rec_18hb)

        # rec_24hb model
        X = df_gaps_train[
            ["positions_per_day_off", "positions_24_hours_before_sat"]
        ].to_numpy()
        cv_results_rec_24hb = cross_validate_threshold_model(X, y, DoubleThresholdClassifier, NUM_REPEATS, gss_list, return_estimator, fhalf_scorer, groups, lowest_rec)
        results_json['cv_results_rec_24hb'] = results_to_json(cv_results_rec_24hb)
        
        end_time = time.time()
        print(start_time, end_time)
        print("TIME LAPSED:", (end_time - start_time)/(60*60))

    # WRITE OUT MODEL RESULTS
    try:
        outfile_cv_results = f"{models_folder}/cv_results.json"
        with open(outfile_cv_results, "w") as outfile:
            json.dump(results_json, outfile)
    except:
        outfile_text_results = f"{models_folder}/cv_results.txt"
        print(f"ERROR: Invalid JSON, dumping as string to {outfile_text_results} instead")
        with open(outfile_text_results, "w") as outfile:
            outfile.write(str(results_json))


    # Fit each of the models to the training set for publication figures

    # 12hb
    X = df_gaps_train[["positions_12_hours_before_sat"]].to_numpy()
    model_12hb = fit_threshold_model(X, y, SingleThresholdClassifier, model_name='12hb', lowest_rec=lowest_rec)
    model_12hb.save(f"{models_folder}/model_12hb.json")

    # 18hb
    X = df_gaps_train[["positions_18_hours_before_sat"]].to_numpy()
    model_18hb = fit_threshold_model(X, y, SingleThresholdClassifier, model_name='18hb', lowest_rec=lowest_rec)
    model_18hb.save(f"{models_folder}/model_18hb.json")

    # 24hb
    X = df_gaps_train[["positions_24_hours_before_sat"]].to_numpy()
    model_24hb = fit_threshold_model(X, y, SingleThresholdClassifier, model_name='24hb', lowest_rec=lowest_rec)
    model_24hb.save(f"{models_folder}/model_24hb.json")

    # rec_only
    X = df_gaps_train[["positions_per_day_off"]].to_numpy()
    model_rec_only = fit_threshold_model(X, y, SingleThresholdClassifier, model_name='rec_only', lowest_rec=lowest_rec)
    model_rec_only.save(f"{models_folder}/model_rec_only.json")

    if run_double:
        # rec_12hb
        X = df_gaps_train[
            ["positions_per_day_off", "positions_12_hours_before_sat"]
        ].to_numpy()        
        model_rec_12hb = fit_threshold_model(X, y, DoubleThresholdClassifier, model_name='rec_12hb', lowest_rec=lowest_rec)
        model_rec_12hb.save(f"{models_folder}/model_rec_12hb.json")

        # rec_18hb
        X = df_gaps_train[
            ["positions_per_day_off", "positions_18_hours_before_sat"]
        ].to_numpy()        
        model_rec_18hb = fit_threshold_model(X, y, DoubleThresholdClassifier, model_name='rec_12hb', lowest_rec=lowest_rec)
        model_rec_18hb.save(f"{models_folder}/model_rec_18hb.json")

        # rec_24hb
        X = df_gaps_train[
            ["positions_per_day_off", "positions_24_hours_before_sat"]
        ].to_numpy()        
        model_rec_24hb = fit_threshold_model(X, y, DoubleThresholdClassifier, model_name='rec_12hb', lowest_rec=lowest_rec)
        model_rec_24hb.save(f"{models_folder}/model_rec_24hb.json")


if __name__ == "__main__":


    parser = argparse.ArgumentParser()

    # The minimum reception at which gaps will be considered.
    # All gaps under this reception are discarded for model
    # selection and the final dataset.
    parser.add_argument('--lowest_rec', type=int, required=True)

    # Determines whether the higher computation models should be run.
    # Set to False when debugging or testing out new
    # model selection architectures to save time.
    parser.add_argument('--run_double', type=bool, required=False)

    args = parser.parse_args()
    lowest_rec = args.lowest_rec
    if not lowest_rec:
        lowest_rec = 10
        
    run_double = args.run_double
    if not run_double:
        run_double = False

    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    models_folder = get_models_folder(lowest_rec)

    print(f"Running model selection:\n\tlowest_rec = {lowest_rec}\n\tmodels_folder = {models_folder}")
    run_model_selection(models_folder, lowest_rec, run_double)