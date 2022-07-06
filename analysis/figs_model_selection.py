import argparse
import json
import pandas as pd

from ais_disabling.threshold_models import SingleThresholdClassifier, DoubleThresholdClassifier

from ais_disabling.model_utils import get_models_folder
from ais_disabling.figure_utils import (
    get_figures_folder,
    model_response_curves,
    pretty_confusion_matrix,
    model_descriptions_table,
    optimal_models_table,
    gap_lat_lon_distributions,
    gap_geartype_distributions,
    reception_response_curves,
    model_results_distribution,
    stats_for_paper,
)

from ais_disabling.config import proj_dataset, gap_events_features_table, gap_events_labeled_table

GAPS_FEATURES = f"{proj_dataset}.{gap_events_features_table}"
GAPS_LABELED = f"{proj_dataset}.{gap_events_labeled_table}"


def model_selection_figures(models_folder, images_folder, lowest_rec, run_double=False):
    
    # Load in the test data and reinstatiate the models.
    df_gaps_test = pd.read_csv(f"{models_folder}/gaps_test_set.csv", index_col=0)

    # Load in the fitted models
    model_12hb = SingleThresholdClassifier()
    model_12hb.load(f"{models_folder}/model_12hb.json")

    model_18hb = SingleThresholdClassifier()
    model_18hb.load(f"{models_folder}/model_18hb.json")

    model_24hb = SingleThresholdClassifier()
    model_24hb.load(f"{models_folder}/model_24hb.json")

    model_rec_only = SingleThresholdClassifier(model_name="rec_only")
    model_rec_only.load(f"{models_folder}/model_rec_only.json")

    models = [model_12hb, model_18hb, model_24hb, model_rec_only]

    if run_double:
        model_rec_12hb = DoubleThresholdClassifier()
        model_rec_12hb.load(f"{models_folder}/model_rec_12hb.json")

        model_rec_18hb = DoubleThresholdClassifier()
        model_rec_18hb.load(f"{models_folder}/model_rec_18hb.json")

        model_rec_24hb = DoubleThresholdClassifier()
        model_rec_24hb.load(f"{models_folder}/model_rec_24hb.json")

        models.extend([model_rec_12hb, model_rec_18hb, model_rec_24hb])

    model_response_curves(*models, images_folder=images_folder)

    # Identify optimal model
    optimal_score = 0
    optimal_model = None
    for model in [model_12hb, model_18hb, model_24hb, model_rec_only]:
        if model.optimal_score_ > optimal_score:
            optimal_score = model.optimal_score_
            optimal_model = model
    print(f"Optimal Model: {optimal_model.model_name}, k: {optimal_model.k_}")
    
    X_test = df_gaps_test[["positions_12_hours_before_sat"]].to_numpy()
    y_true = df_gaps_test.actual_gap_class.to_numpy()
    y_pred = optimal_model.predict(X_test)
    pretty_confusion_matrix(y_true, y_pred, images_folder=images_folder)

    model_descriptions_table(images_folder=images_folder)

    ## Load the cv_results json as a dictionary.
    with open(f"{models_folder}/cv_results.json") as cv_results_file:
        cv_results = json.load(cv_results_file)
    optimal_models_table(cv_results, images_folder=images_folder)
    model_results_distribution(cv_results, images_folder=images_folder)

    gap_lat_lon_distributions(GAPS_LABELED, GAPS_FEATURES, lowest_rec=lowest_rec, images_folder=images_folder)
    gap_geartype_distributions(GAPS_LABELED, GAPS_FEATURES, lowest_rec=lowest_rec, images_folder=images_folder)

    if run_double:
        reception_response_curves(model_rec_12hb, model_rec_18hb, model_rec_24hb, images_folder=images_folder)
    
    stats_for_paper(GAPS_FEATURES, lowest_rec=lowest_rec, model_column='positions_12_hours_before_sat', ping_thresh=optimal_model.k_)


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
    run_double = args.run_double
    if not run_double:
        run_double = False

    models_folder = get_models_folder(lowest_rec)
    images_folder = get_figures_folder(lowest_rec)
    
    print(f"Creating publication images for lowest_rec={lowest_rec}:\n\tfrom models in {models_folder}\n\tto this folder {images_folder}")
    model_selection_figures(models_folder, images_folder, lowest_rec, run_double)