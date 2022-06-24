from gettext import npgettext
import os
import matplotlib
import matplotlib.pyplot as plt
import sklearn.metrics as skm
import seaborn as sns
import numpy as np
import pandas as pd

# Default styling
import pyseas.maps as psm
psm.use(psm.styles.chart_style)

CHART_COLORS = [
    "#204280",
    "#742980",
    "#ad2176",
    "#d73b68",
    "#ee6256",
    "#f68d4b",
    "#f8ba47",
    "#ebe55d",
]
CHART_COLORS_TINT = [
    "#989abc",
    "#b79abe",
    "#d6a2bb",
    "#ebaeb4",
    "#f8bdad",
    "#fdceac",
    "#fde0b1",
    "#f5f3bf",
]
COLOR_DARK_BLUE = "#204280"
COLOR_ORANGE = "#ee6256"


FIGURES_BASE_FOLDER = './figures/'

def get_figures_folder(lowest_rec):
    if not os.path.exists(FIGURES_BASE_FOLDER):
        os.makedirs(FIGURES_BASE_FOLDER)

    return FIGURES_BASE_FOLDER

def get_gridded_gaps(gaps_table, lowest_rec):
    q = f"""
    WITH

    gaps as (
        SELECT
            FLOOR(off_lat * 10) as lat_bin,
            FLOOR(off_lon * 10) as lon_bin,
        FROM
            `{gaps_table}`
        WHERE
            gap_hours >= 12
            AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
            AND off_distance_from_shore_m >= 50*1852
            AND positions_per_day_off > {lowest_rec}
    )

    SELECT 
        lat_bin,
        lon_bin,
        COUNT(*) as num_gaps
    FROM
        gaps
    GROUP BY lat_bin, lon_bin
    """

    return pd.read_gbq(q, project_id="world-fishing-827", dialect="standard")

def model_response_curves(model_12hb, model_18hb, model_24hb, model_rec_only, model_rec_12hb=None, model_rec_18hb=None, model_rec_24hb=None, images_folder=None):
    fig = plt.figure(figsize=(8, 6), dpi=300)

    # Check if figure should include double threshold models
    run_double = False
    if model_rec_12hb and model_rec_18hb and model_rec_24hb:
        run_double = True

    if run_double:
        for score_at_reception in model_rec_12hb.threshold_scores_:
            plt.plot(
                model_rec_12hb.test_thresholds_pings_,
                score_at_reception,
                "-",
                markersize=3,
                linewidth=0.5,
                color=CHART_COLORS_TINT[0],
                zorder=10,
            )

        for score_at_reception in model_rec_18hb.threshold_scores_:
            plt.plot(
                model_rec_18hb.test_thresholds_pings_,
                score_at_reception,
                "-",
                markersize=3,
                linewidth=0.5,
                color=CHART_COLORS_TINT[2],
                zorder=9,
            )

        for score_at_reception in model_rec_24hb.threshold_scores_:
            plt.plot(
                model_rec_24hb.test_thresholds_pings_,
                score_at_reception,
                "-",
                markersize=3,
                linewidth=0.5,
                color=CHART_COLORS_TINT[4],
                zorder=8,
            )

    line_12hb = plt.plot(
        model_12hb.test_thresholds_,
        model_12hb.threshold_scores_,
        "-",
        markersize=3,
        linewidth=1.5,
        color=CHART_COLORS[0],
        label="12hb",
        zorder=10,
    )
    line_18hb = plt.plot(
        model_18hb.test_thresholds_,
        model_18hb.threshold_scores_,
        "-",
        markersize=3,
        linewidth=1.5,
        color=CHART_COLORS[2],
        label="18hb",
        zorder=9,
    )
    line_24hb = plt.plot(
        model_24hb.test_thresholds_,
        model_24hb.threshold_scores_,
        "-",
        markersize=3,
        linewidth=1.5,
        color=CHART_COLORS[4],
        label="24hb",
        zorder=8,
    )
    line_rec_only = plt.plot(
        model_rec_only.test_thresholds_[model_rec_only.lowest_rec-1:],
        model_rec_only.threshold_scores_[model_rec_only.lowest_rec-1:],
        ":",
        markersize=3,
        linewidth=1.5,
        color="#3c3c3b",
        label="Reception only",
        zorder=11,
    )

    plt.xlabel("Threshold")
    plt.ylabel("F0.5 Score")
    plt.xlim(0, 60)
    plt.ylim(0.55, 0.8)


    handles = [
        line_12hb[0],
        line_18hb[0],
        line_24hb[0],
        line_rec_only[0],
    ]
    if run_double:
        patch_12hb = matplotlib.lines.Line2D(
            [0], [0], color=CHART_COLORS_TINT[6], linewidth=0.8, label=f"rec_12hb  (j = {model_rec_12hb.lowest_rec+1} to 60)"
        )
        patch_18hb = matplotlib.lines.Line2D(
            [0], [0], color=CHART_COLORS_TINT[4], linewidth=0.8, label=f"rec_18hb (j = {model_rec_12hb.lowest_rec+1} to 60)"
        )
        patch_24hb = matplotlib.lines.Line2D(
            [0], [0], color=CHART_COLORS_TINT[2], linewidth=0.8, label=f"rec_24hb (j = {model_rec_24hb.lowest_rec+1} to 60)"
        )
        handles.extend([patch_12hb, patch_18hb, patch_24hb])
    legend = plt.legend(handles=handles, bbox_to_anchor=(1.01, 0.61), loc="upper left")
    texts = plt.setp(legend.get_texts(), color="#848B9B")

    fig.patch.set_facecolor("white")
    plt.gca().set_facecolor("white")
    if images_folder:
        plt.savefig(
            f"{images_folder}/gap_classification_model_performance.png",
            dpi=300,
            bbox_inches="tight",
        )
    return fig

def pretty_confusion_matrix(y_true, y_pred, title=None, images_folder=None):
    fig = plt.figure(figsize=(7, 7), dpi=300)
    cf_matrix = skm.confusion_matrix(y_true, y_pred, labels=[1, 0])

    metrics = skm.precision_recall_fscore_support(y_true, y_pred, average="binary")
    accuracy = skm.accuracy_score(y_true, y_pred)
    f_half = skm.fbeta_score(y_true, y_pred, beta=0.5, average="binary")

    acc_text = "\n\nAccuracy={:0.3f}\nPrecision={:0.3f}\nRecall={:0.3f}\nF1 Score={:0.3f}\nF0.5 Score={:0.3f}".format(
        accuracy, metrics[0], metrics[1], metrics[2], f_half
    )

    group_names = ["True Positive", "False Negative", "False Positive", "True Negative"]
    group_counts = ["{0:,d}".format(value) for value in cf_matrix.flatten()]
    group_percentages = [
        "{0:.2%}".format(value) for value in cf_matrix.flatten() / np.sum(cf_matrix)
    ]
    labels = [
        f"{v1}\n{v2}\n{v3}"
        for v1, v2, v3 in zip(group_names, group_counts, group_percentages)
    ]
    labels = np.asarray(labels).reshape(2, 2)

    categories = ["True gap", "False gap"]
    sns.heatmap(
        cf_matrix,
        annot=labels,
        fmt="",
        cmap="Blues",
        cbar=None,
        xticklabels=categories,
        yticklabels=categories,
        annot_kws={"size": 14},
    )
    plt.ylabel("Test set label")
    plt.xlabel("Predicted label" + acc_text)
    fig.patch.set_facecolor("white")
    plt.gca().set_facecolor("white")
    if title:
        plt.title(title)
    plt.tight_layout()

    if images_folder:
        plt.savefig(
            f"{images_folder}/confusion_matrix.png",
            dpi=300,
            bbox_inches="tight",
        )
    return fig


def model_descriptions_table(images_folder=None):
    desc_data = {
        "Model ID": [
            "12hb",
            "18hb",
            "24hb",
            "rec_only",
            "rec_12hb",
            "rec_18hb",
            "rec_24hb",
        ],
        "Description": [
            "number of positions 12 hours before gap >= k*",
            "number of positions 18 hours before gap >= k*",
            "number of positions 24 hours before gap >= k*",
            "reception at gap start >= j**",
            "reception at gap start >= j and number of positions 12 hours before gap >= k",
            "reception at gap start >= j and number of positions 18 hours before gap >= k",
            "reception at gap start >= j and number of positions 24 hours before gap >= k",
        ],
    }

    df_desc = pd.DataFrame(desc_data)
    if images_folder:
        df_desc.to_csv(f"{images_folder}/model_descriptions.csv")
    return df_desc

def optimal_models_table(cv_results, images_folder=None):
    ## Create a dataframe for visualization.
    model_names = []
    model_scores = []
    for model in cv_results:
        scores_for_model = (
            np.asarray([cv_repeat["test_score"] for cv_repeat in cv_results[model]])
            .flatten()
            .tolist()
        )
        model_names.extend([model] * len(scores_for_model))
        model_scores.extend(scores_for_model)

    df_cv_results = pd.DataFrame(data={"model_name": model_names, "score": model_scores})

    # Format for table visualization
    df_cv_model_scores = df_cv_results.groupby("model_name").mean()
    df_cv_model_scores.score = df_cv_model_scores.score.round(4)
    df_cv_model_scores = df_cv_model_scores.merge(
        df_cv_results.groupby("model_name").sem().rename(columns={"score": "std_error"}),
        left_index=True,
        right_index=True,
    ).reset_index()
    df_cv_model_scores.std_error = df_cv_model_scores.std_error.round(5)
    df_cv_model_scores.model_name = df_cv_model_scores.model_name.apply(
        lambda x: x[11:].replace("X", "t")
    )
    df_cv_model_scores.rename(
        {
            "model_name": "Model ID",
            "score": "Cross Validation Score",
            "std_error": "Standard Error",
        },
        axis="columns",
        inplace=True,
    )

    if images_folder:
        df_cv_model_scores.to_csv(f"{images_folder}/cv_scores_per_model.csv")
    return df_cv_model_scores


def gap_lat_lon_distributions(gaps_train_table, gaps_all_table, lowest_rec, images_folder=None):

    df_gaps_train_gridded = get_gridded_gaps(gaps_train_table, lowest_rec)
    df_gaps_all_gridded = get_gridded_gaps(gaps_all_table, lowest_rec)

    ## Group gaps by one degree instead of tenth degree.
    df_gaps_train_grouped = df_gaps_train_gridded.copy()
    df_gaps_train_grouped.lat_bin = np.floor(df_gaps_train_grouped.lat_bin / 10)
    df_gaps_train_grouped.lon_bin = np.floor(df_gaps_train_grouped.lon_bin / 10)

    df_gaps_all_grouped = df_gaps_all_gridded.copy()
    df_gaps_all_grouped.lat_bin = np.floor(df_gaps_all_grouped.lat_bin / 10)
    df_gaps_all_grouped.lon_bin = np.floor(df_gaps_all_grouped.lon_bin / 10)

    ## Sum all gaps for each degree of latitude and longitude
    lat_train = df_gaps_train_grouped.groupby("lat_bin").sum()[["num_gaps"]]
    lat_all = df_gaps_all_grouped.groupby("lat_bin").sum()[["num_gaps"]]

    lon_train = df_gaps_train_grouped.groupby("lon_bin").sum()[["num_gaps"]]
    lon_all = df_gaps_all_grouped.groupby("lon_bin").sum()[["num_gaps"]]

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10), dpi=300)

    ## Latitude
    ax1.bar(
        x=lat_train.index,
        height=lat_train.num_gaps,
        width=1,
        color=COLOR_DARK_BLUE,
        edgecolor=COLOR_DARK_BLUE,
    )
    ax2.bar(
        x=lat_all.index,
        height=lat_all.num_gaps,
        width=1,
        color=COLOR_DARK_BLUE,
        edgecolor=COLOR_DARK_BLUE,
    )
    ax1.set_xlabel("Degree of latitude")
    ax2.set_xlabel("Degree of latitude")
    ax1.set_ylabel("Number of gaps per $\mathregular{km^2}$")

    ## Longitude
    ax3.bar(
        x=lon_train.index,
        height=lon_train.num_gaps,
        width=1,
        color=COLOR_DARK_BLUE,
        edgecolor=COLOR_DARK_BLUE,
    )
    ax4.bar(
        x=lon_all.index,
        height=lon_all.num_gaps,
        width=1,
        color=COLOR_DARK_BLUE,
        edgecolor=COLOR_DARK_BLUE,
    )

    ax3.set_xlabel("Degree of longitude")
    ax4.set_xlabel("Degree of longitude")
    ax3.set_ylabel("Number of gaps per $\mathregular{km^2}$")


    # Labels
    plt.subplots_adjust(hspace=0.4)
    ax1.text(0.5,1.25, 'Training Gaps', ha='center', va='top', fontsize=24, transform=ax1.transAxes)
    ax2.text(0.5,1.25, 'All Gaps', ha='center', va='top', fontsize=24, transform=ax2.transAxes)
    ax1.text(-0.23,0.5, "Latitude", rotation=90, ha='center', va='center', fontsize=24, transform=ax1.transAxes)
    ax3.text(-0.23,0.5, "Longitude", rotation=90, ha='center', va='center', fontsize=24, transform=ax3.transAxes)

    ax1.text(-0.08, 1.15, "A.", ha='left', va='top', transform=ax1.transAxes, fontweight='bold', fontsize=20)
    ax2.text(-0.08, 1.15, "B.", ha='left', va='top', transform=ax2.transAxes, fontweight='bold', fontsize=20)
    ax3.text(-0.08, 1.15, "C.", ha='left', va='top', transform=ax3.transAxes, fontweight='bold', fontsize=20)
    ax4.text(-0.08, 1.15, "D.", ha='left', va='top', transform=ax4.transAxes, fontweight='bold', fontsize=20)

    # Final adjustments
    fig.patch.set_facecolor("white")
    ax1.set_facecolor("white")
    ax2.set_facecolor("white")
    ax3.set_facecolor("white")
    ax4.set_facecolor("white")

    if images_folder:
        plt.savefig(f"{images_folder}/gaps_by_lat_lon.png", dpi=300, bbox_inches="tight")

    return fig

def gap_geartype_distributions(gaps_train_table, gaps_all_table, lowest_rec, images_folder=None):
    q = f"""
    (
        SELECT
            vessel_class,
            'all' as gap_set_type,
            COUNT(*) as num_gaps,
        FROM
            `{gaps_all_table}`
        WHERE
            gap_hours >= 12
            AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
            AND off_distance_from_shore_m >= 50*1852
            AND positions_per_day_off > {lowest_rec}
        GROUP BY vessel_class
    )
    UNION ALL
    (
        SELECT
            vessel_class,
            'train' as gap_set_type,
            COUNT(*) as num_gaps
        FROM
            `{gaps_train_table}`
        WHERE
            gap_hours >= 12
            AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
            AND off_distance_from_shore_m >= 50*1852
            AND positions_per_day_off > {lowest_rec}
        GROUP BY vessel_class
    )
    """

    df_gaps_by_class = pd.read_gbq(q, project_id="world-fishing-827", dialect="standard")

    ## Add proportions
    total_gaps_train = df_gaps_by_class[
        df_gaps_by_class.gap_set_type == "train"
    ].num_gaps.sum()
    total_gaps_all = df_gaps_by_class[df_gaps_by_class.gap_set_type == "all"].num_gaps.sum()
    df_gaps_by_class["prop_gaps"] = np.where(
        df_gaps_by_class.gap_set_type == "all",
        df_gaps_by_class.num_gaps / total_gaps_all,
        df_gaps_by_class.num_gaps / total_gaps_train,
    )
    df_gaps_by_class["gap_set_name"] = np.where(
        df_gaps_by_class.gap_set_type == "all", "All gaps", "Training gaps"
    )

    # Create figure.
    fig = plt.figure()
    sns.set(
        rc={
            "figure.figsize": (6, 4),
            "figure.dpi": 300,
            "savefig.dpi": 300,
            "axes.facecolor": "white",
            "figure.facecolor": "white",
            "grid.color": "#E6E7EB",
        }
    )
    data=df_gaps_by_class.sort_values("prop_gaps", ascending=False)
    g = sns.catplot(
        kind="bar",
        data=data,
        x="vessel_class",
        y="prop_gaps",
        hue="gap_set_name",
        height=4,
        aspect=1.5,
        palette=[COLOR_DARK_BLUE, COLOR_ORANGE],
        legend=False,
    )
    plt.xticks(rotation=90)
    plt.xlabel("Gear Type", labelpad=10)
    plt.ylabel("Proportion of gaps")

    legend = plt.legend(
        bbox_to_anchor=(0.95, 0.4, 0, 0)
    )

    # Change the labels to proper names
    label_mapping = {'drifting_longlines': 'Drifting Longlines',
                    'squid_jigger': 'Squid Jiggers',
                    'trawlers': 'Trawlers',
                    'tuna_purse_seines': 'Tuna Purse Seines', 
                    'other': 'All Other Geartypes', 
                    }    
    # label_mapping = {'drifting_longlines': 'Drifting Longlines',
    #                 'squid_jigger': 'Squid Jiggers',
    #                 'trawlers': 'Trawlers',
    #                 'fishing': 'Fishing',
    #                 'tuna_purse_seines': 'Tuna Purse Seines', 
    #                 'set_longlines': 'Set Longlines', 
    #                 'pole_and_line': 'Pole and Line',
    #                 'purse_seines': 'Purse Seines', 
    #                 'set_gillnets': 'Set Gillnets', 
    #                 'fixed_gear': 'Fixed Gear', 
    #                 'other_purse_seines': 'Other Purse Seines',
    #                 'pots_and_traps': 'Pots and Traps', 
    #                 'trollers': 'Trollers', 
    #                 'gear': 'Gear', 
    #                 'dredge_fishing': 'Dredge Fishing',
    #                 'other_seines': 'Other Seines', 
    #                 'seiners': 'Seiners'
    #                 }
    ax = g.axes.flat[0]
    ax.set_xticklabels([label_mapping[xtick.get_text()] for xtick in ax.get_xticklabels()])

    if images_folder:
        plt.savefig(f"{images_folder}/gaps_by_vessel_class.png", dpi=300, bbox_inches="tight")
    return fig

def reception_response_curves(model_rec_12hb, model_rec_18hb, model_rec_24hb, images_folder=None):
    fig = plt.figure(figsize=(10, 7), dpi=300)

    plt.plot(
        model_rec_12hb.test_thresholds_rec_,
        [max(l) for l in model_rec_12hb.threshold_scores_],
        "-",
        linewidth=1.5,
        color=CHART_COLORS[0],
        label="rec_12hb",
    )
    plt.plot(
        model_rec_18hb.test_thresholds_rec_,
        [max(l) for l in model_rec_18hb.threshold_scores_],
        "-",
        linewidth=1.5,
        color=CHART_COLORS[2],
        label="rec_18hb",
    )
    plt.plot(
        model_rec_24hb.test_thresholds_rec_,
        [max(l) for l in model_rec_24hb.threshold_scores_],
        "-",
        linewidth=1.5,
        color=CHART_COLORS[4],
        label="rec_24hb",
    )

    plt.xlabel("Reception Threshold (j)")
    plt.ylabel("F0.5 Score")
    plt.xlim(0, 60)
    plt.ylim(0.7, 0.75)

    legend = fig.legend(bbox_to_anchor=(0.7, 0.85), loc="upper left")
    texts = plt.setp(legend.get_texts(), color="#848B9B")

    fig.patch.set_facecolor("white")
    plt.gca().set_facecolor("white")

    if images_folder:
        plt.savefig(f"{images_folder}/optimal_model_over_reception.png", dpi=300, bbox_inches="tight")
    return fig

def model_results_distribution(cv_results, images_folder=None):
    ## Create a dataframe for visualization.
    model_names = []
    model_scores = []
    for model in cv_results:
        scores_for_model = (
            np.asarray([cv_repeat["test_score"] for cv_repeat in cv_results[model]])
            .flatten()
            .tolist()
        )
        model_names.extend([model] * len(scores_for_model))
        model_scores.extend(scores_for_model)

    df_cv_results = pd.DataFrame(data={"model_name": model_names, "score": model_scores})

    # Box and Whisker plot
    fig = plt.figure(figsize=(8, 5), dpi=300)
    ax = sns.boxplot(x="model_name", y="score", data=df_cv_results, color="#c77ca1")
    plt.xticks(
        list(range(len(cv_results))),
        [key[11:] for key in cv_results],
        rotation=90,
    )
    fig.patch.set_facecolor("white")
    plt.gca().set_facecolor("white")
    plt.xlabel("Model")
    plt.ylabel("Score")

    plt.tight_layout()
    if images_folder:
        plt.savefig(f"{images_folder}/distribution_cv_scores.png", dpi=300, bbox_inches="tight")
    return fig

def stats_for_paper(gaps_table, lowest_rec, model_column, ping_thresh):
    q = f'''
    SELECT
        COUNT(*) AS num_gaps, 
        COUNT(DISTINCT ssvid) AS num_distinct_mmsi, 
        COUNT(DISTINCT flag) AS num_distinct_flags,
    FROM `{gaps_table}`
    WHERE gap_hours >= 12 
        AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
        AND off_distance_from_shore_m > 1852*50 
        AND positions_per_day_off > {lowest_rec} 
    '''
    
    df_labeled_gap_stats = pd.read_gbq(q, project_id="world-fishing-827", dialect="standard")
    print("\nLABELED GAPS STATS\n---------------------")
    print(f"NUM GAPS: {df_labeled_gap_stats.iloc[0].num_gaps}")
    print(f"DISTINCT MMSI: {df_labeled_gap_stats.iloc[0].num_distinct_mmsi}")
    print(f"FLAG STATES: {df_labeled_gap_stats.iloc[0].num_distinct_flags}")
    
