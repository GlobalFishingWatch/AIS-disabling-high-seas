# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: gfw-rad-test
#     language: python
#     name: gfw-rad-test
# ---

# %%
import os
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import matplotlib.cm as mpcm
import sklearn.metrics as skm
from scipy.stats import sem
import seaborn as sns
import json
import imgkit

import sys
sys.path.append('../model_selection/model_selection/')
from threshold_models import SingleThresholdClassifier, DoubleThresholdClassifier

# Default styling
import pyseas.maps as psm
psm.use(psm.styles.chart_style)

## Change the path to the models_folder if saved elsewhere
models_folder = "../model_selection/model_selection/models_v20210816"
images_folder = "figures"

# Make the images folder, if necessary
if not os.path.exists(images_folder):
    os.makedirs(images_folder)

# %% [markdown]
# # Setup

# %% [markdown]
# ## Visualization settings
#
# Colors from the GFW Data Guidelines.

# %%
# Parameters used for visualizations
color_dark_pink = "#d73b68"
color_gray = "#b2b2b2"
color_dark_blue = "#204280"
color_purple = "#742980"
color_light_pink = "#d73b68"
color_orange = "#ee6256"
color_light_orange = "#f8ba47"
color_yellow = "#ebe55d"

# %% [markdown]
# ## Load the test set and the models

# %%
# Load in the test data and reinstatiate the models.
df_gaps_test = pd.read_csv(f"{models_folder}/gaps_test_set.csv", index_col=0)

# Final model fitted to the training data
final_model = SingleThresholdClassifier(model_name="final_model")
final_model.load(f"{models_folder}/final_model.json")

# Each model fitted to the traini
model_12hb = SingleThresholdClassifier(model_name="12hb")
model_12hb.load(f"{models_folder}/model_12hb.json")

model_12hba = SingleThresholdClassifier(model_name="12hba")
model_12hba.load(f"{models_folder}/model_12hba.json")

model_Xhb = SingleThresholdClassifier(model_name="Xhb")
model_Xhb.load(f"{models_folder}/model_Xhb.json")

model_Xhba = SingleThresholdClassifier(model_name="Xhba")
model_Xhba.load(f"{models_folder}/model_Xhba.json")

model_rec_only = SingleThresholdClassifier(model_name="rec_only")
model_rec_only.load(f"{models_folder}/model_rec_only.json")

model_rec_12hb = DoubleThresholdClassifier(model_name="rec_12hb")
model_rec_12hb.load(f"{models_folder}/model_rec_12hb.json")

model_rec_12hba = DoubleThresholdClassifier(model_name="rec_12hba")
model_rec_12hba.load(f"{models_folder}/model_rec_12hba.json")

model_rec_Xhb = DoubleThresholdClassifier(model_name="rec_Xhb")
model_rec_Xhb.load(f"{models_folder}/model_rec_Xhb.json")

model_rec_Xhba = DoubleThresholdClassifier(model_name="rec_Xhba")
model_rec_Xhba.load(f"{models_folder}/model_rec_Xhba.json")


# %% [markdown]
# # Generate figures for paper

# %% [markdown]
# ## Accuracy metrics

# %%
def accuracy_metrics_table(y_true, y_pred, model_name, round_to=2):

    acc_data = []
    metrics = skm.precision_recall_fscore_support(y_true, y_pred, average="binary")
    accuracy = skm.accuracy_score(y_true, y_pred)
    cf_matrix = skm.confusion_matrix(y_true, y_pred, labels=[1, 0])
    #         specificity = cf_matrix[1][1]/(cf_matrix[1][0]+cf_matrix[1][1])
    mcc = skm.matthews_corrcoef(y_true, y_pred)
    f_half = skm.fbeta_score(y_true, y_pred, beta=0.5, average="binary")
    num_real = y_pred[y_pred == 1].shape[0]
    num_not = y_pred[y_pred == 0].shape[0]
    acc_data.append(
        {
            "model": model_name,
            #                          'model_name': col_names_short[col],
            "precision": metrics[0].round(round_to),
            "recall": metrics[1].round(round_to),
            #                          'specificity': specificity.round(round_to),
            "f1_score": metrics[2].round(round_to),
            "accuracy": accuracy.round(round_to),
            "mcc": mcc.round(round_to),
            "fhalf_score": f_half.round(round_to),
            "num_real": num_real,
            "num_not": num_not,
        }
    )

    df_report = pd.DataFrame(acc_data)
    df_report.num_real = df_report.num_real.astype(int)
    df_report.num_not = df_report.num_not.astype(int)
    return df_report


# %%
def pretty_confusion_matrix(y_true, y_pred, title=None):
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
    )
    plt.ylabel("Test set label")
    plt.xlabel("Predicted label" + acc_text)
    fig.patch.set_facecolor("white")
    plt.gca().set_facecolor("white")
    if title:
        plt.title(title)
    plt.tight_layout()

    font = {"family": "DejaVu Sans", "weight": "normal", "size": 14}
    matplotlib.rc("font", **font)

    return fig


# %%
X_test = df_gaps_test[["positions_X_hours_before_sat"]].to_numpy()
y_true = df_gaps_test.actual_gap_class.to_numpy()
y_pred = final_model.predict(X_test)
accuracy_metrics_table(
    y_true, y_pred, model_name=f"thb: k={final_model.k_}", round_to=3
)

# %%
fig = pretty_confusion_matrix(
    y_true, y_pred#, f"Model: t hours before (thb) >= {final_model.k_}"
)
plt.savefig(f"{images_folder}/confusion_matrix.png", dpi=300, bbox_inches="tight")
plt.show()


# %% [markdown]
# ## Model response curves

# %%
chart_colors = [
    "#204280",
    "#742980",
    "#ad2176",
    "#d73b68",
    "#ee6256",
    "#f68d4b",
    "#f8ba47",
    "#ebe55d",
]
chart_colors_tint = [
    "#989abc",
    "#b79abe",
    "#d6a2bb",
    "#ebaeb4",
    "#f8bdad",
    "#fdceac",
    "#fde0b1",
    "#f5f3bf",
]

# %%
fig = plt.figure(figsize=(8, 6), dpi=300)

for score_at_reception in model_rec_12hb.threshold_scores_:
    plt.plot(
        model_rec_12hb.test_thresholds_pings_,
        score_at_reception,
        "-",
        markersize=3,
        linewidth=0.5,
        color=chart_colors_tint[6],
        zorder=1,
    )

for score_at_reception in model_rec_12hba.threshold_scores_:
    plt.plot(
        model_rec_12hba.test_thresholds_pings_,
        score_at_reception,
        "-",
        markersize=3,
        linewidth=0.5,
        color=chart_colors_tint[4],
        zorder=2,
    )

for score_at_reception in model_rec_Xhb.threshold_scores_:
    plt.plot(
        model_rec_Xhb.test_thresholds_pings_,
        score_at_reception,
        "-",
        markersize=3,
        linewidth=0.5,
        color=chart_colors_tint[2],
        zorder=4,
    )

for score_at_reception in model_rec_Xhba.threshold_scores_:
    plt.plot(
        model_rec_Xhba.test_thresholds_pings_,
        score_at_reception,
        "-",
        markersize=3,
        linewidth=0.5,
        color=chart_colors_tint[0],
        zorder=3,
    )

line_12hb = plt.plot(
    model_12hb.test_thresholds_,
    model_12hb.threshold_scores_,
    "--",
    markersize=3,
    linewidth=1.5,
    color=chart_colors[6],
    label="12hb",
    zorder=1,
)
line_12hba = plt.plot(
    model_12hba.test_thresholds_,
    model_12hba.threshold_scores_,
    "--",
    markersize=3,
    linewidth=1.5,
    color=chart_colors[4],
    label="12hba",
    zorder=2,
)
line_Xhb = plt.plot(
    model_Xhb.test_thresholds_,
    model_Xhb.threshold_scores_,
    "-",
    markersize=3,
    linewidth=1.5,
    color=chart_colors[2],
    label="thb",
    zorder=4,
)
line_Xhba = plt.plot(
    model_Xhba.test_thresholds_,
    model_Xhba.threshold_scores_,
    "-",
    markersize=3,
    linewidth=1.5,
    color=chart_colors[0],
    label="thba",
    zorder=3,
)
line_rec_only = plt.plot(
    model_rec_only.test_thresholds_,
    model_rec_only.threshold_scores_,
    ":",
    markersize=3,
    linewidth=1.5,
    color="#3c3c3b",
    label="Reception only",
    zorder=10,
)

# Plot the maximum F0.5 score
optimal_k = final_model.k_
optimal_score = final_model.optimal_score_
plt.scatter(
    x=[optimal_k], y=[optimal_score], marker=".", s=100, c=chart_colors[2], zorder=50
)
plt.annotate(
    "Best Model: thb >= %d" % optimal_k,
    (optimal_k, optimal_score),
    textcoords="offset points",
    xytext=(0, 7),
    ha="center",
    fontsize=12,
)

# plt.suptitle("Gap Classification Model Performance", y=0.93)
plt.xlabel("Threshold")
plt.ylabel("F0.5 Score")
plt.xlim(0, 60)
plt.ylim(0.5, 0.8)

patch_12hb = matplotlib.lines.Line2D(
    [0], [0], color=chart_colors_tint[6], linewidth=0.8, label="rec_12hb  (j = 6 to 40)"
)
patch_12hba = matplotlib.lines.Line2D(
    [0], [0], color=chart_colors_tint[4], linewidth=0.8, label="rec_12hba (j = 6 to 40)"
)
patch_Xhb = matplotlib.lines.Line2D(
    [0],
    [0],
    color=chart_colors_tint[2],
    linewidth=0.8,
    label="rec_thb     (j = 6 to 40)",
)
patch_Xhba = matplotlib.lines.Line2D(
    [0],
    [0],
    color=chart_colors_tint[0],
    linewidth=0.8,
    label="rec_thba   (j = 6 to 40)",
)
handles = [
    patch_12hb,
    patch_12hba,
    patch_Xhb,
    patch_Xhba,
    line_12hb[0],
    line_12hba[0],
    line_Xhb[0],
    line_Xhba[0],
    line_rec_only[0],
]
legend = plt.legend(handles=handles, bbox_to_anchor=(1.01, 0.61), loc="upper left")
texts = plt.setp(legend.get_texts(), color="#848B9B")

fig.patch.set_facecolor("white")
plt.gca().set_facecolor("white")
plt.savefig(
    f"{images_folder}/gap_classification_model_performance.png",
    dpi=300,
    bbox_inches="tight",
)

plt.show()


# %%
fig = plt.figure(figsize=(10, 7), dpi=300)

plt.plot(
    model_rec_12hb.test_thresholds_rec_,
    [max(l) for l in model_rec_12hb.threshold_scores_],
    "--",
    linewidth=1.5,
    color=chart_colors[6],
    label="rec_12hb",
)
plt.plot(
    model_rec_12hba.test_thresholds_rec_,
    [max(l) for l in model_rec_12hba.threshold_scores_],
    "--",
    linewidth=1.5,
    color=chart_colors[4],
    label="rec_12hba",
)
plt.plot(
    model_rec_Xhb.test_thresholds_rec_,
    [max(l) for l in model_rec_Xhb.threshold_scores_],
    "-",
    linewidth=1.5,
    color=chart_colors[2],
    label="rec_thb",
)
plt.plot(
    model_rec_Xhba.test_thresholds_rec_,
    [max(l) for l in model_rec_Xhba.threshold_scores_],
    "-",
    linewidth=1.5,
    color=chart_colors[0],
    label="rec_thba",
)

# plt.suptitle(
#     "Performance of optimal model\nat each reception threshold, 6≤j≤60", y=0.95
# )
plt.xlabel("Reception Threshold (j)")
plt.ylabel("F0.5 Score")
plt.xlim(0, 60)

legend = fig.legend(bbox_to_anchor=(0.7, 0.85), loc="upper left")
texts = plt.setp(legend.get_texts(), color="#848B9B")

fig.patch.set_facecolor("white")
plt.gca().set_facecolor("white")
plt.savefig(
    f"{images_folder}/optimal_model_over_reception.png", dpi=300, bbox_inches="tight"
)

plt.show()

# %% [markdown]
# ## Cross Validation Results

# %%
## Load the cv_results json as a dictionary.
with open(f"{models_folder}/cv_results.json") as cv_results_file:
    cv_results = json.load(cv_results_file)

# %%
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

# %% [markdown]
# #### Boxplot of the model scores for each model

# %%
fig = plt.figure(figsize=(8, 5), dpi=300)
ax = sns.boxplot(x="model_name", y="score", data=df_cv_results, color="#c77ca1")
plt.xticks(
    [0, 1, 2, 3, 4, 5, 6, 7, 8],
    [key[11:].replace("X", "t") for key in cv_results],
    rotation=90,
)
# plt.title("Distribution of cross validation scores by model", fontsize=16)
fig.patch.set_facecolor("white")
plt.gca().set_facecolor("white")
plt.xlabel("Model")
plt.ylabel("Score")

plt.tight_layout()
plt.savefig(f"{images_folder}/distribution_cv_scores.png", dpi=300, bbox_inches="tight")

plt.show()

# %% [markdown]
# # Description of models


# %%
desc_data = {
    "Model ID": [
        "12hb",
        "12hba",
        "thb",
        "thba",
        "rec_only",
        "rec_12hb",
        "rec_12hba",
        "rec_thb",
        "rec_thba",
    ],
    "Description": [
        "number of positions 12 hours before gap >= k*",
        "number of positions 12 hours before gap >= k and number of positions 12 hours after gap >= k",
        "number of positions t hours before gap >= k, where t is the length of the gap",
        "number of positions t hours before gap >= k and number of positions t hours after gap >= k, where t is the length of the gap",
        "reception at gap start >= j**",
        "reception at gap start >= j and number of positions 12 hours before gap >= k",
        "reception at gap start >= j and number of positions 12 hours before gap >= k and number of positions 12 hours after gap >= k",
        "reception at gap start >= j and number of positions t hours before gap >= k, where t is the length of the gap",
        "reception at gap start >= j and number of positions t hours before gap >= k and number of positions t hours after gap >= k, where t is the length of the gap",
    ],
}

df_desc = pd.DataFrame(desc_data)

# %%
df_desc.to_csv(f"{images_folder}/model_descriptions.csv")
df_desc

# %% [markdown]
# ### Optimal Models

# %%
df_cv_model_scores = df_cv_results.groupby("model_name").mean()
df_cv_model_scores.score = df_cv_model_scores.score.round(4)
df_cv_model_scores = df_cv_model_scores.merge(
    df_cv_results.groupby("model_name").sem().rename(columns={"score": "std_error"}),
    left_index=True,
    right_index=True,
).reset_index()
df_cv_model_scores.std_error = df_cv_model_scores.std_error.round(4)
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

# %%
df_cv_model_scores.to_csv(f"{images_folder}/cv_scores_per_model.csv")
df_cv_model_scores

# %%

# %% [markdown]
# ## Representativeness of training set

# %% [markdown]
# ### Geospatial distribution

# %%
gaps_training = (
    "world-fishing-827.proj_ais_gaps_catena.ais_gap_events_labeled_v20210722"
)
gaps_all = "world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722"

# %% [markdown]
# #### Pull training gaps

# %%
q = f"""
WITH

gaps as (
    SELECT
        FLOOR(off_lat * 10) as lat_bin,
        FLOOR(off_lon * 10) as lon_bin,
    FROM
        `{gaps_training}`
    WHERE
        (off_distance_from_shore_m >= 50*1852 AND on_distance_from_shore_m >= 50*1852)
        AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
)

SELECT 
    lat_bin,
    lon_bin,
    COUNT(*) as num_gaps
FROM
    gaps
GROUP BY lat_bin, lon_bin
"""

df_gaps_train_gridded = pd.read_gbq(
    q, project_id="world-fishing-827", dialect="standard"
)

# %% [markdown]
# #### Pull all gaps

# %%
q = f"""
WITH

gaps as (
    SELECT
        FLOOR(off_lat * 10) as lat_bin,
        FLOOR(off_lon * 10) as lon_bin,
    FROM
        `{gaps_all}`
    WHERE
        (off_distance_from_shore_m >= 50*1852 AND on_distance_from_shore_m >= 50*1852)
        AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
)

SELECT 
    lat_bin,
    lon_bin,
    COUNT(*) as num_gaps
FROM
    gaps
GROUP BY lat_bin, lon_bin
"""

df_gaps_gridded = pd.read_gbq(q, project_id="world-fishing-827", dialect="standard")

# %% [markdown]
# #### Map distribution of gap starts for each set

# %%
gaps_train_raster = psm.rasters.df2raster(
    df_gaps_train_gridded,
    "lon_bin",
    "lat_bin",
    "num_gaps",
    xyscale=10,
    scale=1,
    origin="lower",
    per_km2=True,
)
gaps_raster = psm.rasters.df2raster(
    df_gaps_gridded,
    "lon_bin",
    "lat_bin",
    "num_gaps",
    xyscale=10,
    scale=1,
    origin="lower",
    per_km2=True,
)

# %%
fig = plt.figure(figsize=(28, 28))

# Display a raster along with standard colorbar.
norm = mpcolors.LogNorm(vmin=0.001, vmax=0.1)
cmap = mpcm.get_cmap("viridis")
with plt.rc_context(psm.styles.light):
    ax, im = psm.plot_raster(
        gaps_train_raster, cmap=cmap, norm=norm, origin="lower", subplot=(2, 1, 1)
    )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title("Gaps in the training set", fontsize=20)
    cb = psm.colorbar.add_colorbar(
        im, ax=ax, label="Number of gaps per $\mathregular{km^2}$"
    )  # , loc=loc, hspace=hspace, wspace=wspace, format=cbformat)

# Display a raster along with standard colorbar.
norm = mpcolors.LogNorm(vmin=0.001, vmax=0.1)
with plt.rc_context(psm.styles.light):
    ax, im = psm.plot_raster(
        gaps_raster, cmap=cmap, norm=norm, origin="lower", subplot=(2, 1, 2)
    )
    psm.add_countries()
    psm.add_eezs()
    ax.set_title("All gaps", fontsize=20)
    cb = psm.colorbar.add_colorbar(
        im, ax=ax, label="Number of gaps per $\mathregular{km^2}$"
    )  # , loc=loc, hspace=hspace, wspace=wspace, format=cbformat)

plt.subplots_adjust(hspace=0.15)
fig.patch.set_facecolor("white")
plt.savefig(f"{images_folder}/gaps_training_vs_all.png", dpi=300, bbox_inches="tight")

# %% [markdown]
# #### Visualize the number of gaps for each degree of latitude and longitude

# %%
## Group gaps by one degree instead of tenth degree.
df_gaps_train_grouped = df_gaps_train_gridded.copy()
df_gaps_train_grouped.lat_bin = np.floor(df_gaps_train_grouped.lat_bin / 10)
df_gaps_train_grouped.lon_bin = np.floor(df_gaps_train_grouped.lon_bin / 10)

df_gaps_grouped = df_gaps_gridded.copy()
df_gaps_grouped.lat_bin = np.floor(df_gaps_grouped.lat_bin / 10)
df_gaps_grouped.lon_bin = np.floor(df_gaps_grouped.lon_bin / 10)

## Sum all gaps for each degree of latitude and longitude
lat_train = df_gaps_train_grouped.groupby("lat_bin").sum()[["num_gaps"]]
lat_all = df_gaps_grouped.groupby("lat_bin").sum()[["num_gaps"]]

lon_train = df_gaps_train_grouped.groupby("lon_bin").sum()[["num_gaps"]]
lon_all = df_gaps_grouped.groupby("lon_bin").sum()[["num_gaps"]]


# %%
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10), dpi=300)

## Latitude
ax1.bar(
    x=lat_train.index,
    height=lat_train.num_gaps,
    width=1,
    color=color_dark_blue,
    edgecolor=color_dark_blue,
)
ax2.bar(
    x=lat_all.index,
    height=lat_all.num_gaps,
    width=1,
    color=color_dark_blue,
    edgecolor=color_dark_blue,
)
ax1.set_xlabel("Degree of latitude")
ax2.set_xlabel("Degree of latitude")
ax1.set_ylabel("Number of gaps per $\mathregular{km^2}$")

## Longitude
ax3.bar(
    x=lon_train.index,
    height=lon_train.num_gaps,
    width=1,
    color=color_dark_blue,
    edgecolor=color_dark_blue,
)
ax4.bar(
    x=lon_all.index,
    height=lon_all.num_gaps,
    width=1,
    color=color_dark_blue,
    edgecolor=color_dark_blue,
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

plt.savefig(f"{images_folder}/gaps_by_lat_lon.png", dpi=300, bbox_inches="tight")

plt.show()

# %% [markdown]
# ### Geartype distribution


# %%
q = f"""
(
    SELECT
        vessel_class,
        'all' as gap_set_type,
        COUNT(*) as num_gaps,
    FROM
        `{gaps_all}`
    WHERE
        (off_distance_from_shore_m >= 50*1852 AND on_distance_from_shore_m >= 50*1852)
        AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
    GROUP BY vessel_class
)
UNION ALL
(
    SELECT
        vessel_class,
        'train' as gap_set_type,
        COUNT(*) as num_gaps
    FROM
        `{gaps_training}`
    WHERE
        (off_distance_from_shore_m >= 50*1852 AND on_distance_from_shore_m >= 50*1852)
        AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
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

# %% tags=[]
psm.use(psm.styles.chart_style)
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
    palette=[color_dark_blue, color_orange],
    legend=False,
)
plt.xticks(rotation=90)
plt.xlabel("Gear Type")
plt.ylabel("Proportion of gaps")
# plt.title("Distribution of geartype across gaps")

legend = plt.legend(
    bbox_to_anchor=(0.95, 0.4, 0, 0)
)

# Change the labels to proper names
label_mapping = {'drifting_longlines': 'Drifting Longlines',
                 'squid_jigger': 'Squid Jiggers',
                 'trawlers': 'Trawlers',
                 'fishing': 'Fishing',
                 'tuna_purse_seines': 'Tuna Purse Seines', 
                 'set_longlines': 'Set Longlines', 
                 'pole_and_line': 'Pole and Line',
                 'purse_seines': 'Purse Seines', 
                 'set_gillnets': 'Set Gillnets', 
                 'fixed_gear': 'Fixed Gear', 
                 'other_purse_seines': 'Other Purse Seines',
                 'pots_and_traps': 'Pots and Traps', 
                 'trollers': 'Trollers', 
                 'gear': 'Gear', 
                 'dredge_fishing': 'Dredge Fishing',
                 'other_seines': 'Other Seines', 
                 'seiners': 'Seiners'
                }
ax = g.axes.flat[0]
ax.set_xticklabels([label_mapping[xtick.get_text()] for xtick in ax.get_xticklabels()])


plt.savefig(f"{images_folder}/gaps_by_vessel_class.png", dpi=300, bbox_inches="tight")
plt.show()
# %%

# %%

# %%

