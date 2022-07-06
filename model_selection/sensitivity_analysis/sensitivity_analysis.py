# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3.9.6 ('rad')
#     language: python
#     name: python3
# ---

# %%
from ais_disabling.threshold_models import SingleThresholdClassifier

import pandas as pd
import matplotlib.pyplot as plt

# import ais_disabling
# ais_disabling._reload()
from ais_disabling.figure_utils import model_response_curves
from ais_disabling.config import proj_dataset, gap_events_features_table

GAPS_FEATURES = f"{proj_dataset}.{gap_events_features_table}"

models_folder = "./model_selection/models"
figures_folder = "./analysis/figures"



# %%
q = f"""
SELECT
gap_id,
gap_hours,
positions_per_day_off,
positions_per_day_on,
positions_12_hours_before_sat,
positions_18_hours_before_sat,
positions_24_hours_before_sat,
FROM
`{GAPS_FEATURES}` 
WHERE
gap_hours >= 12
AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
AND (off_distance_from_shore_m > 50*1852)
"""

df_all_gaps = pd.read_gbq(q, project_id="world-fishing-827", dialect="standard")

# %%
MODELS_FOLDER_0PPD = f"{models_folder}/models_0ppd"
MODELS_FOLDER_5PPD = f"{models_folder}/models_5ppd"
MODELS_FOLDER_10PPD = f"{models_folder}/models_10ppd"
MODELS_FOLDER_15PPD = f"{models_folder}/models_15ppd"
MODELS_FOLDER_20PPD = f"{models_folder}/models_20ppd"
MODELS_FOLDER_25PPD = f"{models_folder}/models_25ppd"
MODELS_FOLDER_30PPD = f"{models_folder}/models_30ppd"
MODELS_FOLDER_35PPD = f"{models_folder}/models_35ppd"
MODELS_FOLDER_40PPD = f"{models_folder}/models_40ppd"
MODELS_FOLDER_45PPD = f"{models_folder}/models_45ppd"
MODELS_FOLDER_50PPD = f"{models_folder}/models_50ppd"
MODELS_FOLDER_55PPD = f"{models_folder}/models_55ppd"
MODELS_FOLDER_60PPD = f"{models_folder}/models_60ppd"


# %%
   
def load_models(models_folder):
       
    # Load in the fitted models
    model_12hb = SingleThresholdClassifier()
    model_12hb.load(f"{models_folder}/model_12hb.json")

    model_18hb = SingleThresholdClassifier()
    model_18hb.load(f"{models_folder}/model_18hb.json")

    model_24hb = SingleThresholdClassifier()
    model_24hb.load(f"{models_folder}/model_24hb.json")

    model_rec_only = SingleThresholdClassifier(model_name="rec_only")
    model_rec_only.load(f"{models_folder}/model_rec_only.json")

    return model_12hb, model_18hb, model_24hb, model_rec_only


# %%
model_12hb_0ppd, model_18hb_0ppd, model_24hb_0ppd, model_rec_only_0ppd = load_models(MODELS_FOLDER_0PPD)
model_12hb_5ppd, model_18hb_5ppd, model_24hb_5ppd, model_rec_only_5ppd = load_models(MODELS_FOLDER_5PPD)
model_12hb_10ppd, model_18hb_10ppd, model_24hb_10ppd, model_rec_only_10ppd = load_models(MODELS_FOLDER_10PPD)
model_12hb_15ppd, model_18hb_15ppd, model_24hb_15ppd, model_rec_only_15ppd = load_models(MODELS_FOLDER_15PPD)
model_12hb_20ppd, model_18hb_20ppd, model_24hb_20ppd, model_rec_only_20ppd = load_models(MODELS_FOLDER_20PPD)
model_12hb_25ppd, model_18hb_25ppd, model_24hb_25ppd, model_rec_only_25ppd = load_models(MODELS_FOLDER_25PPD)
model_12hb_30ppd, model_18hb_30ppd, model_24hb_30ppd, model_rec_only_30ppd = load_models(MODELS_FOLDER_30PPD)
model_12hb_35ppd, model_18hb_35ppd, model_24hb_35ppd, model_rec_only_35ppd = load_models(MODELS_FOLDER_35PPD)
model_12hb_40ppd, model_18hb_40ppd, model_24hb_40ppd, model_rec_only_40ppd = load_models(MODELS_FOLDER_40PPD)
model_12hb_45ppd, model_18hb_45ppd, model_24hb_45ppd, model_rec_only_45ppd = load_models(MODELS_FOLDER_45PPD)
model_12hb_50ppd, model_18hb_50ppd, model_24hb_50ppd, model_rec_only_50ppd = load_models(MODELS_FOLDER_50PPD)
model_12hb_55ppd, model_18hb_55ppd, model_24hb_55ppd, model_rec_only_55ppd = load_models(MODELS_FOLDER_55PPD)
model_12hb_60ppd, model_18hb_60ppd, model_24hb_60ppd, model_rec_only_60ppd = load_models(MODELS_FOLDER_60PPD)

all_models = [model_12hb_0ppd, model_18hb_0ppd, model_24hb_0ppd, model_rec_only_0ppd,
              model_12hb_5ppd, model_18hb_5ppd, model_24hb_5ppd, model_rec_only_5ppd,
              model_12hb_10ppd, model_18hb_10ppd, model_24hb_10ppd, model_rec_only_10ppd,
              model_12hb_15ppd, model_18hb_15ppd, model_24hb_15ppd, model_rec_only_15ppd,
              model_12hb_20ppd, model_18hb_20ppd, model_24hb_20ppd, model_rec_only_20ppd,
              model_12hb_25ppd, model_18hb_25ppd, model_24hb_25ppd, model_rec_only_25ppd,
              model_12hb_30ppd, model_18hb_30ppd, model_24hb_30ppd, model_rec_only_30ppd,
              model_12hb_35ppd, model_18hb_35ppd, model_24hb_35ppd, model_rec_only_35ppd,
              model_12hb_40ppd, model_18hb_40ppd, model_24hb_40ppd, model_rec_only_40ppd,
              model_12hb_45ppd, model_18hb_45ppd, model_24hb_45ppd, model_rec_only_45ppd,
              model_12hb_50ppd, model_18hb_50ppd, model_24hb_50ppd, model_rec_only_50ppd,
              model_12hb_55ppd, model_18hb_55ppd, model_24hb_55ppd, model_rec_only_55ppd,
              model_12hb_60ppd, model_18hb_60ppd, model_24hb_60ppd, model_rec_only_60ppd,
            ]


# %%
def get_true_gaps(model, df_gaps):
    rec_thresh = model.lowest_rec
    ping_thresh = model.k_

    model_to_metric = {'12hb': 'positions_12_hours_before_sat',
                       '18hb': 'positions_18_hours_before_sat',
                       '24hb': 'positions_24_hours_before_sat',
                       'rec_only': 'positions_per_day_off'
                       }

    return df_gaps[(df_gaps.positions_per_day_off > rec_thresh) & (df_gaps.positions_per_day_on > rec_thresh)
                                    & (df_gaps[model_to_metric[model.model_name]] >= ping_thresh)]


# %%
df_results = pd.DataFrame(columns=['model_name', 'lowest_rec', 'f_half_score', 'k', 'num_true', 'hours_true'])

for model in all_models:
    df_true = get_true_gaps(model, df_all_gaps)
    df_results = df_results.append({'model_name': model.model_name, 
                                    'lowest_rec': model.lowest_rec, 
                                    'f_half_score': model.optimal_score_, 
                                    'k': int(model.k_), 
                                    'num_true': df_true.shape[0], 
                                    'hours_true': df_true.gap_hours.sum()},
                                    ignore_index=True)
df_results['k'] = df_results.k.astype('int64')
df_results.rename(columns={'k': 'k_thresh'}, inplace=True)
df_results

# %% [markdown]
# ### This indicates that the biggest jump in F0.5 comes with moving to a lowest_rec of 10

# %%
fig = plt.figure()
plt.plot('lowest_rec', 'f_half_score', data=df_results[df_results.model_name == '12hb'], label='12hb')
plt.plot('lowest_rec', 'f_half_score', data=df_results[df_results.model_name == '18hb'], label='18hb')
plt.plot('lowest_rec', 'f_half_score', data=df_results[df_results.model_name == '24hb'], label='24hb')
fig.patch.set_facecolor("white")
plt.xlim(0, 60)
plt.gca().set_facecolor("white")
plt.legend()
plt.xlabel("Lower reception bound (ppd)")
plt.ylabel("Optimal F0.5")
plt.savefig(f"{figures_folder}/sensitvity_analysis.png")
plt.show()


# %%
fig = plt.figure()
plt.plot('lowest_rec', 'num_true', data=df_results[df_results.model_name == '12hb'], label='12hb')
plt.plot('lowest_rec', 'num_true', data=df_results[df_results.model_name == '18hb'], label='18hb')
plt.plot('lowest_rec', 'num_true', data=df_results[df_results.model_name == '24hb'], label='24hb')
fig.patch.set_facecolor("white")
plt.gca().set_facecolor("white")
plt.xlim(0, 60)
plt.legend()
plt.xlabel("Lower reception bound (ppd)")
plt.ylabel("Number of true gaps")
plt.show()


# %%
fig = plt.figure()
plt.plot('lowest_rec', 'hours_true', data=df_results[df_results.model_name == '12hb'], label='12hb')
plt.plot('lowest_rec', 'hours_true', data=df_results[df_results.model_name == '18hb'], label='18hb')
plt.plot('lowest_rec', 'hours_true', data=df_results[df_results.model_name == '24hb'], label='24hb')
fig.patch.set_facecolor("white")
plt.gca().set_facecolor("white")
plt.xlim(0, 60)
plt.legend()
plt.xlabel("Lower reception bound (ppd)")
plt.ylabel("Hours of true gaps")
plt.show()


# %%
fig = plt.figure()
plt.plot('lowest_rec', 'k_thresh', data=df_results[df_results.model_name == '12hb'], label='12hb')
plt.plot('lowest_rec', 'k_thresh', data=df_results[df_results.model_name == '18hb'], label='18hb')
plt.plot('lowest_rec', 'k_thresh', data=df_results[df_results.model_name == '24hb'], label='24hb')
fig.patch.set_facecolor("white")
plt.gca().set_facecolor("white")
plt.xlim(0, 60)
plt.legend()
plt.xlabel("Lower reception bound (ppd)")
plt.ylabel("Optimal ping threshold (k)")
plt.show()


# %%
fig = model_response_curves(model_12hb_10ppd, model_18hb_10ppd, model_24hb_10ppd, model_rec_only_10ppd)

# %%
