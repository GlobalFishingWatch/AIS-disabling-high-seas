# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # How much activity is close to a line?
#
# If we interpolate a line between the start and end of a gap, how much of the activity using the raster method will be within 1 degree -- or 111 km -- of this line?

# +
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import matplotlib as mpl
import pandas as pd
from tabulate import tabulate

# %matplotlib inline

mpl.rcParams["axes.spines.right"] = False
mpl.rcParams["axes.spines.top"] = False
plt.rcParams["figure.facecolor"] = "white"
plt.rcParams["axes.facecolor"] = "white"
plt.rc("legend", fontsize="12")
plt.rc("legend", frameon=False)


def gbq(q):
    return pd.read_gbq(q, project_id="world-fishing-827")


# -

with open('queries/CloseToLine.sql','r') as f:
    q = f.read()
df = gbq(q)

df.head()

df['hours_closs_cumsum'] = df.hours_close.cumsum()
df['hours_cumsum'] = df.hours.cumsum()

df['frac_cumsum'] = df.hours_closs_cumsum/df.hours_cumsum

plt.figure(figsize=(7,3))
plt.plot(df.gap_days, df.frac_cumsum)
plt.ylim(.7,1)
plt.xlim(0,120)
plt.title("Fraction of time within 1 degree of line")
plt.xlabel("days")
plt.ylabel("fraction")

plt.plot(df.gap_days, df.frac_within_line)
plt.xlim(0,30)
plt.title("Fraction of time within 111km of line\nbetween start and end of gap")
plt.xlabel("days")
plt.ylabel("fraction of time")

# +
df.head(40)

rows = []
for index, row in df.iterrows():
    rows.append([int(row.gap_days),
                 round(row.frac_within_line,2),
                 round(row.frac_cumsum,2)])
print(tabulate(rows[:40]+rows[-5:],headers=['gap days','frac close to line','cumsum close to line']))
# -

# # Take Aways:
#   
#  - For gaps 14 days long, 64% of the likely activity is within 111km of a straight line between the start and end
#  - For gaps 28 days long, 52% is
#  - The cumsum shows that at all time ranges, the majority of activity is close to a line. Across all time,~80% of activity is close to the line connecting the start and end
#  


