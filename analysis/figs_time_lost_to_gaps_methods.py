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
#     display_name: rad
#     language: python
#     name: rad
# ---

# %%
import os
import numpy as np
from math import floor
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import pandas as pd
import proplot as pplt

# %matplotlib inline

import warnings
warnings.filterwarnings("ignore")

def gbq(q):
    return pd.read_gbq(q, project_id="world-fishing-827")


# %% [markdown]
# ### Setup figures folder

# %%
figures_folder = './figures/'

if not os.path.exists(figures_folder):
    os.makedirs(figures_folder)


# %% [markdown]
# # Visualize Probability Raster

# %% [markdown]
# ### Download the data

# %%
def probability_raster(days, distance_km):
    q = f'''select x, y,
    hours,
    vessel_class,
    hours_diff,
    distance_km
    from 
    proj_ais_gaps_catena.raster_gaps_norm_v20211021
    where distance_km = {distance_km}
    and hours_diff = {days}*24
    and vessel_class not in ('tug','cargo_or_tanker')
    '''
    
    return gbq(q)



# %%
distance_km = 320
max_dist=300

# %%
df_raster_2days = probability_raster(days=2, distance_km=distance_km)
df_raster_8days = probability_raster(days=8, distance_km=distance_km)
df_raster_14days = probability_raster(days=14, distance_km=distance_km)
df_raster_30days = probability_raster(days=30, distance_km=distance_km)


# %%

# %%
def plot_probability_raster(df_raster, days, distance_km, vmin=.001, vmax=1,
                            max_dist = 1000, save=False):

    array = [[1,1,2,2],
            [3,3,4,4],
            [5,5,0,0]]

    fig, axs = pplt.subplots(array, figwidth=9*.56, figheight=17.5*.75*3/5,
                             aspect=1)#,axwidth=1.1, wratios=(3)
    titles = []
#     max_dist = distance_km*4

    def fill_grid(row):
        x = int(row.x)+int(max_dist/10)
        y = int(row.y)+int(max_dist/10)
        try:
            grid[y][x]+=row.hours
        except:
            pass


    vessel_classes = ['drifting_longlines', 'trawlers', 'squid_jigger', 'purse_seines', 'other']
    for i, vessel_class in enumerate(vessel_classes):

        grid = np.zeros(shape=(int(max_dist/10*2),int(max_dist/10*2)))
        d = df_raster[df_raster.vessel_class==vessel_class]
        d.apply(fill_grid, axis=1)
        cbar = axs[i].imshow(grid, interpolation=None,extent=[-max_dist,max_dist,-max_dist,max_dist], vmin=0, vmax=vmax)
        axs[i].set_title(f"{vessel_class}")


    axs.format(
        abc=True, abcstyle='a.', titleloc='l', #title=titles,
        xlabel='km', ylabel='km', 
        suptitle=f'{distance_km} km, {days} days'
    )
    fig.colorbar(cbar, loc='b', label='hours')
    
    if save:
        plt.savefig(figures_folder + f"probability_raster_{distance_km}km_{days}days.png", dpi=300, bbox_inches = 'tight')


# %%
plot_probability_raster(df_raster_2days, 2, distance_km, max_dist=max_dist, 
                        vmax=2, save=True)


# %%
plot_probability_raster(df_raster_2days, 2, distance_km, max_dist=max_dist, 
                        vmax=2, save=True)
plot_probability_raster(df_raster_8days, 8, distance_km, max_dist=max_dist,
                        vmax=2, save=True)
plot_probability_raster(df_raster_14days, 14, distance_km, max_dist=max_dist,
                        vmax=2, save=True)
plot_probability_raster(df_raster_30days, 30, distance_km, max_dist=max_dist,
                        vmax=2, save=True)


# %%

# %%

# %%

# %%

# %%

# %%
def plot_probability_raster(df_raster, days, distance_km, gs, fig, vmin=.001, vmax=1,
                            max_dist = 1000, log = True, max_value =1):

    array = [[1,1,2,2],
            [3,3,4,4],
            [5,5,0,0]]

    
    grid = gs.subgridspec(3,2, hspace=0)
    axs = grid.subplots()
    # fig, axs = pplt.subplots(array, figwidth=9*.56, figheight=17.5*.75*3/5,
    #                          aspect=1)#,axwidth=1.1, wratios=(3)
    titles = []
#     max_dist = distance_km*4

    if log:
        norm = mpcolors.LogNorm(vmin=.001,vmax=max_value)
    else:
        norm = mpcolors.Normalize(vmin=0,vmax=max_value)
  

    def fill_grid(row):
        x = int(row.x)+int(max_dist/10)
        y = int(row.y)+int(max_dist/10)
        try:
            grid[y][x]+=row.hours
        except:
            pass


    def get_axis(axs, i):
        row = floor(i/2)
        col = i%2
        return axs[row, col]
        
    vessel_classes = ['drifting_longlines', 'trawlers', 'squid_jigger', 'purse_seines', 'other']
    for i, vessel_class in enumerate(vessel_classes):
        ax = get_axis(axs, i)
        
        grid = np.zeros(shape=(int(max_dist/10*2),int(max_dist/10*2)))
        d = df_raster[df_raster.vessel_class==vessel_class]
        d.apply(fill_grid, axis=1)
        cbar = ax.imshow(grid, interpolation=None,extent=[-max_dist,max_dist,-max_dist,max_dist],
                            norm=norm)
        ax.set_title(f"{vessel_class}")

    ax = get_axis(axs, 5)
    ax.axis('off')

    # axs.format(
    #     abc=True, abcstyle='a.', titleloc='l', #title=titles,
    #     xlabel='km', ylabel='km', 
    #     suptitle=f'{distance_km} km, {days} days'
    # )
    fig.colorbar(cbar, use_gridspec=True, label='hours', norm=norm,
                    orientation='horizontal')

# %%
fig = plt.figure(figsize=(20, 30), constrained_layout=True)
gs = fig.add_gridspec(2,2, wspace=0.0, hspace=0.0)
axs = gs.subplots()

plot_probability_raster(df_raster_2days, 2, distance_km, gs[0,0], fig,
                        max_dist=max_dist, log=False, max_value=2)
# plot_probability_raster(df_raster_8days, 8, distance_km, gs[0,1], fig,
#                         max_dist=max_dist, log=False, max_value=2)
# plot_probability_raster(df_raster_8days, 14, distance_km, gs[1,0], fig,
#                         max_dist=max_dist, log=False, max_value=2)
# plot_probability_raster(df_raster_8days, 30, distance_km, gs[1,1], fig,
#                         max_dist=max_dist, log=False, max_value=2)

# plt.tight_layout()
plt.show()

# %%

# %%

# %%

# %% [markdown]
# # ORIGINAL CODE

# %%
for days in [2,8,14,30]:
    show_gaps(distance_km = int(1280/4), days = days,
              max_dist = 300, log = False, max_value = 2)

# %%
for days in [2,8,14,30]:
    show_gaps(distance_km = int(1280/8), days = days, 
              max_dist = 500, log = False, max_value = 2)
