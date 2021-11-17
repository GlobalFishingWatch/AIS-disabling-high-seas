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
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import matplotlib as mpl
import pandas as pd
from matplotlib.pyplot import yticks
import matplotlib.patches as mpatches

import pyseas
pyseas._reload()
import pyseas.maps as psm
import pyseas.cm

import warnings
warnings.filterwarnings("ignore")

def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')


# %% [markdown]
# ### Setup figures folder

# %%
figures_folder = './figures/'

if not os.path.exists(figures_folder):
    os.makedirs(figures_folder)

# %% [markdown]
# ### Styling

# %%
mpl.rcParams["axes.spines.right"] = False
mpl.rcParams["axes.spines.top"] = False
plt.rcParams['figure.facecolor'] = 'white'
plt.rcParams['axes.facecolor'] = 'white'
plt.rc('legend',fontsize='12') 
plt.rc('legend',frameon=False)

# Bivariate color map
blue_red = mpcolors.LinearSegmentedColormap.from_list("blue_red", 
    [x.strip() for x in 
     "#0062F0, #405FF2, #645BE1, #855ECF, #A463BB, #BB63A3, #D06286, #E16063, #F2613B, #F56A00"
    .split(",")])


# %% [markdown]
# ### Bivariate Mapping

# %%
def map_bivariate(grid_total, grid_ratio, title, vmax=0.2, a_vmin=0.1, a_vmax=10, 
                  l_vmin=None, l_vmax=None, yticks=None):
    width_fudge = 1.5
    if l_vmax is None:
        l_vmax = a_vmax
    if l_vmin is None:
        l_vmin = a_vmin
    norm1 = mpcolors.Normalize(vmin=0.0, vmax=vmax, clip=True)
    norm2 = mpcolors.LogNorm(vmin=l_vmin, vmax=l_vmax, clip=True)
    transmin, transmax = norm2([a_vmin, a_vmax])
    def transmap(x):
        return np.clip((x - transmin) / (transmax - transmin), 0, 1) 
    cmap = psm.bivariate.TransparencyBivariateColormap(blue_red, transmap=transmap)
#     cmap = psm.cm.bivariate.TransparencyBivariateColormap(psm.cm.bivariate.blue_orange)
    with psm.context(psm.styles.light):
        fig = plt.figure(figsize=(15, 15))
        ax = psm.create_map()
#         ax.background_patch.set_fill(False)
        psm.add_land(ax, facecolor="#C2CDE0", 
                     edgecolor=tuple(np.array((0x16, 0x3F, 0x89, 127)) / 255),
                    linewidth=width_fudge * 72 / 400, )

        norm1 = mpcolors.Normalize(vmin=0.0, vmax=vmax, clip=True)
        norm2 = mpcolors.LogNorm(vmin=l_vmin, vmax=l_vmax, clip=True)

        psm.add_bivariate_raster(
            grid_ratio, np.clip(grid_total, l_vmin, l_vmax), cmap, norm1, norm2, origin="lower"
        )

        cb_ax = psm.add_bivariate_colorbox(
            cmap,
            norm1,
            norm2,
            xlabel="Fraction of time in disabling events",
            ylabel="Hours/km$^{2}$",
            xformat="{x:.0%}",
            yformat="{x:.2f}",
            aspect_ratio=2.0,
            fontsize=12,
        )
        
        if yticks is not None:
            cb_ax.set_yticks(yticks)
            
        psm.add_eezs(edgecolor="#163F89", linewidth=width_fudge * 2 * 72 / 400, alpha=0.5)
        ax.set_title(title, pad=10, fontsize=15)



# %%
def map_bivariate(grid_total, grid_ratio, title=None, vmax=0.2, a_vmin=0.1, a_vmax=10, 
                  l_vmin=None, l_vmax=None, yticks=None):#, add_label=False):
    width_fudge = 1.5
    if l_vmax is None:
        l_vmax = a_vmax
    if l_vmin is None:
        l_vmin = a_vmin
    norm1 = mpcolors.Normalize(vmin=0.0, vmax=vmax, clip=True)
    norm2 = mpcolors.LogNorm(vmin=l_vmin, vmax=l_vmax, clip=True)
    transmin, transmax = norm2([a_vmin, a_vmax])
    def transmap(x):
        return np.clip((x - transmin) / (transmax - transmin), 0, 1) 
    cmap = psm.bivariate.TransparencyBivariateColormap(blue_red, transmap=transmap)
    with psm.context(psm.styles.light):
        fig = plt.figure(figsize=(15, 15))
        ax = psm.create_map()
        ax.background_patch.set_fill(False)
        psm.add_land(ax, facecolor="#C2CDE0", 
                     edgecolor=tuple(np.array((0x16, 0x3F, 0x89, 127)) / 255),
                    linewidth=width_fudge * 72 / 400, )

        norm1 = mpcolors.Normalize(vmin=0.0, vmax=vmax, clip=True)
        norm2 = mpcolors.LogNorm(vmin=l_vmin, vmax=l_vmax, clip=True)

        psm.add_bivariate_raster(
            grid_ratio, np.clip(grid_total, l_vmin, l_vmax), cmap, norm1, norm2, origin="lower"
        )
        psm.add_eezs(edgecolor="#163F89", linewidth=width_fudge * 2 * 72 / 400, alpha=0.5)

        cb_ax = psm.add_bivariate_colorbox(
            cmap,
            norm1,
            norm2,
            xlabel="Fraction of time\nin disabling events",
            ylabel="Total vessel\nactivity\n(hours per km$^{2}$)",
            xformat="{x:.0%}",
            yformat="{x:.1f}",
            aspect_ratio=2.0,
            fontsize=18,
            loc=(0.6, -0.29)
        )

        # if add_label:
        #     ax1 = ax.inset_axes([0.38, -0.32, 0.2, 0.2 / 3], transform=ax.transAxes)
        #     ax1.axis('off')
        #     ax1.text(0.5, 0.5, ha='right', fontsize=20,
        #              s="Ratio of Fishing Vessel Activity"
        #              "\nSpent in Disabling Events\n"
        #              "to All Vessel Activity\n"
        #              "at One Degree Resolution")
        
        if yticks is not None:
            cb_ax.set_yticks(yticks)
        cb_ax.tick_params(labelsize=18)
        
        if title is not None:
            ax.set_title(title, pad=20, fontsize=30)



# %%
def generate_rect(ax, lon_min, lon_max, lat_min, lat_max,):
    
    ll_corners = [
        (lon_min, lat_min),
        (lon_max, lat_min),
        (lon_max, lat_max),
        (lon_min, lat_max),
        ((lon_min + lon_max) / 2, (lat_min + lat_max) / 2)
    ]
    lons = np.array([x for (x, y) in ll_corners])
    lats = np.array([y for (x, y) in ll_corners])

    xformed = ax.projection.transform_points(psm.identity, lons, lats)[:, :2]
    xy = xformed[:4]
    cntr = xformed[4]

    rect = mpatches.Polygon(
        xy,
        linewidth=1.5,
        edgecolor="#3c3c3b",
        facecolor="none",
        zorder=5
    )
    
    return rect


# %%
def map_bivariate(grid_total, grid_ratio, title=None, vmax=0.2, a_vmin=0.1, a_vmax=10, 
                  l_vmin=None, l_vmax=None, yticks=None):#, add_label=False):
    width_fudge = 1.5
    if l_vmax is None:
        l_vmax = a_vmax
    if l_vmin is None:
        l_vmin = a_vmin
        
    norm1 = mpcolors.Normalize(vmin=0.0, vmax=vmax, clip=True)
    norm2 = mpcolors.LogNorm(vmin=l_vmin, vmax=l_vmax, clip=True)

    transmin, transmax = norm2([a_vmin, a_vmax])
    def transmap(x):
        return np.clip((x - transmin) / (transmax - transmin), 0, 1) 
    cmap = psm.bivariate.TransparencyBivariateColormap(blue_red, transmap=transmap)
    
    with psm.context(psm.styles.light):
        fig = plt.figure(figsize=(15, 15))
        ax = psm.create_map()
        ax.background_patch.set_fill(False)
        psm.add_land(ax, facecolor="#C2CDE0", 
                     edgecolor=tuple(np.array((0x16, 0x3F, 0x89, 127)) / 255),
                    linewidth=width_fudge * 72 / 400, )

        psm.add_bivariate_raster(
            grid_ratio, np.clip(grid_total, l_vmin, l_vmax), cmap, norm1, norm2, origin="lower"
        )
        psm.add_eezs(edgecolor="#163F89", linewidth=width_fudge * 2 * 72 / 400, alpha=0.5)

        cb_ax = psm.add_bivariate_colorbox(
            cmap,
            norm1,
            norm2,
            xlabel="Fraction of time\nin disabling events",
            ylabel="Total vessel\nactivity\n(hours per km$^{2}$)",
            xformat="{x:.0%}",
            yformat="{x:.1f}",
            aspect_ratio=2.0,
            fontsize=18,
            loc=(0.6, -0.29)
        )
        
        if yticks is not None:
            cb_ax.set_yticks(yticks)
        cb_ax.tick_params(labelsize=18)
        
        if title is not None:
            ax.set_title(title, pad=20, fontsize=30)
            
        # Add bounding boxes
        # ARG: -65,-55,-50,-35
        lon_min, lon_max = -65, -55
        lat_min, lat_max = -50, -35
        rect = generate_rect(ax, lon_min, lon_max, lat_min, lat_max)
        ax.add_patch(rect)
        
        # NW pacific: 143,175,38,52
        lon_min, lon_max = 143, 175
        lat_min, lat_max = 38, 52
        rect = generate_rect(ax, lon_min, lon_max, lat_min, lat_max)
        ax.add_patch(rect)
        
        # West Africa: -22.7,15.0,-8.2,23.3
        lon_min, lon_max = -22.7, 15.0
        lat_min, lat_max = -8.2, 23.3
        rect = generate_rect(ax, lon_min, lon_max, lat_min, lat_max)
        ax.add_patch(rect)



# %% [markdown]
# # Generate raster data for figures

# %% [markdown]
# ## Pull data from BigQuery

# %% [markdown]
# ### Gaps allocated based on interpolation

# %%
q = '''
select 
lat_index,
lon_index,
sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
sum(gap_hours) gap_hours,
sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
vessel_class
from 
proj_ais_gaps_catena.gaps_allocated_interpolate_v20211109
where 
over_50nm 
group by lat_index, lon_index, vessel_class
'''
df_interp = gbq(q)

# %% [markdown]
# ### Gaps allocated raster based on raster method

# %%
q = '''
select 
lat_index,
lon_index,
sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
sum(gap_hours) gap_hours,
sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
sum(if(is_real_gap and not over_four_weeks, gap_hours, 0)) real_gap_hours_4w,
sum(if(not over_four_weeks, gap_hours,0)) gap_hours_4w,
vessel_class
from 
proj_ais_gaps_catena.gaps_allocated_raster_v20211109
where 
over_50nm 
group by lat_index, lon_index, vessel_class
'''
df_raster = gbq(q)

# %% [markdown]
# ### Fishing activity

# %%
q = '''
select 
lat_index,
lon_index,
sum(hours_in_gaps_under_12) hours,
over_50nm,
vessel_class
from 
proj_ais_gaps_catena.fishing_activity_v20211109
where 
over_50nm 
group by lat_index, lon_index, vessel_class, over_50nm'''

df_fishing = gbq(q)

# %% [markdown]
# ### Reception

# %%
q = '''select * from `world-fishing-827.proj_ais_gaps_catena.sat_reception_smoothed_one_degree_v20210722` 
where year = 2019
and month = 1 
and class = "A"'''

df_reception = gbq(q)

# %% [markdown]
# ## Make the rasters

# %%
# Rasters are at one-degree
scale = 1

# %% [markdown]
# ### Fishing

# %%
fishing_raster = psm.rasters.df2raster(df_fishing,
                               'lon_index', 'lat_index',
                               'hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

# %% [markdown]
# ### Using the raster method

# %%

itentional_gap_raster = psm.rasters.df2raster(df_raster,
                               'lon_index', 'lat_index',
                               'real_gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

itentional_gap_raster_2w = psm.rasters.df2raster(df_raster,
                               'lon_index', 'lat_index',
                               'real_gap_hours_2w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_raster = psm.rasters.df2raster(df_raster,
                               'lon_index', 'lat_index',
                               'gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_raster_2w = psm.rasters.df2raster(df_raster,
                               'lon_index', 'lat_index',
                               'gap_hours_2w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

# %%
frac_timelost_raster = itentional_gap_raster /(all_gap_raster + fishing_raster)

frac_timelost_raster_2w = itentional_gap_raster_2w /(all_gap_raster_2w + fishing_raster)

# %% [markdown]
# ### Using the interpolation method

# %%
itentional_gap_interp = psm.rasters.df2raster(df_interp,
                               'lon_index', 'lat_index',
                               'real_gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

itentional_gap_interp_2w = psm.rasters.df2raster(df_interp,
                               'lon_index', 'lat_index',
                               'real_gap_hours_2w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_interp = psm.rasters.df2raster(df_interp,
                               'lon_index', 'lat_index',
                               'gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_interp_2w = psm.rasters.df2raster(df_interp,
                               'lon_index', 'lat_index',
                               'gap_hours_2w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

# %%
frac_timelost_interp = itentional_gap_interp /(all_gap_interp + fishing_raster)

frac_timelost_interp_2w = itentional_gap_interp_2w /(all_gap_interp_2w + fishing_raster)


# %% [markdown]
# ### Reception

# %%
reception = psm.rasters.df2raster(df_reception,
                               'lon_bin', 'lat_bin',
                               'positions_per_day', xyscale=scale, 
                                per_km2=False, origin = 'lower')

# make a raster that is 0 or 1 to muliply the result rasters
reception[reception<5] = 0
reception[reception>1] = 1

# %% [markdown]
# # Figures

# %% [markdown]
# ## Figure 1

# %%
# Bivariate using interpolation
grid_total = (all_gap_interp_2w + fishing_raster) * reception
grid_ratio = frac_timelost_interp_2w * reception
# title = 'Fraction of Fishing Vessel Activity Lost to AIS Disabling'
map_bivariate(grid_total, grid_ratio, a_vmin=.02, a_vmax=10)
plt.savefig(figures_folder + "fig1_fraction_disabling_all.png", dpi=300, bbox_inches = 'tight')

# %% [markdown]
# ## Figure 2
#
# By vessel class

# %%
cmap = psm.cm.bivariate.TransparencyBivariateColormap(psm.cm.bivariate.blue_orange)


for i, vessel_class in enumerate(df_interp.vessel_class.unique()):
    intentional_gaps = psm.rasters.df2raster(
                        df_interp[df_interp.vessel_class == vessel_class],
                        "lon_index",
                        "lat_index",
                        "real_gap_hours_4w",
                        xyscale=scale,
                        per_km2=True,
                        origin="lower",
                    )

    all_gaps = psm.rasters.df2raster(
                df_interp[df_interp.vessel_class == vessel_class],
                "lon_index",
                "lat_index",
                "gap_hours_4w",
                xyscale=scale,
                per_km2=True,
                origin="lower",
            )

    fishing = psm.rasters.df2raster(
                df_fishing[df_fishing.vessel_class == vessel_class],
                "lon_index",
                "lat_index",
                "hours",
                xyscale=scale,
                per_km2=True,
                origin="lower",
            )
    
    grid_total = all_gaps + fishing
    grid_ratio = (intentional_gaps / grid_total) * reception

    title = f"{vessel_class} "
    map_bivariate(grid_total, grid_ratio, title, a_vmin=.02, a_vmax=10)
    
#     with psm.context(psm.styles.light):
#         fig = plt.figure(figsize=(6, 6))
#         ax = psm.create_map()
#         psm.add_land(ax)

#         norm1 = mpcolors.Normalize(vmin=0.0, vmax=.2, clip=True)
#         norm2 = mpcolors.LogNorm(vmin=0.01, vmax=10, clip=True)

#         psm.add_bivariate_raster(
#             grid_ratio, np.clip(grid_total, 0.01, 5), cmap, norm1, norm2, origin = 'lower'
#         )
#         if i == 4:
#             cb_ax = psm.add_bivariate_colorbox(
#                 cmap,
#                 norm1,
#                 norm2,
#                 xlabel="Fraction of time in disabling events",
#                 ylabel="Hours/km2",
#                 yformat="{x:.2f}",
#                 aspect_ratio=2.0,
#             )
#         ax.set_title(f"{vessel_class} ", pad=10, fontsize=12)
    
    plt.show()


# %%
