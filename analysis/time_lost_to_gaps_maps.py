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

# %% [markdown]
# # Code to generate Figures 1 and 2 of the intentional disabling paper

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
        zorder=1
    )
    
    return rect


# %%
def map_bivariate(grid_total, grid_ratio, gs=None, title=None, label=None, number=None, 
                  vmax=0.2, a_vmin=0.1, a_vmax=10, l_vmin=None, l_vmax=None, 
                  land_scale='10m', yticks=None, bboxes=None, add_cbar=False):
    '''
    Bivariate map that assumes it's mapping total vessel activity against 
    the fraction of that activity spent in disabling events.
    
    Parameters
    -----------
    grid_total: total vessel activity, ndarray
    grid_ratio: fraction of activity spent in disabling events, ndarray
    title: title, default=None
    vmax: x-axis max, default=0.2
    a_vmin:
    a_vmax:
    l_vmin:
    l_vmax:
    yticks: labels for the yticks
    bboxes: bounding boxes to plot on the map 
            as tuples (lon_min, lon_max, lat_min, lat_max), list
    '''
    
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
        
        if gs:
            ax = psm.create_map(gs)
        else:
            fig = plt.figure(figsize=(15, 15))
            ax = psm.create_map()
            
        ax.background_patch.set_fill(False)
        psm.add_land(ax, facecolor="#C2CDE0", scale=land_scale,
                     edgecolor=tuple(np.array((0x16, 0x3F, 0x89, 127)) / 255),
                    linewidth=width_fudge * 72 / 400, )

        psm.add_bivariate_raster(
            grid_ratio, np.clip(grid_total, l_vmin, l_vmax), cmap, norm1, norm2, origin="lower", ax=ax
        )
        psm.add_eezs(edgecolor="#163F89", linewidth=width_fudge * 2 * 72 / 400, alpha=0.5)

        if add_cbar:
            cb_ax = psm.add_bivariate_colorbox(
                cmap,
                norm1,
                norm2,
                xformat="{x:.0%}",
                yformat="{x:.1f}",
                pad=0.1,
                aspect_ratio=2.0,
                fontsize=18,
                loc=(0.6, -0.29)
            )

            if yticks is not None:
                cb_ax.set_yticks(yticks)
            cb_ax.tick_params(labelsize=18)
            cb_ax.set_xlabel("Fraction of activity obscured\nby suspected disabling", labelpad=10)
            cb_ax.set_ylabel("Estimated total\nfishing vessel\nactivity\n(hours per km$^{2}$)", labelpad=10)
        
        if title is not None:
            ax.set_title(title, pad=20, fontsize=30)

        if number is not None:
            ax.text(0.0, 1.0, number, fontweight='bold', fontsize=24, color='#363c4c',
                    ha='left', va='top', transform=ax.transAxes) 
            
        if label is not None:
            ax.text(0.5, 0.05, label, fontweight='bold', fontsize=18, color='#363c4c',
                    ha='center', va='bottom', transform=ax.transAxes) 
        
        if bboxes is not None:
            for lon_min, lon_max, lat_min, lat_max in bboxes:
                rect = generate_rect(ax, lon_min, lon_max, lat_min, lat_max)
                ax.add_patch(rect)
                
        return ax



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
# ### Gaps allocated based on interpolation and fishing by flag

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
flag
from 
proj_ais_gaps_catena.gaps_allocated_interpolate_v20211101
where 
over_200_nm 
group by lat_index, lon_index, flag
'''
df_interp_byflag = gbq(q)

# %%
q = '''select 
lat_index,
lon_index,
sum(hours_in_gaps_under_12) hours,
flag
from 
proj_ais_gaps_catena.fishing_activity_v20211101
where 
over_200_nm 
group by lat_index, lon_index, flag'''

df_fishing_byflag = gbq(q)

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

intentional_gap_raster = psm.rasters.df2raster(df_raster,
                               'lon_index', 'lat_index',
                               'real_gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

intentional_gap_raster_2w = psm.rasters.df2raster(df_raster,
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
frac_timelost_raster = intentional_gap_raster /(all_gap_raster + fishing_raster)

frac_timelost_raster_2w = intentional_gap_raster_2w /(all_gap_raster_2w + fishing_raster)

# %% [markdown]
# ### Using the interpolation method

# %%
intentional_gap_interp = psm.rasters.df2raster(df_interp,
                               'lon_index', 'lat_index',
                               'real_gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

intentional_gap_interp_2w = psm.rasters.df2raster(df_interp,
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
frac_timelost_interp = intentional_gap_interp /(all_gap_interp + fishing_raster)

frac_timelost_interp_2w = intentional_gap_interp_2w /(all_gap_interp_2w + fishing_raster)


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
grid_total = (all_gap_interp_2w.copy() + fishing_raster.copy()) * reception.copy()
grid_ratio = frac_timelost_interp_2w.copy() * reception.copy()
# title = 'Fraction of Fishing Vessel Activity Lost to AIS Disabling'

# Create bounding boxes to add to map
# ARG: -65,-55,-50,-35
bbox_ARG = (-65, -55, -50, -35)
# NW pacific: 143,175,38,52
bbox_NWP = (143, 175, 38, 52)
# West Africa: -23,15,-8,23
bbox_WA = (-23, 15, -8, 23)
bboxes = [bbox_ARG, bbox_NWP, bbox_WA]
        
ax = map_bivariate(grid_total, grid_ratio, a_vmin=.02, a_vmax=10, bboxes=bboxes, add_cbar=True, land_scale='110m')
plt.savefig(figures_folder + "fig1_fraction_disabling_all.png", dpi=300, bbox_inches = 'tight')
plt.show()

# %% [markdown] tags=[]
# ## Figure 2
#
# By vessel class and flag

# %%
fig = plt.figure(figsize=(15, 15))#, constrained_layout=True)
gs = fig.add_gridspec(4,2, wspace=0.0, hspace=0.1)

a_vmin = 0.02
a_vmax = 10

# BY VESSEL CLASS
vessel_classes = [('A', 'purse_seines', 'Tuna purse seines'),
                  ('C', 'squid_jigger', 'Squid jiggers'),
                  ('E', 'drifting_longlines', 'Drifting longlines'),
                  ('G', 'trawlers', 'Trawlers'),
                 ]
for i, (number, vessel_class, vessel_class_label) in enumerate(vessel_classes):
    intentional_gaps = psm.rasters.df2raster(
                        df_interp[df_interp.vessel_class == vessel_class],
                        "lon_index",
                        "lat_index",
                        "real_gap_hours_2w",
                        xyscale=scale,
                        per_km2=True,
                        origin="lower",
                    )

    all_gaps = psm.rasters.df2raster(
                df_interp[df_interp.vessel_class == vessel_class],
                "lon_index",
                "lat_index",
                "gap_hours_2w",
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

    map_bivariate(grid_total, grid_ratio, label=vessel_class_label, number=number, 
                  gs=gs[i,0], a_vmin=a_vmin, a_vmax=a_vmax, land_scale='110m')

    
# BY FLAG
flags = [('B', 'ESP', 'Spain'),
         ('D', 'TWN', 'Taiwan'),
         ('F', 'CHN', 'China'),
         ('H', 'KOR', 'South Korea'),
        ]
for i, (number, flag, flag_label) in enumerate(flags):
    intentional_gaps = psm.rasters.df2raster(
        df_interp_byflag[df_interp_byflag.flag == flag],
        "lon_index",
        "lat_index",
        "real_gap_hours_2w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    all_gaps = psm.rasters.df2raster(
        df_interp_byflag[df_interp_byflag.flag == flag],
        "lon_index",
        "lat_index",
        "gap_hours_2w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    fishing = psm.rasters.df2raster(
        df_fishing_byflag[df_fishing_byflag.flag == flag],
        "lon_index",
        "lat_index",
        "hours",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    
    grid_total = all_gaps + fishing
    grid_ratio = (intentional_gaps / grid_total) * reception

    ax = map_bivariate(grid_total, grid_ratio, label=flag_label, number=number, 
                       gs=gs[i,1], a_vmin=a_vmin, a_vmax=a_vmax, land_scale='110m')
    

# Add one color bar for entire figure.
# This adds it to the last axis to be created (lower right).
# Some code is currently duplicated here from `map_bivariate()`
# which could be improved upon later.
vmax = 0.2
norm1 = mpcolors.Normalize(vmin=0.0, vmax=vmax, clip=True)
norm2 = mpcolors.LogNorm(vmin=a_vmin, vmax=a_vmax, clip=True)

transmin, transmax = norm2([a_vmin, a_vmax])
def transmap(x):
    return np.clip((x - transmin) / (transmax - transmin), 0, 1) 
cmap = psm.bivariate.TransparencyBivariateColormap(blue_red, transmap=transmap)

with psm.context(psm.styles.light):
    cb_ax = psm.add_bivariate_colorbox(
        cmap,
        norm1,
        norm2,
        xformat="{x:.0%}",
        yformat="{x:.1f}",
        pad=0.1,
        width = 0.5,
        height = 0.5,
        fontsize=18,
        loc=(0.32, -0.75),
        ax=ax
    )
    cb_ax.tick_params(labelsize=18)
    cb_ax.set_xmargin(0)
    cb_ax.set_xlabel("Fraction of activity obscured\nby suspected disabling", labelpad=10)
    cb_ax.set_ylabel("Estimated total\nfishing vessel\nactivity\n(hours per km$^{2}$)", labelpad=10)

plt.savefig(figures_folder + "fig2_fraction_disabling_geartype_flag.png", dpi=300, bbox_inches = 'tight')
plt.show()
