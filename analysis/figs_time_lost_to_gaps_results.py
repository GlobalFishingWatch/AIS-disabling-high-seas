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
import matplotlib.patches as mpatches

import pyseas
pyseas._reload()
import pyseas.maps as psm

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
# ## 1. Pull data from BigQuery

# %%
table_gaps_interp = "proj_ais_gaps_catena.gaps_allocated_interpolate_v20211203"
table_gaps_raster = "proj_ais_gaps_catena.gaps_allocated_raster_v20211203"
table_fishing_vessel_activity = "proj_ais_gaps_catena.fishing_activity_v20211203"
table_reception = "world-fishing-827.proj_ais_gaps_catena.sat_reception_smoothed_one_degree_v20210722"


# %% [markdown]
# ### Overall and by Class

# %% [markdown]
# #### Gaps allocated based on interpolation

# %%
q = f'''
select 
lat_index,
lon_index,
sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
sum(gap_hours) gap_hours,
sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
vessel_class
from 
{table_gaps_interp}
where 
over_50nm 
group by lat_index, lon_index, vessel_class
'''
df_interp = gbq(q)

# %% [markdown]
# #### Gaps allocated raster based on raster method

# %%
q = f'''
select 
lat_index,
lon_index,
sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
sum(gap_hours) gap_hours,
sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
vessel_class
from 
{table_gaps_raster}
where 
over_50nm 
group by lat_index, lon_index, vessel_class
'''
df_raster = gbq(q)

# %% [markdown]
# ####  Activity by fishing vessels for gaps under 12 hours
#
# The total activity is the sum of `hours_in_gaps_under_12` because all activity is a gap between points. So this is all activity less than 12 because that is the minimum threshold for being considered a gap. This amount will later be added to all gap activity to get the total activity for calculating the fraction of time lost to gaps in each cell.

# %%
q = f'''
select 
lat_index,
lon_index,
sum(hours_in_gaps_under_12) hours,
over_50nm,
vessel_class
from 
{table_fishing_vessel_activity}
where 
over_50nm 
group by lat_index, lon_index, vessel_class, over_50nm'''

df_activity_under_12_hours = gbq(q)

# %% [markdown]
# ### By Flag

# %% [markdown]
# #### Gaps allocated based on interpolation

# %%
q = f'''
select 
lat_index,
lon_index,
sum(if(is_real_gap, gap_hours, 0)) real_gap_hours,
sum(gap_hours) gap_hours,
sum(if(is_real_gap and not over_two_weeks, gap_hours, 0)) real_gap_hours_2w,
sum(if(not over_two_weeks, gap_hours,0)) gap_hours_2w,
flag
from 
{table_gaps_interp}
where 
over_50nm 
group by lat_index, lon_index, flag
'''
df_interp_byflag = gbq(q)

# %% [markdown]
# ####  Activity by fishing vessels for gaps under 12 hours
#
# The total activity is the sum of `hours_in_gaps_under_12` because all activity is a gap between points. So this is all activity less than 12 because that is the minimum threshold for being considered a gap. This amount will later be added to all gap activity to get the total activity for calculating the fraction of time lost to gaps in each cell.

# %%
q = f'''select 
lat_index,
lon_index,
sum(hours_in_gaps_under_12) hours,
flag
from 
{table_fishing_vessel_activity}
where 
over_50nm 
group by lat_index, lon_index, flag'''

df_activity_under_12_hours_byflag = gbq(q)

# %% [markdown]
# ### Reception
#
# Get the average positions_per_day for A class devices.
#
# This is used as a rough filter to eliminate areas of activity where we really can't tell whether or not there are gaps. This limits false implications that a particular low reception area is not prone to disabling events.

# %%
q = f'''
SELECT
lat_bin, 
lon_bin,
AVG(positions_per_day) as avg_positions_per_day
FROM {table_reception} 
WHERE class = "A"
GROUP BY lat_bin, lon_bin
'''

df_reception = gbq(q)

# %%
# q = f'''
# select * from {table_reception}
# where year = 2019
# and month = 1 
# and class = "A"
# '''

# df_reception = gbq(q)

# %% [markdown]
# ---

# %% [markdown]
# ## 2. Make the rasters
#
# Note: the rasters for specific flags and classes are programmatically created in the figure code section as classes and flags are iterated over.

# %%
# Rasters are at one-degree
scale = 1

# %% [markdown]
# ### Activity by fishing vessels under 12 hours

# %%
activity_under_12_hours = psm.rasters.df2raster(df_activity_under_12_hours,
                                        'lon_index', 'lat_index',
                                        'hours', xyscale=scale, 
                                         per_km2=True, origin = 'lower')

# %% [markdown]
# ### Using the interpolation method

# %%
disabling_gap_activity_interp = psm.rasters.df2raster(df_interp,
                               'lon_index', 'lat_index',
                               'real_gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

disabling_gap_activity_interp_2w = psm.rasters.df2raster(df_interp,
                                   'lon_index', 'lat_index',
                                   'real_gap_hours_2w', xyscale=scale, 
                                    per_km2=True, origin = 'lower')

all_gap_activity_interp = psm.rasters.df2raster(df_interp,
                                   'lon_index', 'lat_index',
                                   'gap_hours', xyscale=scale, 
                                    per_km2=True, origin = 'lower')

all_gap_activity_interp_2w = psm.rasters.df2raster(df_interp,
                                   'lon_index', 'lat_index',
                                   'gap_hours_2w', xyscale=scale, 
                                    per_km2=True, origin = 'lower')

# %%
total_activity_interp = (all_gap_activity_interp + activity_under_12_hours)
frac_time_lost_interp = disabling_gap_activity_interp / total_activity_interp

total_activity_interp_2w = (all_gap_activity_interp_2w + activity_under_12_hours)
frac_time_lost_interp_2w = disabling_gap_activity_interp_2w / total_activity_interp


# %% [markdown] tags=[]
# ### Using the raster method

# %%

disabling_gap_activity_raster = psm.rasters.df2raster(df_raster,
                                           'lon_index', 'lat_index',
                                           'real_gap_hours', xyscale=scale, 
                                            per_km2=True, origin = 'lower')

disabling_gap_activity_raster_2w = psm.rasters.df2raster(df_raster,
                                           'lon_index', 'lat_index',
                                           'real_gap_hours_2w', xyscale=scale, 
                                            per_km2=True, origin = 'lower')

all_gap_activity_raster = psm.rasters.df2raster(df_raster,
                                           'lon_index', 'lat_index',
                                           'gap_hours', xyscale=scale, 
                                            per_km2=True, origin = 'lower')

all_gap_activity_raster_2w = psm.rasters.df2raster(df_raster,
                                           'lon_index', 'lat_index',
                                           'gap_hours_2w', xyscale=scale, 
                                            per_km2=True, origin = 'lower')

# %%
total_activity_raster = (all_gap_activity_raster + activity_under_12_hours)
frac_time_lost_raster = disabling_gap_activity_raster / total_activity_raster

total_activity_raster_2w = (all_gap_activity_raster_2w + activity_under_12_hours)
frac_time_lost_raster_2w = disabling_gap_activity_raster_2w / total_activity_raster

# %% [markdown]
# ### Reception

# %%
reception = psm.rasters.df2raster(df_reception,
                               'lon_bin', 'lat_bin',
                               'avg_positions_per_day', xyscale=scale, 
                                per_km2=False, origin = 'lower')

# make a raster that is 0 or 1 to muliply the result rasters
reception[reception<=5] = 0
reception[reception>5] = 1

# %%
# reception = psm.rasters.df2raster(df_reception,
#                                'lon_bin', 'lat_bin',
#                                'positions_per_day', xyscale=scale, 
#                                 per_km2=False, origin = 'lower')

# # make a raster that is 0 or 1 to muliply the result rasters
# reception[reception<=5] = 0
# reception[reception>5] = 1

# %% [markdown]
# # Figures

# %% [markdown]
# ### Settings

# %%
a_vmin = 0.02
a_vmax = 10
land_scale = '110m'

# %% [markdown]
# ## Figure 1
#
# Both the grid of total activity and the grid of the fraction of time lost to gaps are masked by reception to exclude areas of reception <= 5. Due to the bivariate nature of the figure, vessel activity in these areas of low reception implies that there is a lot of vessel activity and no intentional disabling gaps. In reality, we simply can't say anything about the gaps in that area. Therefore, we exclude these areas from the figure using the average reception for A class devices. Future work could include more sophisticated reception filtering that matches the gaps model where vessel activity is excluded by the reception for it's vessel type during that particular month.

# %%
# Bivariate using interpolation
grid_total = total_activity_interp_2w.copy() * reception.copy()
grid_ratio = frac_time_lost_interp_2w.copy() * reception.copy()
# title = 'Fraction of Fishing Vessel Activity Lost to AIS Disabling'

# Create bounding boxes to add to map
# ARG: -65,-55,-50,-35
bbox_ARG = (-65, -55, -50, -35)
# NW pacific: 143,175,38,52
bbox_NWP = (143, 175, 38, 52)
# West Africa: -23,15,-8,23
bbox_WA = (-23, 15, -8, 23)
bboxes = [bbox_ARG, bbox_NWP, bbox_WA]
        
ax = map_bivariate(grid_total, grid_ratio, a_vmin=a_vmin, a_vmax=a_vmax, bboxes=bboxes, add_cbar=True, land_scale=land_scale)
plt.savefig(figures_folder + "fig1_fraction_disabling_all.png", dpi=300, bbox_inches = 'tight')
plt.show()

# %% [markdown] tags=[]
# ## Figure 2
#
# By vessel class and flag

# %%
fig = plt.figure(figsize=(15, 15))#, constrained_layout=True)
gs = fig.add_gridspec(4,2, wspace=0.0, hspace=0.1)

# BY VESSEL CLASS
vessel_classes = [('A', 'purse_seines', 'Tuna purse seines'),
                  ('C', 'squid_jigger', 'Squid jiggers'),
                  ('E', 'drifting_longlines', 'Drifting longlines'),
                  ('G', 'trawlers', 'Trawlers'),
                 ]
for i, (number, vessel_class, vessel_class_label) in enumerate(vessel_classes):
    disabling_gaps_activity_2w = psm.rasters.df2raster(
                                df_interp[df_interp.vessel_class == vessel_class],
                                "lon_index",
                                "lat_index",
                                "real_gap_hours_2w",
                                xyscale=scale,
                                per_km2=True,
                                origin="lower",
                            )

    all_gaps_activity_2w = psm.rasters.df2raster(
                            df_interp[df_interp.vessel_class == vessel_class],
                            "lon_index",
                            "lat_index",
                            "gap_hours_2w",
                            xyscale=scale,
                            per_km2=True,
                            origin="lower",
                        )

    activity_under_12_hours_2w = psm.rasters.df2raster(
                                        df_activity_under_12_hours[df_activity_under_12_hours.vessel_class == vessel_class],
                                        "lon_index",
                                        "lat_index",
                                        "hours",
                                        xyscale=scale,
                                        per_km2=True,
                                        origin="lower",
                                    )
    
    total_activity_2w = (all_gaps_activity_2w + activity_under_12_hours_2w)
    
    grid_total = total_activity_2w.copy() * reception.copy()
    grid_ratio = (disabling_gaps_activity_2w / total_activity_2w) * reception.copy()

    map_bivariate(grid_total, grid_ratio, label=vessel_class_label, number=number, 
                  gs=gs[i,0], a_vmin=a_vmin, a_vmax=a_vmax, land_scale=land_scale)

    
# BY FLAG
flags = [('B', 'ESP', 'Spain'),
         ('D', 'TWN', 'Taiwan'),
         ('F', 'CHN', 'China'),
         ('H', 'KOR', 'South Korea'),
        ]
for i, (number, flag, flag_label) in enumerate(flags):
    disabling_gaps_activity_2w = psm.rasters.df2raster(
                        df_interp_byflag[df_interp_byflag.flag == flag],
                        "lon_index",
                        "lat_index",
                        "real_gap_hours_2w",
                        xyscale=scale,
                        per_km2=True,
                        origin="lower",
                    )

    all_gaps_activity_2w = psm.rasters.df2raster(
                        df_interp_byflag[df_interp_byflag.flag == flag],
                        "lon_index",
                        "lat_index",
                        "gap_hours_2w",
                        xyscale=scale,
                        per_km2=True,
                        origin="lower",
                    )

    activity_under_12_hours_2w = psm.rasters.df2raster(
                        df_activity_under_12_hours_byflag[df_activity_under_12_hours_byflag.flag == flag],
                        "lon_index",
                        "lat_index",
                        "hours",
                        xyscale=scale,
                        per_km2=True,
                        origin="lower",
                    )

    total_activity_2w = (all_gaps_activity_2w + activity_under_12_hours_2w)
    
    grid_total = total_activity_2w.copy() * reception.copy()
    grid_ratio = (disabling_gaps_activity_2w / total_activity_2w) * reception.copy()

    ax = map_bivariate(grid_total, grid_ratio, label=flag_label, number=number, 
                       gs=gs[i,1], a_vmin=a_vmin, a_vmax=a_vmax, land_scale=land_scale)
    

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

# %%

# %%

# %% [markdown] tags=[]
# # Supps S18
#
# Time disabled at Sea for Disabling Events < 2 Weeks and All Time Using Two Different Methods of Spatially Allocating Disabling Events. 

# %%
fig = plt.figure(figsize=(15, 8), dpi=300)#, constrained_layout=True)
gs = fig.add_gridspec(2,2, wspace=0.1, hspace=0.1)

# Bivariate using interpolation with 2 week cap
grid_total_interp_2w = total_activity_interp_2w.copy() * reception.copy()
grid_ratio_interp_2w = frac_time_lost_interp_2w.copy() * reception.copy()
ax = map_bivariate(grid_total_interp_2w, grid_ratio_interp_2w, gs=gs[0,0], a_vmin=a_vmin, a_vmax=a_vmax, add_cbar=False, land_scale=land_scale)
# ax.set_title('Linear Interpolation Method', fontsize=14, pad=10)
ax.text(0.5,1.1, 'Linear Interpolation Method', ha='center', va='top', fontsize=14, transform=ax.transAxes)
ax.text(-0.05,0.5, 'Lower Bound', ha='left', va='center', fontsize=14, rotation=90, transform=ax.transAxes)

# Bivariate using interpolation with no cap
grid_total_interp_all = total_activity_interp.copy() * reception.copy()
grid_ratio_interp_all = frac_time_lost_interp.copy() * reception.copy()
ax = map_bivariate(grid_total_interp_all, grid_ratio_interp_all, gs=gs[1,0], a_vmin=a_vmin, a_vmax=a_vmax, add_cbar=False, land_scale=land_scale)
ax.text(-0.05,0.5, 'Upper Bound', ha='left', va='center', fontsize=14, rotation=90, transform=ax.transAxes)

# Bivariate using raster method with 2 week cap
grid_total_raster_2w = total_activity_raster_2w.copy() * reception.copy()
grid_ratio_raster_2w = frac_time_lost_raster_2w.copy() * reception.copy()
ax = map_bivariate(grid_total_raster_2w, grid_ratio_raster_2w, gs=gs[0,1], a_vmin=a_vmin, a_vmax=a_vmax, add_cbar=False, land_scale=land_scale)
ax.text(0.5,1.1, 'Raster Method', ha='center', va='top', fontsize=14, transform=ax.transAxes)

# Bivariate using raster method with no cap
grid_total_raster_all = total_activity_raster.copy() * reception.copy()
grid_ratio_raster_all = frac_time_lost_raster.copy() * reception.copy()
ax = map_bivariate(grid_total_raster_all, grid_ratio_raster_all, gs=gs[1,1], a_vmin=a_vmin, a_vmax=a_vmax, add_cbar=False, land_scale=land_scale)


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

plt.savefig(figures_folder + "supps18_comparing_raster_interpolation.png", dpi=300, bbox_inches = 'tight')
plt.show()

# %%
