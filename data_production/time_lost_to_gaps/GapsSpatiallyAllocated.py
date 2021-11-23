# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Spatially Allocate Gaps to 1 Degree

# +
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import matplotlib as mpl
import pandas as pd
import pyseas.maps as psm
import pyseas.cm

import warnings
warnings.filterwarnings("ignore")

mpl.rcParams["axes.spines.right"] = False
mpl.rcParams["axes.spines.top"] = False
plt.rcParams['figure.facecolor'] = 'white'
plt.rcParams['axes.facecolor'] = 'white'
plt.rc('legend',fontsize='12') 
plt.rc('legend',frameon=False)

def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')
# -



# ## Make Rasters by Geartype

# +
# gaps allocated based on interpolation

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
proj_ais_gaps_catena.gaps_allocated_interpolate_v20211021
where 
over_200_nm 
group by lat_index, lon_index, vessel_class
'''
df_i = gbq(q)


# +
# gaps allocated raster based on raster method

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
proj_ais_gaps_catena.gaps_allocated_raster_v20211101
where 
over_200_nm 
group by lat_index, lon_index, vessel_class
'''
df_r = gbq(q)


# +
q = '''select 
lat_index,
lon_index,
sum(hours_in_gaps_under_12) hours,
vessel_class
from 
proj_ais_gaps_catena.fishing_activity_v20211101
where 
over_200_nm 
group by lat_index, lon_index, vessel_class'''

df_f = gbq(q)
# -

scale = 1

fishing_raster = psm.rasters.df2raster(df_f,
                               'lon_index', 'lat_index',
                               'hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

# +
itentional_gap_raster = psm.rasters.df2raster(df_r,
                               'lon_index', 'lat_index',
                               'real_gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

itentional_gap_raster_2w = psm.rasters.df2raster(df_r,
                               'lon_index', 'lat_index',
                               'real_gap_hours_2w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

itentional_gap_raster_4w = psm.rasters.df2raster(df_r,
                               'lon_index', 'lat_index',
                               'real_gap_hours_4w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_raster = psm.rasters.df2raster(df_r,
                               'lon_index', 'lat_index',
                               'gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_raster_2w = psm.rasters.df2raster(df_r,
                               'lon_index', 'lat_index',
                               'gap_hours_2w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_raster_4w = psm.rasters.df2raster(df_r,
                               'lon_index', 'lat_index',
                               'gap_hours_4w', xyscale=scale, 
                                per_km2=True, origin = 'lower')


# -

def map_raster(raster, norm, minvalue = 1e-2,
                  colorbar_label = r"hours of gaps  per 1000 $\mathregular{km^2}$ ",
                  title = "Gaps",figsize=(20, 20),
               title_font_size = 20 ):

    fig = plt.figure(figsize=figsize)
    with pyseas.context(psm.styles.dark):
        with pyseas.context({'text.color' : '#FFFFFF'}):
            ax = psm.create_map()
            im = psm.add_raster(raster, ax=ax,
                            cmap='fishing',
                            norm=norm,
                            origin='lower'
                            )
            psm.add_land()
            cb = psm.colorbar.add_colorbar(im, label=colorbar_label, 
                                       loc='bottom', format='%.1f')
            ax.spines['geo'].set_visible(False)
            ax.set_title(title, pad=10, fontsize=title_font_size, color = 'white' )
            psm.add_figure_background(color='black')
    #     plt.savefig(f"images/all_ais_2012_2020.png", dpi=400, bbox_inches = 'tight')
    plt.show()


norm = mpcolors.LogNorm(vmin=5, vmax=1000)
map_raster(
    itentional_gap_raster_2w*1000,
    norm,
    title=f'Time lost to Disabling Events -- FIGURE 1',
)

norm = mpcolors.LogNorm(vmin=5, vmax=1000)
map_raster(
    itentional_gap_raster*1000,
    norm,
    title=f'Time lost to Disabling Events, all time, raster',
)

# +
raster_timelost = \
    itentional_gap_raster /(all_gap_raster + fishing_raster)

raster_timelost_2w = \
    itentional_gap_raster_2w /(all_gap_raster_2w + fishing_raster)

raster_timelost_4w = \
    itentional_gap_raster_4w /(all_gap_raster_4w + fishing_raster)
# -



norm = mpcolors.Normalize(vmin=0, vmax=.3)
raster_timelost_2w_b = np.copy(raster_timelost_2w)
# raster_timelost_2w_b[fishing_raster<.1]=0
map_raster(raster_timelost_2w_b, norm, 
               title = "Fraction of time lost to Gaps, 2 week",
          colorbar_label = "fraction of time lost to gaps")

norm = mpcolors.Normalize(vmin=0, vmax=.3)
raster_timelost_2w_b = np.copy(raster_timelost_2w)
raster_timelost_2w_b[fishing_raster<.01]=0
map_raster(raster_timelost_2w_b, norm, 
               title = "Fraction of time lost to Gaps, 2 week",
          colorbar_label = "fraction of time lost to gaps")

norm = mpcolors.Normalize(vmin=0, vmax=.3)
raster_timelost_2w_b = np.copy(raster_timelost_2w)
raster_timelost_2w_b[fishing_raster<.05]=0
map_raster(raster_timelost_2w_b, norm, 
               title = "Fraction of time lost to Gaps, 2 week",
          colorbar_label = "fraction of time lost to gaps")

# +
itentional_gap_interp = psm.rasters.df2raster(df_i,
                               'lon_index', 'lat_index',
                               'real_gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

itentional_gap_interp_2w = psm.rasters.df2raster(df_i,
                               'lon_index', 'lat_index',
                               'real_gap_hours_2w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

itentional_gap_interp_4w = psm.rasters.df2raster(df_i,
                               'lon_index', 'lat_index',
                               'real_gap_hours_4w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_interp = psm.rasters.df2raster(df_i,
                               'lon_index', 'lat_index',
                               'gap_hours', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_interp_2w = psm.rasters.df2raster(df_i,
                               'lon_index', 'lat_index',
                               'gap_hours_2w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

all_gap_interp_4w = psm.rasters.df2raster(df_i,
                               'lon_index', 'lat_index',
                               'gap_hours_4w', xyscale=scale, 
                                per_km2=True, origin = 'lower')

# +
raster_timelost_interp = \
    itentional_gap_interp /(all_gap_interp + fishing_raster)

raster_timelost_interp_2w = \
    itentional_gap_interp_2w /(all_gap_interp_2w + fishing_raster)

raster_timelost_interp_4w = \
    itentional_gap_interp_4w /(all_gap_interp_4w + fishing_raster)
# -

norm = mpcolors.LogNorm(vmin=5, vmax=1000)
map_raster(
    itentional_gap_interp_2w*1000,
    norm,
    title=f'Time lost to Disabling Events -- FIGURE 1 -- ineterp',
)

norm = mpcolors.Normalize(vmin=0, vmax=.3)
raster_timelost_2w_b = np.copy(raster_timelost_interp_2w)
raster_timelost_2w_b[fishing_raster<.01]=0
map_raster(raster_timelost_2w_b, norm, 
               title = "Fraction of time lost to Gaps, 2 week, Interpolation",
          colorbar_label = "fraction of time lost to gaps")

# bivariate using interpolation
grid_total = all_gap_interp_2w + fishing_raster
grid_ratio = raster_timelost_interp_2w

cmap = psm.cm.bivariate.TransparencyBivariateColormap(psm.cm.bivariate.blue_orange)
with psm.context(psm.styles.light):
    fig = plt.figure(figsize=(15, 15))
    ax = psm.create_map()
    psm.add_land(ax)

    norm1 = mpcolors.Normalize(vmin=0.0, vmax=.2, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.01, vmax=10, clip=True)

    psm.add_bivariate_raster(
        grid_ratio, np.clip(grid_total, 0.01, 5), cmap, norm1, norm2, origin = 'lower'
    )

    cb_ax = psm.add_bivariate_colorbox(
        cmap,
        norm1,
        norm2,
        xlabel="Fraction of time in disabling events",
        ylabel="Total time on high seas",
        yformat="{x:.2f}",
        aspect_ratio=2.0,
    )
    ax.set_title("Fraction of High Seas Fishing Vessel Activity with AIS Disabled, Interpolate ", pad=10, fontsize=15)

# bivariate using raster
grid_total = all_gap_raster_2w + fishing_raster
grid_ratio = itentional_gap_raster_2w /(all_gap_raster_2w + fishing_raster)



# +
cmap = psm.cm.bivariate.TransparencyBivariateColormap(psm.cm.bivariate.blue_orange)
with psm.context(psm.styles.light):
    fig = plt.figure(figsize=(15, 15))
    ax = psm.create_map()
    psm.add_land(ax)

    norm1 = mpcolors.Normalize(vmin=0.0, vmax=.2, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.01, vmax=10, clip=True)

    psm.add_bivariate_raster(
        grid_ratio, np.clip(grid_total, 0.01, 5), cmap, norm1, norm2, origin = 'lower'
    )

    cb_ax = psm.add_bivariate_colorbox(
        cmap,
        norm1,
        norm2,
        xlabel="Fraction of time in disabling events",
        ylabel="Total time on high seas",
        yformat="{x:.2f}",
        aspect_ratio=2.0,
    )
    ax.set_title("Fraction of High Seas Fishing Vessel Activity with AIS Disabled, Raster ", pad=10, fontsize=15)

    


# +
# bivariate using raster
grid_total = all_gap_raster + fishing_raster
grid_ratio = itentional_gap_raster /(all_gap_raster + fishing_raster)

cmap = psm.cm.bivariate.TransparencyBivariateColormap(psm.cm.bivariate.blue_orange)
with psm.context(psm.styles.light):
    fig = plt.figure(figsize=(15, 15))
    ax = psm.create_map()
    psm.add_land(ax)

    norm1 = mpcolors.Normalize(vmin=0.0, vmax=.4, clip=True)
    norm2 = mpcolors.LogNorm(vmin=0.01, vmax=10, clip=True)

    psm.add_bivariate_raster(
        grid_ratio, np.clip(grid_total, 0.01, 5), cmap, norm1, norm2, origin = 'lower'
    )

    cb_ax = psm.add_bivariate_colorbox(
        cmap,
        norm1,
        norm2,
        xlabel="Fraction of time in disabling events",
        ylabel="Total time on high seas",
        yformat="{x:.2f}",
        aspect_ratio=2.0,
    )
    ax.set_title("Fraction of High Seas Fishing Vessel Activity with AIS Disabled, Raster, All Time ", pad=10, fontsize=15)


# -



# +
for i, vessel_class in enumerate(df_r.vessel_class.unique()):
    itentional_gap_raster_2w = psm.rasters.df2raster(
        df_r[df_r.vessel_class == vessel_class],
        "lon_index",
        "lat_index",
        "real_gap_hours_4w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    all_gap_raster_2w = psm.rasters.df2raster(
        df_r[df_r.vessel_class == vessel_class],
        "lon_index",
        "lat_index",
        "gap_hours_4w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    fishing_raster = psm.rasters.df2raster(
        df_f[df_f.vessel_class == vessel_class],
        "lon_index",
        "lat_index",
        "hours",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    
    grid_total = all_gap_raster_2w + fishing_raster
    grid_ratio = itentional_gap_raster_2w / grid_total
    
    with psm.context(psm.styles.light):
        fig = plt.figure(figsize=(6, 6))
        ax = psm.create_map()
        psm.add_land(ax)

        norm1 = mpcolors.Normalize(vmin=0.0, vmax=.2, clip=True)
        norm2 = mpcolors.LogNorm(vmin=0.01, vmax=10, clip=True)

        psm.add_bivariate_raster(
            grid_ratio, np.clip(grid_total, 0.01, 5), cmap, norm1, norm2, origin = 'lower'
        )
        if i == 4:
            cb_ax = psm.add_bivariate_colorbox(
                cmap,
                norm1,
                norm2,
                xlabel="Fraction of time in disabling events",
                ylabel="Total time on high seas",
                yformat="{x:.2f}",
                aspect_ratio=2.0,
            )
        ax.set_title(f"{vessel_class} ", pad=10, fontsize=12)
    
    plt.show()


#     norm = mpcolors.Normalize(vmin=0, vmax=.5)
#     raster_timelost_b = np.copy(raster_timelost)
#     raster_timelost_b[fishing_raster < 0.005] = 0
#     map_raster(
#         raster_timelost_b,
#         norm,
#         title=f'Fraction of time lost to Gaps, {vessel_class.replace("_"," ").title()}',
#         colorbar_label="fraction of time lost to gaps",
#         figsize = (5,5),
#         title_font_size = 15
#     )

#     norm = mpcolors.LogNorm(vmin=10, vmax=1000)
#     map_raster(
#         itentional_gap_raster_2w*1000,
#         norm,
#         title=f'Time lost to Gaps, {vessel_class.replace("_"," ").title()}',
#         figsize = (5,5),
#         title_font_size = 15
#     )
# -



for vessel_class in df_r.vessel_class.unique():
    itentional_gap_raster_2w = psm.rasters.df2raster(
        df_r[df_r.vessel_class == vessel_class],
        "lon_index",
        "lat_index",
        "real_gap_hours_4w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    all_gap_raster_2w = psm.rasters.df2raster(
        df_r[df_r.vessel_class == vessel_class],
        "lon_index",
        "lat_index",
        "gap_hours_4w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    fishing_raster = psm.rasters.df2raster(
        df_f[df_f.vessel_class == vessel_class],
        "lon_index",
        "lat_index",
        "hours",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    raster_timelost = itentional_gap_raster_2w / (all_gap_raster_2w + fishing_raster)

    norm = mpcolors.Normalize(vmin=0, vmax=.5)
    raster_timelost_b = np.copy(raster_timelost)
    raster_timelost_b[fishing_raster < 0.005] = 0
    map_raster(
        raster_timelost_b,
        norm,
        title=f'Fraction of time lost to Gaps, {vessel_class.replace("_"," ").title()}',
        colorbar_label="fraction of time lost to gaps",
        figsize = (5,5),
        title_font_size = 15
    )

    norm = mpcolors.LogNorm(vmin=10, vmax=1000)
    map_raster(
        itentional_gap_raster_2w*1000,
        norm,
        title=f'Time lost to Gaps, {vessel_class.replace("_"," ").title()}',
        figsize = (5,5),
        title_font_size = 15
    )

# # By Flag

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
proj_ais_gaps_catena.gaps_allocated_raster_v20211101
where 
over_200_nm 
group by lat_index, lon_index, flag
'''
df_rf = gbq(q)

# +
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

df_ff = gbq(q)
# -

for flag in ["CHN", "KOR", "JPN", "ESP", "TWN"]:
    itentional_gap_raster_4w = psm.rasters.df2raster(
        df_rf[df_rf.flag == flag],
        "lon_index",
        "lat_index",
        "real_gap_hours_4w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    all_gap_raster_4w = psm.rasters.df2raster(
        df_rf[df_rf.flag == flag],
        "lon_index",
        "lat_index",
        "gap_hours_4w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    fishing_raster = psm.rasters.df2raster(
        df_ff[df_ff.flag == flag],
        "lon_index",
        "lat_index",
        "hours",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    raster_timelost = itentional_gap_raster_2w / (all_gap_raster_2w + fishing_raster)

    norm = mpcolors.Normalize(vmin=0, vmax=0.5)
    raster_timelost_b = np.copy(raster_timelost)
    raster_timelost_b[fishing_raster < 0.005] = 0
    map_raster(
        raster_timelost_b,
        norm,
        title=f"Fraction of time lost to Gaps, {flag}",
        colorbar_label="fraction of time lost to gaps",
        figsize=(5, 5),
        title_font_size=15,
    )

    norm = mpcolors.LogNorm(vmin=10, vmax=1000)
    map_raster(
        itentional_gap_raster_4w * 1000,
        norm,
        title=f"Time lost to Gaps, {flag}",
        figsize=(5, 5),
        title_font_size=15,
    )

# +
for i, flag in enumerate(["CHN", "KOR",  "ESP", "TWN","JPN"]):
    itentional_gap_raster_2w = psm.rasters.df2raster(
        df_rf[df_rf.flag == flag],
        "lon_index",
        "lat_index",
        "real_gap_hours_2w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    all_gap_raster_2w = psm.rasters.df2raster(
        df_rf[df_rf.flag == flag],
        "lon_index",
        "lat_index",
        "gap_hours_2w",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    fishing_raster = psm.rasters.df2raster(
        df_ff[df_ff.flag == flag],
        "lon_index",
        "lat_index",
        "hours",
        xyscale=scale,
        per_km2=True,
        origin="lower",
    )

    
    grid_total = all_gap_raster_2w + fishing_raster
    grid_ratio = itentional_gap_raster_2w / grid_total
    
    with psm.context(psm.styles.light):
        fig = plt.figure(figsize=(6, 6))
        ax = psm.create_map()
        psm.add_land(ax)

        norm1 = mpcolors.Normalize(vmin=0.0, vmax=.2, clip=True)
        norm2 = mpcolors.LogNorm(vmin=0.01, vmax=10, clip=True)

        psm.add_bivariate_raster(
            grid_ratio, np.clip(grid_total, 0.01, 5), cmap, norm1, norm2, origin = 'lower'
        )
        
        if i == 4:
            cb_ax = psm.add_bivariate_colorbox(
                cmap,
                norm1,
                norm2,
                xlabel="Fraction of time in disabling events",
                ylabel="Total time on high seas",
                yformat="{x:.2f}",
                aspect_ratio=2.0,
            )
        ax.set_title(f"{flag} ", pad=10, fontsize=12)
    
    plt.show()


#     norm = mpcolors.Normalize(vmin=0, vmax=.5
#     raster_timelost_b = np.copy(raster_timelost)
#     raster_timelost_b[fishing_raster < 0.005] = 0
#     map_raster(
#         raster_timelost_b,
#         norm,
#         title=f'Fraction of time lost to Gaps, {vessel_class.replace("_"," ").title()}',
#         colorbar_label="fraction of time lost to gaps",
#         figsize = (5,5),
#         title_font_size = 15
#     )

#     norm = mpcolors.LogNorm(vmin=10, vmax=1000)
#     map_raster(
#         itentional_gap_raster_2w*1000,
#         norm,
#         title=f'Time lost to Gaps, {vessel_class.replace("_"," ").title()}',
#         figsize = (5,5),
#         title_font_size = 15
#     )
# -




