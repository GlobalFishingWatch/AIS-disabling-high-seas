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
import pyseas
import pyseas.maps as psm

# %matplotlib inline

import warnings
warnings.filterwarnings("ignore")

def gbq(q):
    return pd.read_gbq(q, project_id="world-fishing-827")


# %% [markdown]
# ### Setup figures folder

# %%
figures_folder = './figures/'
precursors_folder = './figures/precursors/'
if not os.path.exists(figures_folder):
    os.makedirs(figures_folder)
    
if not os.path.exists(precursors_folder):
    os.makedirs(precursors_folder)


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
                            max_dist = 1000, save=False, fig_label=""):

    array = [[1,1,2,2],
            [3,3,4,4],
            [5,5,0,0]]

    fig, axs = pplt.subplots(array, figwidth=9*.56, figheight=17.5*.75*3/5,
                             aspect=1, top=5, bottom=5)
#     max_dist = distance_km*4

    def fill_grid(row):
        x = int(row.x)+int(max_dist/10)
        y = int(row.y)+int(max_dist/10)
        try:
            grid[y][x]+=row.hours
        except:
            pass


    vessel_classes = [('drifting_longlines', 'Drifting longlines'), 
                      ('trawlers', 'Trawlers'), 
                      ('squid_jigger', 'Squid jiggers'), 
                      ('purse_seines', 'Tuna purse seines'), 
                      ('other', 'Other')]
    for i, (vessel_class, title) in enumerate(vessel_classes):

        grid = np.zeros(shape=(int(max_dist/10*2),int(max_dist/10*2)))
        d = df_raster[df_raster.vessel_class==vessel_class]
        d.apply(fill_grid, axis=1)
        cbar = axs[i].imshow(grid, interpolation=None,extent=[-max_dist,max_dist,-max_dist,max_dist], vmin=0, vmax=vmax)
        axs[i].set_title(f"{title}")


    axs.format(
        abc=False, titleloc='l',
        xlabel='km', ylabel='km', 
        suptitle=f'{distance_km} km, {days} days',
        suptitlepad=-0.1
    )
    fig.colorbar(cbar, loc='b', label='hours')
    
    axs[0].text(-0.2, 1.2, fig_label, ha='left', va='top', transform=axs[0].transAxes, fontweight='bold')
    
    if save:
        plt.savefig(precursors_folder + f"S15_probability_raster_{distance_km}km_{days}days.png", dpi=300, bbox_inches = 'tight')


# %%
plot_probability_raster(df_raster_2days, 2, distance_km, max_dist=max_dist, vmax=2, fig_label="A.", save=True)
plot_probability_raster(df_raster_8days, 8, distance_km, max_dist=max_dist, vmax=2, fig_label="B.", save=True)
plot_probability_raster(df_raster_14days, 14, distance_km, max_dist=max_dist, vmax=2, fig_label="C.", save=True)
plot_probability_raster(df_raster_30days, 30, distance_km, max_dist=max_dist, vmax=2, fig_label="D.", save=True)


# %% [markdown]
# ### Glue the previous images together

# %%
def join_images(*rows, bg_color=(0, 0, 0, 0), alignment=(0.5, 0.5)):
    rows = [
        [image.convert('RGBA') for image in row]
        for row
        in rows
    ]

    heights = [
        max(image.height for image in row)
        for row
        in rows
    ]

    widths = [
        max(image.width for image in column)
        for column
        in zip(*rows)
    ]

    tmp = Image.new(
        'RGBA',
        size=(sum(widths), sum(heights)),
        color=bg_color
    )

    for i, row in enumerate(rows):
        for j, image in enumerate(row):
            y = sum(heights[:i]) + int((heights[i] - image.height) * alignment[1])
            x = sum(widths[:j]) + int((widths[j] - image.width) * alignment[0])
            tmp.paste(image, (x, y))
    
    return tmp



# %%
img_2days = figures_folder + f"probability_raster_{distance_km}km_2days.png"
img_8days = figures_folder + f"probability_raster_{distance_km}km_8days.png"
img_14days = figures_folder + f"probability_raster_{distance_km}km_14days.png"
img_30days = figures_folder + f"probability_raster_{distance_km}km_30days.png"

images = [[Image.open(img_2days), Image.open(img_8days)], 
          [Image.open(img_14days), Image.open(img_30days)]]

joined_image = join_images(*images)
joined_image.save(figures_folder + f"S15_probability_raster_{distance_km}km_all.png", format='png')

# %%
joined_image

# %% [markdown]
# # Examples of spatially allocated gaps

# %% [markdown]
# ## Query templates

# %%
q_raster_template = '''
#standardSQL

create temp function radians(x float64) as (
  3.14159265359 * x / 180
);

create temp function degrees(x float64) as (
    x * 180 / 3.14159265359
);

create temp function deglat2km() as (
  111.195
);

create temp function get_midpoint(point1 geography, point2 geography) as (
  -- Equation from http://www.movable-type.co.uk/scripts/latlong.html
  -- They assume a spherical earth, which, of course, is only mostly right

  -- MIDPOINT
  -- Formula:	Bx = cos φ2 ⋅ cos Δλ
  -- By = cos φ2 ⋅ sin Δλ
  -- φm = atan2( sin φ1 + sin φ2, √(cos φ1 + Bx)² + By² )
  -- λm = λ1 + atan2(By, cos(φ1)+Bx)
  -- in both cases,  λ1 is lon1,  λ2 is lon2, φ1 is lat1, φ2 is lat2, measured in radians
  ( select
      st_geogpoint(lon_center, lat_center)
    from
      (
      select degrees(atan2(sin(rlat1) + sin(rlat2),
                           pow((cos(rlat1)+b_x)*(cos(rlat1)+B_x) + B_y*B_y, .5 ))) lat_center,
             degrees(rlon1 + atan2(b_y, cos(rlat1) + b_x)) as lon_center
          from
      (
        select
        cos((rlat2))*cos((rlon2)-(rlon1)) as b_x,
        cos((rlat2)) * sin((rlon2)-(rlon1)) as b_y,
        *
        from
          (select
          radians(st_x(point1)) as rlon1,
          radians(st_y(point1)) as rlat1,
          radians(st_x(point2)) as rlon2,
          radians(st_y(point2)) as rlat2)

  limit 1
  )
)));

create temp function get_course(point1 geography, point2 geography) as ((
  -- Equation are from http://www.movable-type.co.uk/scripts/latlong.html
  -- assumes a spherical earth, which, of course, is only mostly right

  --  BEARING
  -- (which is measured, apparently, counterclockwise from due east, so 
  -- we edited to make it clockwise from due north 
  --        const y = Math.sin(λ2-λ1) * Math.cos(φ2);
  -- const x = Math.cos(φ1)*Math.sin(φ2) -
  --           Math.sin(φ1)*Math.cos(φ2)*Math.cos(λ2-λ1);
  -- const θ = Math.atan2(y, x);
  -- const brng = (θ*180/Math.PI + 360) % 360; // in degrees
  -- λ1 is lon1,  λ2 is lon2, φ1 is lat1, φ2 is lat2, measured in radians

  select (90-degrees(atan2(x, y))) course
  from
  (select
  sin(rlon2-rlon1)*cos(rlat2) as y,
  cos(rlat1)*sin(rlat2) - sin(rlat1)*cos(rlat2)*cos(rlon2-rlon1) as x,
  from
    (select
    radians(st_x(point1)) as rlon1,
    radians(st_y(point1)) as rlat1,
    radians(st_x(point2)) as rlon2,
    radians(st_y(point2)) as rlat2))

));

create temp function get_course_frommidpoint(point1 geography, point2 geography) as (
  get_course(get_midpoint(point1, point2), point2)
);

create temp function weight_average_lons(lon float64, lon2 float64, timeto float64, timeto2 float64) AS
(
  # Make sure that lon < 180 and > -180, and that we average across the dateline
  # appropriately
case
when lon - lon2 > 300 then ( (lon-360)*timeto2 + lon2*timeto)/(timeto+timeto2)
when lon - lon2 < -300 then ( (lon+360)*timeto2 + lon2*timeto)/(timeto+timeto2)
else (lon*timeto2 + lon2*timeto)/(timeto+timeto2) end );

create temp function reasonable_lon(lon float64) AS
(case when lon > 180 then lon - 360
when lon < -180 then lon + 360
else lon end);


create temp function map_label(label string)
as (
  case when label ="drifting_longlines" then "drifting_longlines"
  when label ="purse_seines" then "purse_seines"
  when label ="other_purse_seines" then "purse_seines"
  when label ="tuna_purse_seines" then "purse_seines"
  when label ="cargo_or_tanker" then "cargo_or_tanker"
  when label ="cargo" then "cargo_or_tanker"
  when label ="tanker" then "cargo_or_tanker"
  when label ="squid_jigger" then "squid_jigger"
  when label ="tug" then "tug"
  when label = "trawlers" then "trawlers"
  else "other" end
);

create temp function map_distance(d float64)
as (
case when d < 10/2+3/2 then 3
when d >= 10/2+3/2 and d < 15 then 10
when d >= 15 and d <30 then 20
when d >= 30 and d < 60 then 40
when d >= 60 and d < 120 then 80
when d >= 120 and d <240 then 160
when d >= 240 and d < 480 then 320
when d >= 480 and d < 960 then 640
when d >= 960 then 1280
else null end
);

create temp function map_hours_diff(h float64) as (
case when h < 12 + 18.0 then 12
when h >= 12 + 18.0 and h < 36.0 then 24
when h >= 36 and h < 72 then 48
 when h >= 72 and h < 120 then 96
 when h >= 120 and h < 168 then 144
 when h >= 168 and h < 216 then 192
 when h >= 216 and h < 264 then 240
 when h >= 264 and h < 312 then 288
 when h >= 312 and h < 360 then 336
 when h >= 360 and h < 408 then 384
 when h >= 408 and h < 456 then 432
 when h >= 456 and h < 540 then 480
 when h >= 540 and h < 660 then 600
 when h >= 660 and h < 780 then 720
 when h >= 780 then 840

else null end
);





with gap_table as

(SELECT ssvid, year,
  off_lat, off_lon, on_lat, on_lon, gap_hours, gap_distance_m/1000 as gap_distance_km,
  st_geogpoint(off_lon, off_lat) as gap_start_point,
  st_geogpoint(on_lon, on_lat) as gap_end_point,
   (positions_per_day_off > 5 AND positions_per_day_on > 5)
    AND positions_X_hours_before_sat >= 19 is_real_gap,
    gap_hours/24 > 14 over_two_weeks
 -- for spatial allocation, require start or end to be larger than 50 nautical miles
 -- to avoid counting gaps that are in port  

 from `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
WHERE gap_hours >= 12
    AND (off_distance_from_shore_m > 1852*50 AND on_distance_from_shore_m > 1852*50)
    AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
    and gap_id = '{gap_id}'
 ),


vessel_info as (
select 
  ssvid, 
  year,
  map_label(best_vessel_class) as vessel_class,
  best_flag as flag
from `world-fishing-827.gfw_research.fishing_vessels_ssvid_v20210301`
),

gap_raster_norm as (
  select
    x, y, hours, vessel_class, hours_diff, distance_km, days_to_start
  from
    proj_ais_gaps_catena.raster_gaps_norm_v20211021
),



with_mappings as (

select *,
map_distance(gap_distance_km) distance_km,
map_hours_diff(gap_hours) hours_diff
from gap_table
join
vessel_info
using(ssvid,year)),

with_mappings_center_course as
(select *,
90 - get_course_frommidpoint(gap_start_point, gap_end_point) theta,
get_midpoint(gap_start_point, gap_end_point) midpoint,
gap_hours / hours_diff as hours_adj,
gap_distance_km / distance_km as dist_adj
from with_mappings ),

joined_with_raster as (

select
10*(x*cos(radians(theta)) - y*sin(radians(theta)))*dist_adj km_east, -- 10 because each grid cell is 10km
10*(x*sin(radians(theta)) + y*cos(radians(theta)))*dist_adj km_north,
st_y(midpoint) lat_center,
st_x(midpoint) lon_center,
hours*hours_adj as hours,
gap_hours,
vessel_class,
flag,
is_real_gap
from gap_raster_norm
join
with_mappings_center_course
using(vessel_class, hours_diff, distance_km)
),


with_rotated_lat_lon as (
select *,
lat_center + km_north/deglat2km() lat,
lon_center + km_east/(deglat2km()*cos(radians(lat_center))) lon
from joined_with_raster
),

with_distance_to_shore as (

select * except(lat, lon), a.lat lat, a.lon lon
from
with_rotated_lat_lon a
left join
`pipe_static.distance_from_shore` b
    ON
      CAST( (a.lat*100) AS int64) = CAST( (b.lat*100) AS int64)
      AND CAST((a.lon*100) AS int64) =CAST(b.lon*100 AS int64)

)

select
vessel_class,
is_real_gap,
flag,
gap_hours > 7*24 over_one_week,
gap_hours > 14*24 over_two_weeks,
gap_hours > 7*24*4 over_four_weeks,
distance_from_shore_m >= 50*1852 as over_50_nm,
distance_from_shore_m >= 200*1852 as over_200_nm,
floor(lat*{scale}) lat_index,
floor(lon*{scale}) lon_index,
sum(hours) gap_hours
from
with_distance_to_shore
group by lat_index, lon_index,
vessel_class, over_50_nm, over_200_nm,
over_one_week, over_four_weeks, over_two_weeks, flag,
is_real_gap
'''

# %%
q_interpolate_template = '''#standardSQL

create temp function map_label(label string)
as (
  case when label ="drifting_longlines" then "drifting_longlines"
  when label ="purse_seines" then "purse_seines"
  when label ="other_purse_seines" then "purse_seines"
  when label ="tuna_purse_seines" then "purse_seines"
  when label ="cargo_or_tanker" then "cargo_or_tanker"
  when label ="cargo" then "cargo_or_tanker"
  when label ="tanker" then "cargo_or_tanker"
  when label ="squid_jigger" then "squid_jigger"
  when label ="tug" then "tug"
  when label = "trawlers" then "trawlers"
  else "other" end
);




with
# Best vessel class
vessel_info AS (
select 
  ssvid, 
  year,
  map_label(best_vessel_class) as vessel_class,
  best_flag as flag
from `world-fishing-827.gfw_research.fishing_vessels_ssvid_v20210301`
),


real_gaps as
(
select distinct gap_id
from
  `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
WHERE gap_hours >= 12
    AND (off_distance_from_shore_m > 1852*50 AND on_distance_from_shore_m > 1852*50)
    AND (positions_per_day_off > 5 AND positions_per_day_on > 5)
    AND (DATE(gap_start) >= '2017-01-01' AND DATE(gap_end) <= '2019-12-31')
    AND positions_X_hours_before_sat >= 19
)

select
floor(a.lat*{scale}) lat_index,
floor(a.lon*{scale}) lon_index,
vessel_class,
flag,
gap_hours > 7*24 over_one_week,
gap_hours > 14*24 over_two_weeks,
gap_hours > 7*24*4 over_four_weeks,
b.distance_from_shore_m > 1852*200 over_200_nm,
d.gap_id is not null is_real_gap,
b.distance_from_shore_m > 1852*50 as over_50nm,
count(*) gap_hours
from `world-fishing-827.proj_ais_gaps_catena.gap_positions_hourly_v20210722` a
join
vessel_info c
on a.ssvid = c.ssvid
and extract(year from a._partitiontime) = c.year
JOIN
  `world-fishing-827.pipe_static.distance_from_shore` b
ON
  cast( (a.lat*100) as int64) = cast( (b.lat*100) as int64)
  AND cast((a.lon*100) as int64) =cast(b.lon*100 as int64)
left join real_gaps d
using(gap_id)
where
date(_partitiontime) between "2017-01-01" and "2019-12-31" -- 3 years of data
 -- only analyzing more than 50 nautical miles from shore
 and
gap_hours > 12
and gap_id = '{gap_id}'
group by lat_index, lon_index, vessel_class, is_real_gap, over_200_nm, flag, over_50nm,
over_one_week, over_four_weeks, over_two_weeks'''

# %%
gap_char_query = '''SELECT  ssvid,vessel_class,
   gap_hours, gap_distance_m/1000 as gap_distance_km,
 from `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
WHERE gap_id = "{gap_id}"  '''


# %% [markdown]
# ## Visualization functions

# %%
def get_gap_rasters(gap_id):    
    dfr = gbq(q_raster_template.format(scale=scale, gap_id = gap_id))
    dfi = gbq(q_interpolate_template.format(scale=scale, gap_id = gap_id))
    dfg = gbq(gap_char_query.format(gap_id =gap_id))


    gap_raster = psm.rasters.df2raster(dfr,
                                   'lon_index', 'lat_index',
                                   'gap_hours', xyscale=scale, 
                                    per_km2=True, origin = 'lower')
    gap_interpolate = psm.rasters.df2raster(dfi,
                                   'lon_index', 'lat_index',
                                   'gap_hours', xyscale=scale, 
                                    per_km2=True, origin = 'lower')
    
    return dfg, gap_raster, gap_interpolate


# %%
def map_gap_raster(raster, extent, gs, norm, cmap='fishing', colorbar_label='hours per 1000/km$^{2}$', subplot_label=''):
    with pyseas.context(psm.styles.dark):
        with pyseas.context({'text.color' : '#000000'}):
            with plt.rc_context({
                        "axes.spines.right": False,
                        "axes.spines.top": False,
                        'legend.fontsize': 12, 
            }):
                ax = psm.create_map(gs, extent=extent)
                im = psm.add_raster(raster, ax=ax,
                                cmap=cmap,#'fishing',
                                norm=norm,
                                origin='lower'
                                )
                psm.add_land()
                cb = psm.colorbar.add_colorbar(im, ax=ax, label=colorbar_label, format='%.1f', 
                                               loc='bottom', right_edge=0.9, hspace=0.05, wspace=0.05)
                ax.spines['geo'].set_visible(False)
                ax.text(-0.08, 1.08, subplot_label, ha='left', va='top', transform=ax.transAxes, fontweight='bold', fontsize=14)
    
    return ax


# %% [markdown]
# ## Example 1

# %%
scale = 1

# %%
# Gap Example 1
gap_id_1 = 'ea44f17ad323523620da88f5ca424f76'
gap_info_1, gap_raster_1, gap_interpolate_1 = get_gap_rasters(gap_id_1)

# Gap Example 2
gap_id_2 = '90445f07692616180e808a01c56e2519'
gap_info_2, gap_raster_2, gap_interpolate_2 = get_gap_rasters(gap_id_2)

# Gap Example 3
gap_id_3 = 'ad8379a913d1d4f9ce4264aad409820d'
gap_info_3, gap_raster_3, gap_interpolate_3 = get_gap_rasters(gap_id_3)


# %%
fig = plt.figure(figsize=(8, 10), dpi=300, constrained_layout=True)
gs = fig.add_gridspec(3,2, wspace=0.0, hspace=0.1)

# Gap Example 1
extent = -110.0, -80.0, -15.0, 5.0
norm = mpcolors.Normalize(vmin=0, vmax=5)
ax_a = map_gap_raster(gap_interpolate_1*1000, extent, gs[0,0], norm, subplot_label='A.')
ax_b = map_gap_raster(gap_raster_1*1000, extent, gs[0,1], norm, subplot_label='B.')

# Gap Example 2
extent = -170.0, -80.0, -25.0, 25.0
norm = mpcolors.Normalize(vmin=0, vmax=0.2)
ax_c = map_gap_raster(gap_interpolate_2*1000, extent, gs[1,0], norm, subplot_label='C.')
ax_d = map_gap_raster(gap_raster_2*1000, extent, gs[1,1], norm, subplot_label='D.')

# Gap Example 3
extent = -170.0, -80.0, -30.0, 30.0
norm = mpcolors.Normalize(vmin=0, vmax=0.3)
ax_e = map_gap_raster(gap_interpolate_3*1000, extent, gs[2,0], norm, subplot_label='E.')
ax_f = map_gap_raster(gap_raster_3*1000, extent, gs[2,1], norm, subplot_label='F.')


ax_a.text(0.5,1.2, 'Linear Interpolation Method', ha='center', va='top', fontsize=14, transform=ax_a.transAxes)
ax_b.text(0.5,1.2, 'Raster Method', ha='center', va='top', fontsize=14, transform=ax_b.transAxes)
ax_a.text(-0.3,0.5, f'{round(gap_info_1.iloc[0].gap_hours)} hours\n{round(gap_info_1.iloc[0].gap_distance_km)}km', 
          ha='center', va='center', fontsize=14, transform=ax_a.transAxes)
ax_c.text(-0.2,0.5, f'{round(gap_info_2.iloc[0].gap_hours)} hours\n{round(gap_info_2.iloc[0].gap_distance_km)}km', 
          ha='center', va='center', fontsize=14, transform=ax_c.transAxes)
ax_e.text(-0.25,0.5, f'{round(gap_info_3.iloc[0].gap_hours)} hours\n{round(gap_info_3.iloc[0].gap_distance_km)}km', 
          ha='center', va='center', fontsize=14, transform=ax_e.transAxes)


plt.savefig(figures_folder + f"S16_example_gaps_comparing_methods.png", dpi=300, bbox_inches="tight")

# %%

# %% [markdown]
# # How much activity in the raster method is close to the straight line between points?
#
# If we interpolate a line between the start and end of a gap, how much of the activity using the raster method will be within 1 degree -- or 111 km -- of this line?

# %%
close_to_line_query = '''
create temp function map_label(label string)
as (
  case when label ="drifting_longlines" then "drifting_longlines"
  when label ="purse_seines" then "purse_seines"
  when label ="other_purse_seines" then "purse_seines"
  when label ="tuna_purse_seines" then "purse_seines"
  when label ="cargo_or_tanker" then "cargo_or_tanker"
  when label ="cargo" then "cargo_or_tanker"
  when label ="tanker" then "cargo_or_tanker"
  when label ="squid_jigger" then "squid_jigger"
  when label ="tug" then "tug"
  when label = "trawlers" then "trawlers"
  else "other" end
);

create temp function map_distance(d float64)
as (
case when d < 10/2+3/2 then 3
when d >= 10/2+3/2 and d < 15 then 10
when d >= 15 and d <30 then 20
when d >= 30 and d < 60 then 40
when d >= 60 and d < 120 then 80
when d >= 120 and d <240 then 160
when d >= 240 and d < 480 then 320
when d >= 480 and d < 960 then 640
when d >= 960 then 1280
else null end
);

create temp function map_hours_diff(h float64) as (
case when h < 12 + 18.0 then 12
when h >= 12 + 18.0 and h < 36.0 then 24
when h >= 36 and h < 72 then 48
 when h >= 72 and h < 120 then 96
 when h >= 120 and h < 168 then 144
 when h >= 168 and h < 216 then 192
 when h >= 216 and h < 264 then 240
 when h >= 264 and h < 312 then 288
 when h >= 312 and h < 360 then 336
 when h >= 360 and h < 408 then 384
 when h >= 408 and h < 456 then 432
 when h >= 456 and h < 540 then 480
 when h >= 540 and h < 660 then 600
 when h >= 660 and h < 780 then 720
 when h >= 780 then 840

else null end
);



with

gap_table as
(select
        ssvid,
        off_lat,
        off_lon,
        on_lat,
        on_lon,
        gap_hours,
        gap_distance_m / 1000 as gap_distance_km,
        st_geogpoint(off_lon, off_lat) as gap_start_point,
        st_geogpoint(on_lon, on_lat) as gap_end_point,
        (positions_per_day_off > 5 and positions_per_day_on > 5)
        and positions_x_hours_before_sat >= 19 as is_real_gap,
        gap_hours / 24 > 14 as over_two_weeks
 -- for spatial allocation, require start or end to be larger than 50 nautical miles
 -- to avoid counting gaps that are in port  
from
  `world-fishing-827.proj_ais_gaps_catena.ais_gap_events_features_v20210722`
    where
        gap_hours >= 12
         and (
             off_distance_from_shore_m > 1852 * 50 and on_distance_from_shore_m > 1852 * 50
        )
        and (date(gap_start) >= '2017-01-01' and date(gap_end) <= '2019-12-31')


),



vessel_info as (
    select
        ssvid,
        map_label(best.best_vessel_class) as vessel_class
from
  `world-fishing-827.gfw_research.vi_ssvid_v20210301`
    where
        on_fishing_list_best
),

with_mappings as (
    select
        *,
        map_distance(gap_distance_km) as distance_km,
        map_hours_diff(gap_hours) as hours_diff
    from
        gap_table
    join
        vessel_info
        using (ssvid)),

gap_raster_norm as (
    select
        x,
        y,
        hours,
        vessel_class,
        hours_diff,
        distance_km,
        days_to_start,
        dist_to_line_km
    from
        proj_ais_gaps_catena.raster_gaps_norm_v20211021
),


mapped_to_raster as
(
    select
        *
 except(dist_to_line_km, hours),
        -- if the gap distance is bigger, expand
        dist_to_line_km * gap_distance_km / distance_km as dist_to_line_km,
        gap_hours * hours / hours_diff as hours
    from
        gap_raster_norm
    join
        with_mappings
        using (vessel_class, distance_km, hours_diff)

)

select
    floor(gap_hours / 24) as gap_days,
    sum(hours) as hours,
    sum(if(dist_to_line_km < 111, hours, 0)) / sum(hours) as frac_within_line,
    sum(if(dist_to_line_km < 111, hours, 0)) as hours_close
from
    mapped_to_raster
group by
    gap_days
order by
    gap_days
'''

df_close_to_line = gbq(close_to_line_query)

# %%
df_close_to_line['weeks'] = df_close_to_line.gap_days.apply(lambda x: int(x/7+1))
df_grouped = df_close_to_line.groupby('weeks').sum()
df_grouped['frac'] = df_grouped.hours_close/df_grouped.hours

with plt.rc_context({
            "axes.spines.right": False,
            "axes.spines.top": False,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            'legend.fontsize': 12,
            }):
    fig = plt.figure(figsize=(6,4))
    df_grouped[df_grouped.index<7]['frac'].plot(kind='bar')
    plt.ylabel("Fraction of time", fontsize=11)
    plt.xlabel("Duration of disabling event (weeks)", fontsize=11)
    plt.xticks(rotation=0, fontsize=10)
    plt.yticks(fontsize=10)

    plt.tight_layout()
    plt.savefig(figures_folder + f"S17_raster_method_activity_within_1deg_of_line.png", dpi=300, bbox_inches="tight")
    plt.show()

# %%
