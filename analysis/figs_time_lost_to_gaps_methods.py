import os
import numpy as np
from math import floor
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import pandas as pd
from PIL import Image
import proplot as pplt
from jinja2 import Template
import pyseas
import pyseas.maps as psm
psm.use(psm.styles.chart_style)

from ais_disabling.figure_utils import get_figures_folder

import warnings
warnings.filterwarnings("ignore")

def gbq(q):
    return pd.read_gbq(q, project_id="world-fishing-827")

queries_folder = 'queries'

figures_folder = 'figures'
figures_folder_precursors = f'{figures_folder}/precursors'
if not os.path.exists(figures_folder):
    os.makedirs(figures_folder)
    os.makedirs(figures_folder_precursors)
if not os.path.exists(figures_folder_precursors):
    os.makedirs(figures_folder_precursors)

from ais_disabling.config import (
    gap_filters,
    min_positions_per_day,
    min_positions_before,
    min_gap_hours,
    min_distance_from_shore_m,
    tp,
    proj_dataset,
    gap_events_features_table,
    fishing_vessels_table,
    raster_gaps_norm_table,
    pipe_static_distance_from_shore,
    gap_positions_hourly,
)

TABLE_GAPS_FEATURES = f"{proj_dataset}.{gap_events_features_table}"
TABLE_FISHING_VESSELS = f"{proj_dataset}.{fishing_vessels_table}"
TABLE_RASTER_GAPS_NORM = f"{proj_dataset}.{raster_gaps_norm_table}"
TABLE_DISTANCE_FROM_SHORE = pipe_static_distance_from_shore
TABLE_GAPS_POSITIONS_HOURLY = f"{proj_dataset}.{gap_positions_hourly}"

# Query settings
START_DATE = tp[0]
END_DATE = tp[-1]

# Map Settings
SCALE = 1

# PROBABILITY RASTERS
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


def plot_probability_raster(df_raster, days, distance_km, vmin=.001, vmax=1,
                            max_dist = 1000, fig_label="", figures_folder=None):

    array = [[1,1,2,2],
            [3,3,4,4],
            [5,5,0,0]]

    fig, axs = pplt.subplots(array, figwidth=9*.56, figheight=17.5*.75*3/5,
                             aspect=1, top=8, bottom=8)
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
        axs[i].set_title(f"{title}", fontsize=14)


    axs.format(
        abc=False, titleloc='l',
        xlabel='km', ylabel='km',
        suptitle=f'{distance_km} km, {days} days',
        rc_kw={'suptitle.pad': -0.1, 'suptitle.size': 16}
    )
    fig.colorbar(cbar, loc='b', label='hours')

    axs[0].text(-0.2, 1.2, fig_label, ha='left', va='top', transform=axs[0].transAxes, fontweight='bold', fontsize=16)

    if figures_folder:
        plt.savefig(f"{figures_folder_precursors}/S15_probability_raster_{distance_km}km_{days}days.png", dpi=300, bbox_inches = 'tight')


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


def probability_rasters(figures_folder=None):

  distance_km = 320
  max_dist=300

  df_raster_2days = probability_raster(days=2, distance_km=distance_km)
  df_raster_8days = probability_raster(days=8, distance_km=distance_km)
  df_raster_14days = probability_raster(days=14, distance_km=distance_km)
  df_raster_30days = probability_raster(days=30, distance_km=distance_km)

  plot_probability_raster(df_raster_2days, 2, distance_km, max_dist=max_dist, vmax=2, fig_label="A.", figures_folder=figures_folder)
  plot_probability_raster(df_raster_8days, 8, distance_km, max_dist=max_dist, vmax=2, fig_label="B.", figures_folder=figures_folder)
  plot_probability_raster(df_raster_14days, 14, distance_km, max_dist=max_dist, vmax=2, fig_label="C.", figures_folder=figures_folder)
  plot_probability_raster(df_raster_30days, 30, distance_km, max_dist=max_dist, vmax=2, fig_label="D.", figures_folder=figures_folder)

  img_2days = f"{figures_folder_precursors}/S15_probability_raster_{distance_km}km_2days.png"
  img_8days = f"{figures_folder_precursors}/S15_probability_raster_{distance_km}km_8days.png"
  img_14days = f"{figures_folder_precursors}/S15_probability_raster_{distance_km}km_14days.png"
  img_30days = f"{figures_folder_precursors}/S15_probability_raster_{distance_km}km_30days.png"

  images = [[Image.open(img_2days), Image.open(img_8days)],
            [Image.open(img_14days), Image.open(img_30days)]]

  joined_image = join_images(*images)

  if figures_folder:
    joined_image.save(f"{figures_folder}/S15_probability_raster_{distance_km}km_all.png", format='png')
  return joined_image

# EXAMPLES OF SPATIALLY ALLOCATED GAPS
def get_gap_rasters(gap_id):
  with open(f"{queries_folder}/time_lost_to_gaps_raster.sql.j2") as f:
      sql_template = Template(f.read())

  q_raster_template = sql_template.render(
    TABLE_GAPS_FEATURES = TABLE_GAPS_FEATURES,
    TABLE_FISHING_VESSELS = TABLE_FISHING_VESSELS,
    TABLE_RASTER_GAPS_NORM = TABLE_RASTER_GAPS_NORM,
    TABLE_DISTANCE_FROM_SHORE = TABLE_DISTANCE_FROM_SHORE,
    SCALE = SCALE,
    GAP_ID = gap_id,
    MIN_POSITIONS_PER_DAY = min_positions_per_day,
    MIN_POSITIONS_BEFORE = min_positions_before,
    MIN_GAP_HOURS = min_gap_hours,
    MIN_DISTANCE_FROM_SHORE_M = min_distance_from_shore_m,
    START_DATE = START_DATE,
    END_DATE = END_DATE,
  )

  with open(f"{queries_folder}/time_lost_to_gaps_interpolate.sql.j2") as f:
      sql_template = Template(f.read())

  q_interpolate_template = sql_template.render(
    TABLE_GAPS_FEATURES = TABLE_GAPS_FEATURES,
    TABLE_FISHING_VESSELS = TABLE_FISHING_VESSELS,
    TABLE_GAPS_POSITIONS_HOURLY = TABLE_GAPS_POSITIONS_HOURLY,
    TABLE_DISTANCE_FROM_SHORE = TABLE_DISTANCE_FROM_SHORE,
    SCALE = SCALE,
    GAP_ID = gap_id,
    GAP_FILTERS = gap_filters,
    MIN_GAP_HOURS = min_gap_hours,
    START_DATE = START_DATE,
    END_DATE = END_DATE,
  )

  q_gap_char = f'''
    SELECT  ssvid,vessel_class,
    gap_hours, gap_distance_m/1000 as gap_distance_km,
    from `{TABLE_GAPS_FEATURES}`
    WHERE gap_id = "{gap_id}"
  '''

  dfr = gbq(q_raster_template)
  dfi = gbq(q_interpolate_template)
  dfg = gbq(q_gap_char)

  gap_raster = psm.rasters.df2raster(dfr,
                                  'lon_index', 'lat_index',
                                  'gap_hours', xyscale=SCALE,
                                  per_km2=True, origin = 'lower')
  gap_interpolate = psm.rasters.df2raster(dfi,
                                  'lon_index', 'lat_index',
                                  'gap_hours', xyscale=SCALE,
                                  per_km2=True, origin = 'lower')

  return dfg, gap_raster, gap_interpolate


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

def spatially_allocated_gaps(figures_folder=None):

  # Gap Example 1
  gap_id_1 = 'ea44f17ad323523620da88f5ca424f76'
  gap_info_1, gap_raster_1, gap_interpolate_1 = get_gap_rasters(gap_id_1)

  # Gap Example 2
  gap_id_2 = '90445f07692616180e808a01c56e2519'
  gap_info_2, gap_raster_2, gap_interpolate_2 = get_gap_rasters(gap_id_2)

  # Gap Example 3
  gap_id_3 = 'ad8379a913d1d4f9ce4264aad409820d'
  gap_info_3, gap_raster_3, gap_interpolate_3 = get_gap_rasters(gap_id_3)


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

  if figures_folder:
    plt.savefig(figures_folder + f"S16_example_gaps_comparing_methods.png", dpi=300, bbox_inches="tight")
  return fig




# How much activity in the raster method is close to the straight line between points?
#
# If we interpolate a line between the start and end of a gap, how much of the activity using the raster method will be within 1 degree -- or 111 km -- of this line?

def close_to_line(figures_folder=None):
  with open(f"{queries_folder}/close_to_line.sql.j2") as f:
      sql_template = Template(f.read())

  q_close_to_line = sql_template.render(
    TABLE_GAPS_FEATURES = TABLE_GAPS_FEATURES,
    TABLE_FISHING_VESSELS = TABLE_FISHING_VESSELS,
    TABLE_RASTER_GAPS_NORM = TABLE_RASTER_GAPS_NORM,
    MIN_POSITIONS_PER_DAY = min_positions_per_day,
    MIN_POSITIONS_BEFORE = min_positions_before,
    MIN_GAP_HOURS = min_gap_hours,
    MIN_DISTANCE_FROM_SHORE_M = min_distance_from_shore_m,
    START_DATE = START_DATE,
    END_DATE = END_DATE,
  )

  df_close_to_line = gbq(q_close_to_line)

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

      if figures_folder:
        plt.savefig(f"{figures_folder}/S17_raster_method_activity_within_1deg_of_line.png", dpi=300, bbox_inches="tight")
      return fig

if __name__ == "__main__":
    figures_folder = get_figures_folder(None)
    probability_rasters(figures_folder)
    spatially_allocated_gaps(figures_folder)
    close_to_line(figures_folder)
