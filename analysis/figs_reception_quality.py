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

# # Reception Quality Figures

# +
import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import colors,colorbar, cm

import pyseas
import pyseas.maps
import pyseas.maps.rasters
import pyseas.styles
import pyseas.cm
import cmocean
# -

# Parameters:

# +
# Parameters
destination_dataset = 'proj_ais_gaps_catena'
output_version = 'v20210722'

# Reception quality tables
sat_reception_smoothed_tbl = 'sat_reception_smoothed_one_degree_{}'.format(output_version)
sat_reception_measured_tbl = 'sat_reception_measured_one_degree_{}'.format(output_version)

# +
# Direct notebook to see local modules for import
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

from data_production import utils
# -

# ## Smoothed reception quality

"""
Query smoothed reception data
"""
sat_reception_smooth_query = '''SELECT 
                                   lat_bin,
                                   lon_bin,
                                   class,
                                   AVG(positions_per_day) as positions_per_day
                                   FROM `{d}.{t}`
                                   WHERE _partitiontime BETWEEN "2017-01-01" 
                                   AND "2019-12-01"
                                   GROUP BY 1,2,3'''.format(d = destination_dataset,
                                                            t = sat_reception_smoothed_tbl)
# Query data
sat_reception_smooth = pd.read_gbq(sat_reception_smooth_query, project_id='world-fishing-827', dialect='standard')


# +
# Plot reception quality
def plot_reception_quality(reception_start_date,
                           reception_df,
                           fig_min_value = 1,
                           fig_max_value = 400
                          ):
    
    # remove NaNs
    df = reception_df.dropna(subset=['lat_bin','lon_bin'])
    

    # Class A
    class_a_reception = pyseas.maps.rasters.df2raster(df[df['class'] == 'A'], 
                                                      'lon_bin', 'lat_bin','positions_per_day',
                                                      xyscale=1, 
                                                      per_km2=False)

    # Class B
    class_b_reception = pyseas.maps.rasters.df2raster(df[df['class'] == 'B'], 
                                                      'lon_bin', 'lat_bin','positions_per_day',
                                                      xyscale=1, 
                                                      per_km2=False)
    """
    Plot 
    """
    # Figure size (one panel plot)
    fig = plt.figure(figsize=(10,10))
    
    titles = ["Class A", "Class B"]

#     titles = ["Class A {}".format(reception_start_date.date()),
#               "Class B {}".format(reception_start_date.date())]

    with pyseas.context(pyseas.styles.light): 

        axes = []
        ims = []

        # Class A
        grid = np.maximum(class_a_reception, 1)
        grid[grid<fig_min_value/fig_max_value]=100
        norm = colors.LogNorm(vmin=fig_min_value, vmax=fig_max_value)

        ax, im = pyseas.maps.plot_raster(grid,
                                         subplot=(2,1,1),
                                         cmap = 'reception',
                                         origin = 'upper',
                                         norm = norm)

        contours = ax.contour(grid,
           origin = 'upper',
           levels = list([5,25,50,100]),
           extent = (-180,180,-90,90),
           zorder = 1,
           colors = 'black',
           linestyles = 'dotted',
           norm = norm,
           transform = pyseas.maps.identity)

        ax.clabel(contours, zorder = 1, inline=True, fontsize=8)
        
        ax.set_title(titles[0])
        ax.text(-150*1000*100,80*1000*100, "A.")
        
        # Class B
        grid = np.maximum(class_b_reception, 1)
        grid[grid<fig_min_value/fig_max_value]=100
        norm = colors.LogNorm(vmin=fig_min_value, vmax=fig_max_value)

        ax, im = pyseas.maps.plot_raster(grid,
                                         subplot=(2,1,2),
                                         cmap='reception',
                                         origin = 'upper',
                                         norm = norm
                                        )
        
        contours = ax.contour(grid,
           origin = 'upper',
           levels = list([5,25,50,100]),
           extent = (-180,180,-90,90),
           zorder = 1,
           colors = 'black',
           linestyles = ('solid','dashdot','dashed','dotted'),
#            linestyles = 'dotted',
           norm = norm,
           transform = pyseas.maps.identity)

#         ax.clabel(contours, zorder = 1, inline=True, fontsize=8)
        
        ax.set_title(titles[1])
        ax.text(-150*1000*100,80*1000*100, "B.")
        
        cbar = fig.colorbar(im, ax=ax,
                  orientation='horizontal',
                  fraction=0.02,
                  aspect=60,
                  pad=0.02,
                 )
    
        cbar.set_label("Satellite positions per day")
        plt.tight_layout(pad=0.5)
# -

# Plot reception quality with same color scales
plot_reception_quality(reception_start_date = "2017-01-01",
                             reception_df = sat_reception_smooth,
                             fig_min_value = 1, 
                             fig_max_value = 100
                        )


