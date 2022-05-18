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

# # Visualize the Probability Rasters

# +
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import pandas as pd
import proplot as pplt

# %matplotlib inline


def gbq(q):
    return pd.read_gbq(q, project_id="world-fishing-827")
# -



def show_gaps(distance_km, days, vmin=.001, vmax=1, max_dist = 1000, log = True, max_value =1):


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

    dfr = gbq(q)
    
    array = [[1,1,2,2],
        [3,3,4,4],
        [5,5,0,0]]


    fig, axs = pplt.subplots(array, figwidth=9*.56, figheight=17.5*.75*3/5,
                             aspect=1)#,axwidth=1.1, wratios=(3)
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


    for i, vessel_class in enumerate(dfr.vessel_class.unique()):

        grid = np.zeros(shape=(int(max_dist/10*2),int(max_dist/10*2)))
        d = dfr[dfr.vessel_class==vessel_class]
        d.apply(fill_grid, axis=1)
        cbar = axs[i].imshow(grid, interpolation=None,extent=[-max_dist,max_dist,-max_dist,max_dist],
                            norm=norm)
        axs[i].set_title(f"{vessel_class}")



    axs.format(
        abc=True, abcstyle='a.', titleloc='l', #title=titles,
        xlabel='km', ylabel='km', 
        suptitle=f'{distance_km} km, {days} days'
    )
    fig.colorbar(cbar, loc='b', label='hours',norm=norm)
    plt.show()

# +
# show_gaps(distance_km = 1280, days = 16, max_dist = 2000)
# -

show_gaps(distance_km = 1280, days = 16, max_dist = 1000, log = False, max_value = .5)

for days in [4,8,14]:
    show_gaps(distance_km = 1280, days = days, max_dist = 1000, log = False, max_value = .5)

# +
# for days in [6,8,10,12,14,16]:
#     show_gaps(distance_km = 1280, days = days, max_dist = 2000)
# -

for days in [2,8,14,30]:
    show_gaps(distance_km = int(1280/4), days = days,
              max_dist = 300, log = False, max_value = 2)

for days in [2,8,14,30]:
    show_gaps(distance_km = int(1280/8), days = days, 
              max_dist = 500, log = False, max_value = 2)




