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

# # Justify 12 Hour Gaps
#
# Dec 21, 2021
# David Kroodsma



# # Create tables to estimate the number of satellites in range 
# (this only has to be run the first time)

from datetime import datetime, timedelta
import os


def execute_commands_in_parallel(commands):
    '''This takes a list of commands and runs them in parallel
    '''
    with open('commands.txt', 'w') as f:
        f.write("\n".join(commands))    
    os.system("parallel -j 32 < commands.txt")
    os.system("rm -f commands.txt")


dist_to_sat_table = 'proj_ais_gaps_catena.dist_to_sat_grid'
os.system("bq mk --time_partitioning_type=DAY "+ dist_to_sat_table)


# +

q = '''

-- This query produces the number of satellites over the horizon
-- at every minute of the day for a given day at a one degree grid

CREATE TEMP FUNCTION earthRadius_m() AS (6356e3);
 
with lon_array_table as (
select GENERATE_ARRAY(-180,179,1) as lon_array),

lon_array_table2 as (
select 0 as lon 
),

lat_array_table as (
select GENERATE_ARRAY(-90,89,1) as lat_array),

lat_array_table2 as (
select lat + .5 as lat from
lat_array_table cross join unnest(lat_array) as lat),

lats_lons as (
select lon, lat from 
lat_array_table2 cross join lon_array_table2),

dist_to_horizon as (
select *, 
-- equation from Wikipedia, which is always right: https://en.wikipedia.org/wiki/Horizon#Distance_to_the_horizon
earthRadius_m()*acos(earthRadius_m()/(earthRadius_m() + altitude)) as horizon from 
`satellite_positions_v20190208.satellite_positions_one_second_resolution_{{ thedate }}` 
where 
-- extract(hour from timestamp)<12
-- because of symmetry, you can do 12 hours
-- and 
extract(second from timestamp) = 0 
or extract(second from timestamp) = 30
)


select 
floor(a.lat) as lat_bin, 
floor(a.lon) as lon_bin, 
timestamp_trunc( timestamp, minute) minute,
count(distinct timestamp)/(2) fraction_hour_inrange,
count(*)/(2) as average_number_sats_inrange
from lats_lons a
cross join
dist_to_horizon b
where st_distance(ST_GEOGPOINT(a.lon,
      a.lat),ST_GEOGPOINT(b.lon,
      b.lat) ) < horizon
group by lat_bin, lon_bin, minute
'''

with open("dist_to_sat_min.sql.j2", 'w') as f:
    f.write(q)

# -

dist_to_sat_table = 'proj_ais_gaps_catena.sats_overhead_min'
os.system("bq mk --time_partitioning_type=DAY "+ dist_to_sat_table)


# +
commands = []

startdate = datetime(2020,1,1)

for i in range(60):
    d = startdate + timedelta(days=i)
    thedate = d.strftime("%Y%m%d")
    dist_to_sat_table_oneday = dist_to_sat_table + "\$" +thedate
    
    command = (  f"jinja2 dist_to_sat_min.sql.j2 "
    f"-D thedate='{thedate}'  "
     "| "
    "bq query --replace "
    f"--destination_table={dist_to_sat_table_oneday} "
    f"--allow_large_results --use_legacy_sql=false ")
    commands.append(command)

# -

print(commands[0])

# +
## uncomment to run
# execute_commands_in_parallel(commands)
# -



# ## Do the analysis

# +
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
import pandas as pd
import pyseas.maps as psm
import pyseas.contrib as psc
import pyseas.cm
# %matplotlib inline
from datetime import datetime, timedelta
from scipy.fft import fft, ifft,fftfreq
import proplot as pplt
import matplotlib as mpl

plt.rcParams['figure.facecolor'] = 'white'
plt.rcParams['axes.facecolor'] = 'white'
mpl.rcParams["axes.spines.right"] = False
mpl.rcParams["axes.spines.top"] = False

def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')


# +

q = '''select 
timestamp_trunc(minute, hour) hour,
lat_bin,
avg(average_number_sats_inrange) average_number_sats_inrange 
from proj_ais_gaps_catena.sats_overhead_min 
where lon_bin = 0
and lat_bin in (0,30,60,85)
group by hour, lat_bin
order by hour'''

df = gbq(q)

# +
array = [[1,1,2,2,3,3],
        [4,4,5,5,6,6],
        [7,7,8,8,9,9],
        [10,10,11,11,12,12]]

#cmap =plt.get_cmap('viridis')#cmocean.cm.haline

fig, axs = pplt.subplots(array, figwidth=10, figheight=10, sharex = True, sharey = False)#,axwidth=1.1, wratios=(3) , aspect=1
 # share=1

for j,  the_lat in enumerate([0,30,60,85]):
    stdevs =[]
    
    d = df[df.lat_bin == the_lat]
    a = axs[j*3]
    a.plot(d.hour.values, d.average_number_sats_inrange.values)
    a.set_xlim(datetime(2020,1,1), datetime(2020,1,4))
    a.set_ylim(0,10)
    
    # fourier transform
    
    y = d.average_number_sats_inrange.values - d.average_number_sats_inrange.mean()
    N = len(y)
    yf = fft(y)
    xf = fftfreq(N,1)

    a = axs[j*3+1]
    a.plot(xf[:N//2]*24, 2.0/N * np.abs(yf)[:N//2])
    a.set_xlim(0,12)
    
    # standard deviation map
    for i in range(1,24*3):
        d['temp'] = d.average_number_sats_inrange.rolling(window=i).mean()
        stdevs.append(d['temp'].std())
    
    a = axs[j*3+2]
    a.plot([i for i in range(1,24*3)], stdevs)
    a.set_xlim(0,48)
    a.set_ylim(0,2)
#     a.set_xlabel("hours of window")
#     a.set_ylabel("standard deviation")
    
#     if j < 3:
#         axs[j*3].axes.xaxis.set_ticklabels([])
#         axs[j*3+1].axes.xaxis.set_ticklabels([])
#         axs[j*3+2].axes.xaxis.set_ticklabels([])

axs[10].text(4,-.3,"days^-1\n(6 is 1/6 of day,\n2 = 12 hours)")
axs[9].set_xlabel("date")
axs[11].set_xlabel("hours of window")

axs.format(
     abc='A.', titleloc='l', #title=titles,
    toplabels=('Satellites overhead', 'Fourier Transform', 'Standard deviation at different windows'),
#     xlabel='hours of gap', ylabel='gap distance, km', 
#     suptitle='Satellite Coverage Periodicity',
    leftlabels=[f'{lat} degrees lat' for lat in [0,30,60,85]]
)

# -




