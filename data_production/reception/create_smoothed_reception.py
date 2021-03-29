# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Create Smoothed Reception
#
# This notebook creates smoothed reception maps at desired time intervals and uploads the tables to BigQuery.

# +
import pandas as pd
import cartopy.crs as ccrs
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
import time

from cartopy import config
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from matplotlib import colors,colorbar, cm
import cmocean

import scipy
import scipy.interpolate

# %matplotlib inline
# -

from google.cloud import bigquery
# Establish BigQuery connection
client = bigquery.Client()


# ## Setup
#
# ### Helper Functions

# +
def make_grids(df, names):
    """
    Parameters
    ----------
    df : Pandas DataFrame
        The data frame should contain gridded data that is gridded
        to match inverse_delta_degrees, where lat and lon are
        specified as `lat_bin` and `lon_bin`. Whether a given
        row corresponds to class A or class B is specified by
        the `cls` variable.
    names: list of str
        Name to extract from the data frame and place in a grid
        
    Returns
    -------
    A_grids, B_grids : dict of arrays
        The dictionary keys are the draw from the passed in names.
    """
    A_grids = {k : np.zeros([n_lat, n_lon]) for k in names}
    B_grids = {k : np.zeros([n_lat, n_lon]) for k in names}
    for row in df.itertuples():
        if np.isnan(row.lat_bin) or np.isnan(row.lon_bin):
            continue
        lat_ndx = int((row.lat_bin - min_lat) * inverse_delta_degrees)
        lon_ndx = int((row.lon_bin - min_lon) * inverse_delta_degrees)
        grids = A_grids if (row.cls == 'A') else B_grids
        for k in names:
            grids[k][lat_ndx][lon_ndx] = getattr(row, k)
    return A_grids, B_grids

def plot_grid(data, normtype=colors.LogNorm, cmap=cm.viridis, min_val=1, max_val=400, figsize=(12, 6)):
    lons = np.arange(min_lon, max_lon+1)
    lats = np.arange(min_lat, max_lat+1)

    norm = normtype(vmin=min_val, vmax=max_val)
    fig = plt.figure(figsize=figsize)
    ax = plt.axes(projection=ccrs.Robinson())
    plt.pcolormesh(lons, lats, data, norm=norm, cmap=cmap, transform = ccrs.PlateCarree())
    ax.coastlines(linewidth=.5)
    ax.add_feature(cfeature.LAND, zorder=10)
    
    ax = fig.add_axes([0.35, 0.05, 0.3, 0.02])  
    cb = colorbar.ColorbarBase(ax, norm=norm, cmap=cmap, orientation='horizontal')
    

    return ax

def interpolate_reception(y, hours, hours_cap=15, elevation_scale=10, smooth=1, epsilon=None, hours_threshold=0,
                rough_draft=True):
    """
    The strategy is to use RBF to interpolate the existing points onto a smooth grid. 
    There does not seem to be any provision for weighting but we exploit a third dimension 
    to apply psudo weighting to the data. Ideally we would use a Haversine metric, but there
    is no built in haversine metric and using a custom metric is very slow. 
    As a result, opting for the default Euclidean metric.
    """
    # Create the interpolation grid.
    lonvec, latvec = np.meshgrid(np.linspace(min_lon, max_lon, n_lon, endpoint=False), 
                                 np.linspace(min_lat, max_lat, n_lat, endpoint=False))

    y = y.copy()
    mask = (hours > hours_threshold)
    if rough_draft:
        # This thins the mask by a factor of 4
        # and is much faster. Useful for experimenting
        for i in range(0, 1):
            mask[:, i::2] = False
            mask[i::2, :] = False

    # Implement pseudo weighting by placing points with
    # fewer hours "higher" than other points so they
    # end up farther away, and effectively downweighting.
    # At inference time all points have elevation zero.
    elevation = elevation_scale * (hours_cap - np.minimum(hours, hours_cap))
    
    # Asssume good reception at poles
    good = np.percentile(y[mask], 90)
    mask[0, :] = mask[-1, :] = True
    y[0, :] = y[-1, :] = good
    elevation[0, :] = elevation[-1, :] = 0
    
    # Since we are using a Euclidean metric, we paste three copied
    # of the data together to avoid a problem at the dateline. We only
    # add 20 degrees worth of data to either side to keep the problem size in 
    # check
    west_mask = mask & (lonvec > 120)
    east_mask = mask & (lonvec < -120)

    latf = np.concatenate([latvec[west_mask].ravel(), latvec[mask].ravel(), latvec[east_mask].ravel()])
    elevf = np.concatenate([elevation[west_mask].ravel(), elevation[mask].ravel(), 
                            elevation[east_mask].ravel()])
    yf = np.concatenate([y[west_mask].ravel(), y[mask].ravel(), y[east_mask].ravel()])
    # One copy of the longitude data is shifted west and one east.
    lonf = np.concatenate([(lonvec[west_mask] - 360).ravel(), lonvec[mask].ravel(), 
                           (lonvec[east_mask] + 360).ravel()])

    # There are two primary knobs to twiddle here, `smooth` and `epsilon`. With `smooth`
    # set to zero, it does an exact fit and larger values result in a smoother less 
    # exact fit. `epsilon` is a scale parameter and also affects smoothness, but I haven't
    # played with it as much. The `function` parameter can also be manipulated, but I haven't
    # had any luck with anything other than default `multiquadric`. In addition, adjusting
    # `elevation_scale` will change the amount of downweighting applied to cells with
    # few hours
    interpolater = scipy.interpolate.Rbf(lonf, latf, elevf, yf, smooth=smooth, epsilon=epsilon)
    
    return interpolater(lonvec.flatten(), latvec.flatten(), 
                                    np.zeros_like(lonvec.flatten())).reshape(180, 360)


# -

# ### Helper Query
#
# The below query calculates the measured reception quality for the given month prior to interpolation.

def reception_measured(start_date, end_date):
    # TODO: remove fishing only from class A
    query = f"""
    CREATE TEMP FUNCTION startdate() AS (DATE('{start_date:%Y-%m-%d}'));
    CREATE TEMP FUNCTION enddate() AS (DATE('{end_date:%Y-%m-%d}'));
    #### Reception Quality
    with 

    good_ssvid as (
        select ssvid
        from `gfw_research.vi_ssvid_v20200410` 
        where best.best_vessel_class not in  ("gear", "squid_jigger", "pole_and_line")
          and not activity.offsetting 
          and activity.active_positions > 1000
          and best.best_vessel_class is not null
    ),
    sat_ssvid as (
        select ssvid,
               sat_positions,
               lat,
               lon,
               hour,
               interpolated_speed_knots,
               _partitiontime date ,
               A_messages,
               B_messages 
        from `gfw_research_precursors.ais_positions_byssvid_hourly_v20191118`
        where not interpolated_at_segment_startorend -- don't extrapolate out at the end of segments
          and date(_partitiontime) >= startdate() 
          and date(_partitiontime) < enddate()
          and ssvid in (select ssvid from good_ssvid)
    ),
    by_half_day as (
        select ssvid,
               avg(interpolated_speed_knots) avg_interpolated_speed_knots,
               min(interpolated_speed_knots) min_interpolated_speed_knots,
               max(interpolated_speed_knots) max_interpolated_speed_knots,
               sum(sat_positions)/count(*) sat_pos_per_hour,
               floor(hour/12) day_half,
               sum(A_messages) A_messages,
               sum(B_messages) B_messages,
               date
        from sat_ssvid
        group by ssvid, date, day_half
    ),
    reception_quality as (
        select floor(a.lat) lat_bin,
               floor(a.lon) lon_bin,
               if(by_half_day.A_messages > 0, "A", "B") class,
               count(*) hours,
               avg(sat_pos_per_hour) * 24 sat_pos_per_day
        from sat_ssvid a
        join by_half_day 
        on    a.ssvid=by_half_day.ssvid 
          and floor(a.hour/12) = by_half_day.day_half
          and a.date = by_half_day.date
          and (-- if Class A, moving at the speed to ping once every 10 seconds
               (by_half_day.A_messages > 0 and  min_interpolated_speed_knots > 0.5 
                and max_interpolated_speed_knots < 14)
               or (by_half_day.B_messages > 0 and min_interpolated_speed_knots > 2 ))
          -- make sure it is just class A or class B... this might make it fail
          -- for the vessels that are both A and B...
          and not (by_half_day.A_messages > 0 and by_half_day.B_messages > 0) 
          and max_interpolated_speed_knots < 30 -- eliminate some weird noise
        group by lat_bin, lon_bin, class
    )

    select class as cls, * except (class) from reception_quality
    """
    ping_density = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')
    
    return(ping_density)


# ### Parameters
#
# Setup global parameters and time range for generation reception maps.

# +
# Time interval for reception maps
reception_level = 'month'

# Min/Max coordinates 
min_lon, min_lat, max_lon, max_lat  = -180, -90, 180, 90

# Number of lat/lon bins
inverse_delta_degrees = 1
n_lat = (max_lat - min_lat) * inverse_delta_degrees
n_lon = (max_lon - min_lon) * inverse_delta_degrees

lons = np.arange(min_lon, max_lon+1)
lats = np.arange(min_lat, max_lat+1)
# -

# ## Table Setup
#
# Create the destination BigQuery table for the smoothed reception maps

# +
# Output location
destination_dataset = 'proj_ais_gaps_catena'
table_name = 'sat_reception_one_degree_v20200806'
destination_table = '{d}.{t}'.format(d=destination_dataset,t=table_name)

# BigQuery references
dataset_ref = client.dataset(destination_dataset)
table_ref = dataset_ref.table(table_name)
create_table = True
# -

# Create date partitioned table prior to running interpolation.

if(create_table):
    table = bigquery.Table(table_ref)
    table = client.create_table(table)
    print("Created table {}".format(table.table_id))

# ## Interpolation and Upload to BigQuery

# Time range over which to generate reception maps
start_date = datetime(2018, 9, 1)
end_date = datetime(2019, 12, 31)

while (start_date < end_date):
    
    """
    Generate smooth reception map for month
    """
    
    # Dates for reception map 
    reception_start = start_date
    # End date (not inclusive)
    reception_end = start_date + relativedelta(months=1)

    ### Measured reception ###
    # Query to calculate measured reception
    print("Querying reception for {}".format(reception_start))
    month_reception = reception_measured(reception_start, reception_end)
    
    # Generate Class A and B grids from ping_density query results
    A_grids, B_grids = make_grids(month_reception, ['sat_pos_per_day', 'hours'])

    ### Interpolated reception ###
    print("Interpolating reception for {}".format(reception_start))
    # Interpolate reception for Class A
    smoothed_A_reception = interpolate_reception(A_grids['sat_pos_per_day'], A_grids['hours'])
    # Interpolate reception for Class A
    smoothed_B_reception = interpolate_reception(B_grids['sat_pos_per_day'], B_grids['hours'])
    
    ### Plots ###
    ax = plot_grid(np.maximum(smoothed_A_reception, 1), max_val=500)
    ax.set_title(f"Smoothed pings per vessel day (class A, {reception_start:%Y-%m})")
    plt.show()
    
    ax = plot_grid(np.maximum(smoothed_B_reception, 1), max_val=100)
    ax.set_title(f"Smoothed pings per vessel day (class B, {reception_start:%Y-%m})")
    plt.show()
    
    """
    Convert data to pandas data frame and upload to BigQuery
    """
    # Empty list to store coordinates and values
    data_A = []
    data_B = []

    # Loop over lat/lon and fill with positions per hour
    for lat in range(n_lat):
        for lon in range(n_lon):
            # Add data to each list
            data_A.append([lats[lat], lons[lon], smoothed_A_reception[lat][lon]])
            data_B.append([lats[lat], lons[lon], smoothed_B_reception[lat][lon]])

    # Convert lists to pandas dataframes
    df_A = pd.DataFrame(data_A, columns=['lat_bin','lon_bin','positions_per_day'])
    df_A['class'] = 'A'

    df_B = pd.DataFrame(data_B, columns=['lat_bin','lon_bin','positions_per_day'])
    df_B['class'] = 'B'

    # Stack dataframes into single table
    df = pd.concat([df_A, df_B], axis = 0).reset_index(drop=True)

    # Add column for year and month
    df['year'] = reception_start.year
    df['month'] = reception_start.month

    # Rearrange columns
    df = df[['year','month','lat_bin','lon_bin','class','positions_per_day']]

    # Set index to remove index colum
    df = df.set_index('year')

    # Save data to tmp csv file
    df.to_csv('tmp.csv')

    # Upload to BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    with open("tmp.csv", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows for {} into {}.".format(job.output_rows, reception_start, table_name))
    
    # Update month for next iteration
    start_date = reception_end


