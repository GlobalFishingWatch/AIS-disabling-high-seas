# utils.py

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta

import scipy
import scipy.interpolate

import pyseas
import pyseas.maps
import pyseas.maps.rasters
import pyseas.styles
import pyseas.cm

from google.cloud import bigquery
# Establish BigQuery connection
client = bigquery.Client()

"""
General parameters
"""
# Min/Max coordinates 
min_lon, min_lat, max_lon, max_lat  = -180, -90, 180, 90

# Number of lat/lon bins
inverse_delta_degrees = 1
n_lat = (max_lat - min_lat) * inverse_delta_degrees
n_lon = (max_lon - min_lon) * inverse_delta_degrees

lons = np.arange(min_lon, max_lon+1)
lats = np.arange(min_lat, max_lat+1)

def execute_commands_in_parallel(commands):
    '''This takes a list of commands and runs them in parallel
    Note that this assumes you can run 16 commands in parallel,
    your mileage may vary if your computer is old and slow.
    Requires having gnu parallel installed on your machine.
    '''
    with open('commands.txt', 'w') as f:
        print(commands)
        f.write("\n".join(commands))    
    
    os.system("parallel -j 16 < commands.txt")
    os.system("rm -f commands.txt")

# Create set of dates
def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)   
    
# Get BQ partitions already processed    
def already_processed(dataset = "gfw_research", 
                      table = "pipe_v20201001", 
                      partition = '_partitiontime',
                      min_date = '2017-01-01'):
    
    '''
    this checks the tables present in a dataset and returns an empty list if
    '''  
    q = '''SELECT * 
    FROM {dataset}.__TABLES__
    WHERE table_id = '{table}'
    '''.format(table=table, dataset=dataset)
    df = pandas_gbq.read_gbq(q, project_id="world-fishing-827")
    if len(df)==0:
        return []
    
    '''
    this returns a list of strings, in format YYYY-MM-DD, that already exist as 
    date in a given dataset and table
    '''    
    q = '''
    SELECT DISTINCT
    {partition} as date
    FROM {dataset}.{table}
    WHERE {partition} >= {min_date}
    GROUP BY date 
    ORDER BY date
    '''.format(table=table, dataset=dataset,partition=partition)
    df = pandas_gbq.read_gbq(q, project_id="world-fishing-827")
    dt = list(df.date)    
    ap = map(lambda x: x.strftime("%Y-%m-%d"), dt)
    return list(ap)

# Create empty date partitioned BQ table
def make_bq_partitioned_table(dataset, 
                              table):
    cmd = "bq mk --time_partitioning_type=DAY {}.{}".format(dataset, table)
    os.system(cmd)

# Off/on events function
def make_ais_events_table(pipeline_table, 
                          segs_table, 
                          event_type, 
                          date, 
                          min_gap_hours, 
                          precursors_dataset, 
                          destination_table):
    
    # Format date string without dashes for partition
    dest_table_partition = destination_table + "\$"+ date.replace("-","")
    
    # Format jinja2 command
    cmd = """jinja2 gaps/ais_off_on_events.sql.j2 \
    -D pipeline_table="{pipeline_table}" \
    -D segs_table="{segs_table}" \
    -D event="{event_type}" \
    -D date="{date}" \
    -D min_gap_length={min_gap_hours} \
    | \
    bq query --replace \
    --destination_table={precursors_dataset}.{destination_table}\
    --allow_large_results --use_legacy_sql=false --max_rows=0
    """.format(pipeline_table=pipeline_table,
               segs_table=segs_table,
               event_type=event_type,
               date=date, 
               min_gap_hours=min_gap_hours,
               precursors_dataset=precursors_dataset,
               destination_table=dest_table_partition)
    return cmd


# Gap events function
def make_ais_gap_events_table(off_events_table,
                              on_events_table,
                              date, 
                              precursors_dataset,
                              destination_dataset, 
                              destination_table):
        
    # Format jinja2 command
    cmd = """jinja2 gaps/ais_gap_events.sql.j2 \
    -D off_events_table="{precursors_dataset}.{off_events_table}" \
    -D on_events_table="{precursors_dataset}.{on_events_table}" \
    -D date="{date}" \
    | \
    bq query --replace \
    --destination_table={destination_dataset}.{destination_table}\
    --allow_large_results --use_legacy_sql=false --max_rows=0
    """.format(off_events_table=off_events_table,
               on_events_table=on_events_table,
               date=date,
               precursors_dataset=precursors_dataset,
               destination_dataset=destination_dataset,
               destination_table=destination_table)
    return cmd

####################################
#
# Interpolation functions
#
# ###################################

def make_hourly_interpolation_table(destination_table,
                                    destination_dataset,
                                    date):

    # Update partition of destination table
    destination_table = destination_table + "\$" + date.replace('-','')
    
    # Set date as YYYY-MM-DD format
    YYYY_MM_DD = date[:4] + "-" + date[4:6] + "-" + date[6:8]
    
    # Format command
    cmd = '''jinja2 interpolation/hourly_interpolation_byseg.sql.j2    \
       -D YYYY_MM_DD="{YYYY_MM_DD}" \
       | \
        bq query --replace \
        --destination_table={destination_dataset}.{destination_table}\
         --allow_large_results --use_legacy_sql=false '''.format(YYYY_MM_DD = date,
                                                                 destination_dataset=destination_dataset,
                                                                 destination_table=destination_table)
    return cmd

def make_hourly_fishing_interpolation_table(destination_table,
                                            destination_dataset,
                                            date):

    # Update partition of destination table
    destination_table = destination_table + "\$" + date.replace('-','')
    
    # Set date as YYYY-MM-DD format
    YYYY_MM_DD = date[:4] + "-" + date[4:6] + "-" + date[6:8]
    
    # Format command
    cmd = '''jinja2 interpolation/hourly_fishing_interpolation.sql.j2    \
       -D YYYY_MM_DD="{YYYY_MM_DD}" \
       | \
        bq query --replace \
        --destination_table={destination_dataset}.{destination_table}\
         --allow_large_results --use_legacy_sql=false '''.format(YYYY_MM_DD = date,
                                                                 destination_dataset=destination_dataset,
                                                                 destination_table=destination_table)
    return cmd

####################################
#
# Reception functions
#
# ###################################

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

# Reception measured function
def make_reception_measured_table(destination_table,
                                  destination_dataset,
                                  start_date,
                                  vi_version,
                                  segs_table,
                                  output_version):
    
    # Set end date to end of month of start date
    reception_start = str(start_date.date())
    # End date (not inclusive)
    reception_end = start_date + relativedelta(months=1)
    reception_end = str(reception_end.date())
    
    # Format date string without dashes for partition
    # Use start of month as partition date
    destination_table = destination_table + "\$"+ reception_start.replace("-","")

    # Format command
    cmd = '''jinja2 reception/reception_measured.sql.j2    \
       -D start_date="{start_date}" \
       -D end_date="{end_date}" \
       -D vi_version="{vi_version}" \
       -D segs_table="{segs_table}" \
       -D destination_dataset="{destination_dataset}" \
       -D output_version="{output_version}" \
       | \
        bq query --replace \
        --destination_table={destination_dataset}.{destination_table} \
         --allow_large_results --use_legacy_sql=false'''.format(start_date = reception_start,
                                                                end_date = reception_end,
                                                                destination_dataset=destination_dataset,
                                                                destination_table=destination_table,
                                                                vi_version = vi_version,
                                                                segs_table = segs_table,
                                                                output_version = output_version)
    return cmd

# Reception interpolation function
def interpolate_reception(y, hours, hours_cap=15, elevation_scale=10, 
                          smooth=1, epsilon=None, hours_threshold=0,
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

#
# Create smooth reception table 
#
def make_smooth_reception_table(start_date,
                                reception_measured_table,
                                destination_dataset,
                                destination_table):
    
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
    
    month_reception_query = '''SELECT * 
                               FROM `{d}.{t}`
                               WHERE _partitiontime = "{m}"'''.format(d = destination_dataset,
                                                                    t = reception_measured_table,
                                                                    m = str(reception_start.date()))
            
    month_reception = pd.read_gbq(month_reception_query, project_id='world-fishing-827', dialect='standard')
    
    # Generate Class A and B grids from ping_density query results
    A_grids, B_grids = make_grids(month_reception, ['sat_pos_per_day', 'hours'])

    ### Interpolated reception ###
    print("Interpolating reception for {}".format(reception_start))
    # Interpolate reception for Class A
    smoothed_A_reception = interpolate_reception(A_grids['sat_pos_per_day'], A_grids['hours'])
    # Interpolate reception for Class A
    smoothed_B_reception = interpolate_reception(B_grids['sat_pos_per_day'], B_grids['hours'])    
    
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

    """
    Upload to BigQuery
    """
    # BigQuery references
    dataset_ref = client.dataset(destination_dataset)
    table_ref = dataset_ref.table(destination_table)
    
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    with open("tmp.csv", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows for {} into {}.{}".format(job.output_rows, reception_start, 
                                                    destination_dataset, destination_table))

# Plot reception quality
def plot_reception_quality(reception_start_date,
                           destination_dataset,
                           reception_smoothed_table):
    """
    Query smoothed reception data
    """
    month_reception_query = '''SELECT * 
                               FROM `{d}.{t}`
                               WHERE _partitiontime = "{m}"'''.format(d = destination_dataset,
                                                                      t = reception_smoothed_table,
                                                                      m = str(reception_start.date()))
    
    # Query data
    month_reception = pd.read_gbq(month_reception_query, project_id='world-fishing-827', dialect='standard')
    
    # remove NaNs
    df = month_reception.dropna(subset=['lat_bin','lon_bin'])

    # Class A
    class_a_reception = pyseas.maps.rasters.df2raster(df[df.cls == 'A'], 
                                                      'lon_bin', 'lat_bin','sat_pos_per_day',
                                                      xyscale=1, 
                                                      per_km2=False)

    # Class B
    class_b_reception = pyseas.maps.rasters.df2raster(df[df.cls == 'B'], 
                                                      'lon_bin', 'lat_bin','sat_pos_per_day',
                                                      xyscale=1, 
                                                      per_km2=False)
    """
    Plot 
    """
    # Figure size (one panel plot)
    fig = plt.figure(figsize=(5,5))

    titles = ["Class A",
              "Class B"]

    with pyseas.context(pyseas.styles.light): 

        fig_min_value = 1
        fig_max_value = 400  

        # Class A
        grid = class_a_reception
        grid[grid<fig_min_value/fig_max_value]=np.nan
        norm = colors.LogNorm(vmin=fig_min_value, vmax=fig_max_value)

        ax, im = pyseas.maps.plot_raster(grid,
                                         cmap = 'reception',
                                         norm = norm)

        ax.set_title("Class A")
        
        cbar = fig.colorbar(im, ax=ax,
                  orientation='horizontal',
                  fraction=0.02,
                  aspect=60,
                  pad=0.02,
                 )
    
        cbar.set_label("Satellite AIS positions per day")
        plt.tight_layout(pad=0.5)
    