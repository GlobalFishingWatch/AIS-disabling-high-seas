# utils.py

import os
from datetime import datetime, timedelta, date

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
def make_bq_partitioned_table(dataset, table):
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
####################################

def make_hourly_interpolation_table(destination_table,
                                    destination_dataset,
                                    date):

    # Update partition of destination table
    destination_table = destination_table + "\$" + date.replace('-','')
    
    # Set date as YYYY-MM-DD format
    YYYY_MM_DD = date[:4] + "-" + date[4:6] + "-" + date[6:8]
    
    # Format command
    cmd = '''jinja2 interpolation/hourly_interpolation.sql.j2    \
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
####################################

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