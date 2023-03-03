#download_disabling_events.py

import pandas as pd
import os
from ais_disabling import config_ole

tables_to_download = config_ole.tables_to_run

if not os.path.exists('data/ole'):
    os.makedirs('data/ole')

###########################
# Download OLE gaps
###########################

if 'gaps' in tables_to_download:
    query = f"""
    SELECT *
    FROM `{config_ole.destination_dataset}.{config_ole.gap_events_features_table}`
    """

    # Download data
    gap_df = pd.read_gbq(query, project_id='world-fishing-827')

    # Save to csv
    gap_df.to_csv(
        f'data/ole/ole_gap_events.zip',
        index = False,
        compression = dict(method='zip', archive_name=f'ole_gap_events.csv')
        )

##############################
# Download OLE fishing vessels
##############################

if 'vessels' in tables_to_download:
    query = f"""
    SELECT *
    FROM `{config_ole.destination_dataset}.{config_ole.fishing_vessels_table}`
    """

    # Download data
    vessels_df = pd.read_gbq(query, project_id='world-fishing-827')

    # Save to csv
    vessels_df.to_csv(
        f'data/ole/ole_vessels.zip',
        index = False,
        compression = dict(method='zip', archive_name=f'ole_vessels.csv')
        )

##############################
# Download OLE vessel presence
##############################

if 'fishing' in tables_to_download:
    
    if not os.path.exists('data/ole/fishing'):
        os.makedirs('data/ole/ole_fishing')

    for y in range(2017,2023):
        for m in range(1,13):
            print(f'Downloading fishing vessel data for {y}-{m}')
    
            query = f"""
            SELECT *
            FROM `{config_ole.destination_dataset}.{config_ole.fishing_table}`
            WHERE EXTRACT(year FROM date) = {y}
            AND EXTRACT(month FROM date) = {m}
            """            

            # Download data
            fishing_df = pd.read_gbq(query, project_id='world-fishing-827')

            # Save to csv
            fishing_df.to_csv(
                f'data/ole/ole_fishing/ole_fishing_{y}_{m}.csv',
                index = False
                )