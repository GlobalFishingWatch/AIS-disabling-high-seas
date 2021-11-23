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

# # Create Gridded Vessel Activity
#
#

# +
import subprocess
from jinja2 import Template
import pandas as pd

def gbq(q):
    return pd.read_gbq(q, project_id='world-fishing-827')


# -

# Version to run
output_version = '20211103'

# The following is just to print SQL to copy into
# the query RasterGaps.sql.j2
days = [1,2,4,6,8,10,12,14,16,18,20,25,30,35]

# +
times = '''time_ranges as (
    select
        11 as min_hours,
        13 as max_hours,
        12 as hours_diff
'''

for d in days:
    times += f'''union all
    select {int(round(d*24*.95))} as min_hours,
    {int(round(d*24*1.05))} as max_hours ,
    {d*24} hours_diff\n'''

times += "),"
print(times)

# +
# the following is to create the function to map time back to these
# # copy and paste the results back to

time_mapper = f"""create temp function map_hours_diff(h float64) as (
case when h < 12 + {days[0]*24/2+12/2} then 12
when h >= 12 + {days[0]*24/2+12/2} and h < {days[1]*24/2+days[0]*24/2} then {days[0]*24}\n"""

for i in range(1, len(days) - 1):
    lower = days[i - 1] * 12 + days[i] * 12
    upper = days[i] * 12 + days[i + 1] * 12
    time_mapper += f"when h >= {lower} and h < {upper} then {days[i]*24}\n "

# when h >= 12 + 6 and h < 24+12 then 24
# when h >= 24+12 and h < 48+24 then 48
# when h >= 48+24 and h < 96 + 48 then 96
# when h >= 96 + 48 and h < 192 + 96 then 192
# when h >= 192 + 96 and h < 384 + 192 then 384
time_mapper += f"""when h >= {days[-1]*12 + days[-2]*12}  then {days[-1]*24}

else null end
);"""

print(time_mapper)
# -
output_table = 'proj_ais_gaps_catena.raster_gaps_v{}'.format(output_version)
print(output_table)

with open('queries/RasterGaps.sql.j2') as f:
    template = Template(f.read())
query = template.render(output_table = output_table)
# print(query)


subprocess.run("bq query".split(), input=bytes(query, "utf-8"))

# ## Normalize
#
# normalize each raster so that the number of hours is equal to the number of hours of each gap.

# +

query_norm = f'''with source_table as
(select * from {output_table}),

grouped_table as (
select
x,y, sum(hours) hours, vessel_class, hours_diff,days_to_start, distance_km,
avg(dist_to_line_km) dist_to_line_km,
avg(dist_to_start_km) dist_to_start_km,
avg(dist_to_end_km) dist_to_end_km
from unioned_tables
group by x, y, vessel_class, hours_diff, days_to_start, distance_km),
##

tot_hours_table as
(select vessel_class, hours_diff, distance_km, sum(hours) tot_hours
from grouped_table
group by vessel_class, hours_diff, distance_km)

##
select * except(hours, tot_hours),
hours/tot_hours*hours_diff hours,
cast(round(tot_hours/hours_diff) as int64) tot_vessels from
grouped_table
join
tot_hours_table
using(vessel_class, hours_diff, distance_km)'''




# -

norm_table = "proj_ais_gaps_catena.raster_gaps_norm_v{}".format(output_version)
command = (  "bq query --replace "
     f"--destination_table={norm_table} "
     f"--allow_large_results --use_legacy_sql=false ")


subprocess.run(command.split(), input=bytes(query_norm, "utf-8"))

# # Use this table to spatially allocate gaps

with open('queries/AllocateGapsRaster.sql.j2') as f:
    template = Template(f.read())


raster_grid_table = 'proj_ais_gaps_catena.gaps_allocated_raster_v{}'.format(output_version)

query = template.render(output_table = raster_grid_table, scale = 1)
subprocess.run("bq query".split(), input=bytes(query, "utf-8"))

# ### Dowload data

# +
dlr_query = """
select * 
from `world-fishing-827.proj_ais_gaps_catena.gaps_allocated_raster_v{}`  
where over_200_nm 
and is_real_gap 
and not over_two_weeks
""".format(output_version)

df_raster = gbq(dlr_query)
# -

# Save as csv
df_raster.to_csv('../../results/gap_inputs_v20210722/figure_1_data_raster.csv')

# # Make an identical table, but using interpolated gaps

# +
interpolate_grid_table = "proj_ais_gaps_catena.gaps_allocated_interpolate_v{}".format(output_version)

with open("queries/AllocateGapsInterpolate.sql.j2") as f:
    template = Template(f.read())

query = template.render(output_table=interpolate_grid_table, scale=1)

subprocess.run("bq query".split(), input=bytes(query, "utf-8"))
# -

# ### Download data
#
# Download and save data.

# +
dl_query = """
select * 
from proj_ais_gaps_catena.gaps_allocated_interpolate_v{} 
where over_200_nm 
and is_real_gap 
and not over_two_weeks
""".format(output_version)

df_interpolate = gbq(dl_query)
# -

# Save as csv
df_interpolate.to_csv('../../results/gap_inputs_v20210722/figure_1_data_interpolate.csv')

# # Make gridded fishing effort table for comparison

# +
fishing_table = "proj_ais_gaps_catena.fishing_activity_v{}".format(output_version)

with open("queries/GridFishing.sql.j2") as f:
    template = Template(f.read())

query = template.render(output_table=fishing_table, scale=1)

subprocess.run("bq query".split(), input=bytes(query, "utf-8"))
# -


