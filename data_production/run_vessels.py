# run_vessels.py

import subprocess
from jinja2 import Template

from ais_disabling import config

#########################################################################
# VESSEL LIST
# Produce the list of fishing vessels used for the analysis
#########################################################################

with open('data_production/vessels/fishing_vessels.sql.j2') as f:
    template = Template(f.read())

query = template.render(
    destination_dataset = config.destination_dataset,
    destination_table = config.fishing_vessels_table,
    vi_version = config.vi_version,
    start_date = str(config.start_date),
    end_date = str(config.end_date)
    )

if config.test_run:
    print(query)

if config.test_run is False:
    subprocess.run("bq query".split(), input=bytes(query, "utf-8"))
