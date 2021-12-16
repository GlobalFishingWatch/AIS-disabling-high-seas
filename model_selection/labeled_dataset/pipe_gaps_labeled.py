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
#     display_name: gfw-viz
#     language: python
#     name: gfw-viz
# ---

# # AIS Gaps Training Set Pipeline
#
# ## Overview
#
# This notebook executes the following query to produce a labeled training set of 12+ hour AIS gaps using Exact Earth positions:
#
# `ais_gap_events_labeled.sql.j2`: This query calculates Exact Earth statistics and produces a final labeling for gaps that are covered by the Exact Earth datasets. Only gaps 12+ hours long are retained. Results are saved to: `proj_ais_gaps_catena.ais_gap_events_labeled_vYYYYMMDD`.

from google.cloud import bigquery
from jinja2 import Template

# ## Table Definitions
#
# Define the names of the BigQuery datasets and tables to create and/or query

from config import GAPS_FEATURES, GAPS_LABELED

# ## Create ais_gap_events_labeled_vYYYYMMDD

# +
with open("ais_gap_events_labeled.sql.j2") as f:
    sql_template = Template(f.read())

q = sql_template.render(
    GAPS_FEATURES=GAPS_FEATURES,
)

### Uncomment print statement is query desired
# print(q)

### Save the table to BigQuery
client = bigquery.Client()
job_config = bigquery.QueryJobConfig(
    destination=gaps_classified, write_disposition="WRITE_TRUNCATE"
)
query_job = client.query(q, job_config=job_config)
query_job.result()

print("Query results loaded to the table {}".format(GAPS_LABELED))
# -


