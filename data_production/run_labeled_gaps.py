# # AIS Gaps Training Set Pipeline
#
# **Last updated:** 2022-06-24
#
# ## Overview
#
# This notebook executes the following query to produce a labeled training set of 
# 12+ hour AIS gaps using Exact Earth positions:
#
# `ais_gap_events_labeled.sql.j2`: This query calculates Exact Earth statistics 
# and produces a final labeling for gaps that are covered by the Exact Earth datasets. 
# Only gaps 12+ hours long are retained.

from google.cloud import bigquery
from jinja2 import Template

from ais_disabling.config import proj_dataset, gap_events_features_table, gap_events_labeled_table

GAPS_FEATURES = f"{proj_dataset}.{gap_events_features_table}"
GAPS_LABELED = f"{proj_dataset}.{gap_events_labeled_table}"


with open("labeled_gaps/ais_gap_events_labeled.sql.j2") as f:
    sql_template = Template(f.read())

q = sql_template.render(
    GAPS_FEATURES=GAPS_FEATURES,
)

# Save the table to BigQuery
client = bigquery.Client()
job_config = bigquery.QueryJobConfig(
    destination=GAPS_LABELED, write_disposition="WRITE_TRUNCATE"
)
query_job = client.query(q, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(GAPS_LABELED))
