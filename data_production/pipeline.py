# pipeline.py

import pandas as pd
import numpy as np
import json
import scipy
import scipy.interpolate
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from jinja2 import Template

class Pipeline():

    def __init__(self,pipeline_dataset,destination_dataset,output_version,vi_version,start_date,end_date):
        #  Assign parmeters to object
        self.client = bigquery.Client()
        self.pipeline_dataset = pipeline_dataset
        self.destination_dataset = destination_dataset
        self.vi_version = vi_version
        self.output_version = output_version
        self.start_date = start_date
        self.end_date = end_date

    def get_query(self, query, **query_args):
        # Open SQL query with jinja2
        with open(query) as f:
            sql_template = Template(f.read())
        # Format query with jinja2 params
        output_template = sql_template.render(query_args)
        return output_template

    def create_bq_table(self, table, schema = None, partition_field = None):
        """ Create BigQuery table
            Method creates the specified table, including setting the partition field
        """
        # Create BQ table object
        bq_table = bigquery.Table(table, schema)

        # Set partitioning
        if partition_field is not None:
            bq_table.time_partitioning = bigquery.TimePartitioning(field= partition_field)

        # Create table
        self.client.create_table(bq_table, exists_ok=True)

    def run_query(self, query, destination_table, write_disposition, **query_args):
        """ Run query for a given date.
        """
        # Format query
        q = self.get_query(query, query_args)

        config = bigquery.QueryJobConfig(
            destination=destination_table,
            priority="BATCH",
            write_disposition=write_disposition
        )

        job = self.client.query(q, job_config=config)

        if job.error_result:
            err = job.error_result["reason"]
            msg = job.error_result["message"]
            print(f"Query for {destination_table} failed with error: {msg}")
            raise RuntimeError(msg)
        else:
            print(f"Calculating measured reception quality for {self.reception_start} through {self.reception_end}")
            job.result()

        return q
