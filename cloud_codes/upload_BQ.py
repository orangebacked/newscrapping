#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 11:43:19 2019

@author: orangebacked
"""

from google.cloud import bigquery
from google.cloud import bigquery
import datetime


job_config.schema = [
    bigquery.SchemaField('name', 'STRING', mode="REQUIRED"),
    bigquery.SchemaField('post_abbr', 'STRING')
]

def bq_create_table(schema):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('test_dataset')
# Prepares a reference to the table
    table_ref = dataset_ref.table('test_table')
    try:
        bigquery_client.get_table(table_ref)
    except:
            table = bigquery.Table(table_ref, schema=schema)
            table = bigquery_client.create_table(table)
            print('table {} created.'.format(table.table_id))
def export_items_to_bigquery():
        # Instantiates a client
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('test_dataset')
