# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

import json
import os
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


keyfile = os.environ.get("KEYFILE_PATH")
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "data-en-bootcamp"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

dataset_name = "users"
is_partition_require = True

datasetList = [
    {
        "dataset_name": "addresses",
        "is_partition_require": False,
        "parse_dates": None
    },
    {
        "dataset_name": "events",
        "is_partition_require": True,
        "parse_dates": ["created_at"]
    },
    {
        "dataset_name": "order_items",
        "is_partition_require": False,
        "parse_dates": None
    },
    {
        "dataset_name": "orders",
        "is_partition_require": True,
        "parse_dates": ["created_at", "estimated_delivery_at", "delivered_at"]
    },
    {
        "dataset_name": "products",
        "is_partition_require": False,
        "parse_dates": None
    },
    {
        "dataset_name": "promos",
        "is_partition_require": False,
        "parse_dates": None
    },
    {
        "dataset_name": "users",
        "is_partition_require": True,
        "parse_dates": ["created_at", "updated_at"]
    }
]

for dataset in datasetList:
    print(f"- start upload for dataset name : {dataset['dataset_name']}")
    if (dataset['is_partition_require'] == True):
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at"
            )
        )
        file_path = f"{dataset['dataset_name']}.csv"
        df = pd.read_csv(file_path, parse_dates=dataset['parse_dates'])
        # df = pd.read_csv(file_path)
        df.info()
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        file_path = f"{dataset['dataset_name']}.csv"
        # df = pd.read_csv(file_path, parse_dates=["created_at"])
        df = pd.read_csv(file_path)
        df.info()

    table_id = f"{project_id}.deb_bootcamp.{dataset['dataset_name']}"
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"- end upload for dataset name : {dataset['dataset_name']}")
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")