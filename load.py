from google.cloud import bigquery
from datetime import date, timedelta
import sys
import os
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
def load_tickers_to_bigquery(bucket_name, dataset_id):
    client = bigquery.Client()
    table_ref = f"{client.project}.{dataset_id}.sp500_tickers"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    uri = f"gs://{bucket_name}/metadata/sp500_tickers.parquet"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded tickers into {table_ref}")

def load_prices_to_bigquery(bucket_name, dataset_id, target_date):
    client = bigquery.Client()
    date_str = target_date.strftime("%Y-%m-%d")
    
    table_ref = f"{client.project}.{dataset_id}.stock_prices${date_str.replace('-', '')}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        ),
    )

    uri = f"gs://{bucket_name}/prices/date={date_str}/raw.parquet"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded prices for {date_str} into {table_ref}")

if __name__ == "__main__":

    if len(sys.argv) > 1:
        target_date = date.fromisoformat(sys.argv[1])
    else:
        target_date = date.today() - timedelta(days=1)

    load_tickers_to_bigquery(BUCKET_NAME, "raw_stock_data")
    load_prices_to_bigquery(BUCKET_NAME, "raw_stock_data", target_date)

    print("Done!")