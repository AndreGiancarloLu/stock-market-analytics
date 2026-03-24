import sys
sys.path.insert(0, '/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from ingest import get_sp500_tickers, fetch_stock_data, upload_to_gcs
from load import load_tickers_to_bigquery, load_prices_to_bigquery
from datetime import date

BUCKET_NAME = "terraform-demo-484209-data-lake"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_task(**context):
    execution_date = context['logical_date'].date()

    tickers_df = get_sp500_tickers()
    symbols = tickers_df['Symbol'].tolist()

    from google.cloud import storage
    import io

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    if not bucket.blob("metadata/sp500_tickers.parquet").exists():
        upload_to_gcs(tickers_df, BUCKET_NAME, "metadata/sp500_tickers.parquet")

    prices_df = fetch_stock_data(symbols, execution_date)

    if prices_df is not None:
        date_str = execution_date.strftime("%Y-%m-%d")
        upload_to_gcs(prices_df, BUCKET_NAME, f"prices/date={date_str}/raw.parquet")

def load_task(**context):
    execution_date = context['logical_date'].date()
    load_tickers_to_bigquery(BUCKET_NAME, "raw_stock_data")
    load_prices_to_bigquery(BUCKET_NAME, "raw_stock_data", execution_date)

with DAG(
    dag_id="stock_pipeline",
    default_args=default_args,
    description="Daily S&P 500 stock data pipeline",
    schedule_interval="0 18 * * 1-5",  # 6pm UTC Mon-Fri (after US market close)
    start_date=datetime(2025, 1, 1),
    catchup=True, 
    tags=["stocks"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_to_gcs",
        python_callable=ingest_task,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_task,
        provide_context=True,
    )

    transform = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/airflow/dbt/stock_market_analytics && dbt build --profiles-dir /opt/airflow/dbt",
    )

    ingest >> load >> transform