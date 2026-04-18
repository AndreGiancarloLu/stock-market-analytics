# Stock Market Analytics Pipeline

## Problem Description

This project builds an end-to-end batch data pipeline that ingests daily S&P 500 stock price data, transforms it in a cloud data warehouse, and visualizes it in a dashboard.

The pipeline answers two key questions for investors and analysts:

1. **How has each market sector performed over time?** A time series of cumulative returns by sector (e.g. Technology, Energy, Financials) normalized to a common baseline, allowing fair comparison regardless of individual stock prices.
2. **Which sectors are the riskiest?** A monthly volatility analysis (standard deviation of daily returns) by sector, helping identify which sectors carry the most risk relative to their returns.

Stock price data is sourced from Yahoo Finance via the `yfinance` Python library, covering all ~503 S&P 500 constituents. Sector metadata is sourced programmatically from the Wikipedia S&P 500 constituent list.

---

## Architecture

The pipeline is a daily batch pipeline that runs through the following stages:

Wikipedia (S&P 500 list) + Yahoo Finance (prices) -> ingest.py -> GCS (Data Lake) -> load.py -> BigQuery Raw -> dbt -> BigQuery Mart -> Dashboard

All steps are orchestrated by Apache Airflow running in Docker.

### Technologies Used

- Cloud: Google Cloud Platform (GCP)
- IaC: Terraform
- Data Lake: Google Cloud Storage (GCS)
- Data Warehouse: BigQuery
- Transformations: dbt (Data Build Tool)
- Orchestration: Apache Airflow via Docker
- Dashboard:  Power BI

---

## Project Structure

```
stock-market-analytics/
├── airflow/
│   ├── dags/
│   │   └── stock_pipeline.py
│   ├── plugins/
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .env                          # gitignored, created locally
├── credentials/
│   ├── .gitkeep
│   └── service-account-key.json      # gitignored, created locally
├── data/
│   ├── sector_performance.csv
│   ├── volatility.csv
│   └── stock-market-analytics.pbix
├── dbt/
│   └── stock_market_analytics/
│       ├── macros/
│       │   └── get_daily_return.sql
│       ├── models/
│       │   ├── intermediate/
│       │   │   └── int_stocks_enriched.sql
│       │   ├── marts/
│       │   │   ├── mart_sector_performance.sql
│       │   │   └── mart_stock_volatility.sql
│       │   └── staging/
│       │       ├── sources.yml
│       │       ├── stg_stocks.sql
│       │       └── stg_tickers.sql
│       ├── tests/
│       │   ├── close_test.sql
│       │   ├── high_vs_low_test.sql
│       │   ├── negative_test.sql
│       │   └── open_test.sql
│       ├── dbt_project.yml
│       └── profiles.yml              # gitignored, created locally
├── screenshots/
│   ├── sector_performance.png
│   └── sector_volatility.png
├── terraform/
│   ├── main.tf
│   ├── outputs.tf
│   ├── variables.tf
│   └── terraform.tfvars              # gitignored, created locally
├── .gitignore
├── .python-version
├── bulk_backfill.py
├── ingest.py
├── load.py
├── main.py
├── pyproject.toml
├── uv.lock
└── README.md
```

---

## Data Warehouse Design

### Raw Layer (raw_stock_data dataset)

`sp500_tickers` stores S&P 500 company metadata (symbol, name, sector, sub-industry). This table is small and static so no partitioning is applied.

`stock_prices` stores daily OHLCV prices for all tickers. This table is partitioned by `date` since the pipeline appends one day at a time and dashboard queries always filter by date range. Partitioning means each daily load and each dashboard query only touches the relevant partition rather than scanning the full table. The load script uses partition decorators (`table$YYYYMMDD`) with `WRITE_TRUNCATE` so re-running for the same day overwrites only that partition, making the pipeline idempotent.

### Mart Layer (mart_stock_data dataset)

`mart_sector_performance` stores daily cumulative return by sector. This is partitioned by `date` and clustered by `gics_sector` because the dashboard queries this table filtered by date range and grouped by sector. The clustering makes those sector-level aggregations faster.

`mart_stock_volatility` stores monthly return volatility (standard deviation of daily returns) by sector. This table only has around 132 rows after aggregation so partitioning provides no real benefit here. It is clustered by `gics_sector` to stay consistent with the other mart, and would become more useful if data grows across multiple years.

### dbt Transformation Flow

stg_stocks and stg_tickers (staging views) -> int_stocks_enriched (intermediate table) -> mart_sector_performance and mart_stock_volatility (mart tables)

Staging models rename columns to snake_case and cast types. The intermediate model joins prices with ticker metadata so sector is available on every price row. Mart models aggregate to sector level for the dashboard.

---

## Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv)
- [Terraform](https://developer.hashicorp.com/terraform/install)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- A GCP account with a project created

---

## Reproduction Steps

### 1. Clone the repository

```bash
git clone https://github.com/AndreGiancarloLu/stock-market-analytics.git
cd stock-market-analytics
```

### 2. Install Python dependencies

```bash
uv sync
```

### 3. Authenticate with GCP

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 4. Provision infrastructure with Terraform

Go into the terraform folder and create a `terraform.tfvars` file:

```hcl
project_id = "your-gcp-project-id"
region     = "us-central1"
```

Then run:

```bash
terraform init
terraform apply
```

This creates a GCS bucket, two BigQuery datasets (raw_stock_data and mart_stock_data), and a service account with the necessary permissions.

### 5. Set up the service account key

1. Go to GCP Console -> IAM & Admin -> Service Accounts
2. Click the `stock-pipeline-sa` service account
3. Go to Keys -> Add Key -> JSON
4. Save the downloaded file into the `/credentials` folder as `service-account-key.json`

### 6. Configure dbt

Create `dbt/stock_market_analytics/profiles.yml` with the following content:

```yaml
stock_market_analytics:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-gcp-project-id
      dataset: mart_stock_data
      keyfile: /opt/airflow/credentials/service-account-key.json
      location: us-central1
      threads: 4
  target: dev
```

Note: the keyfile path above uses `/opt/airflow/` which is the path inside the Docker container. 
If you want to verify the dbt connection locally before running through Airflow, temporarily 
change the keyfile path in `profiles.yml` to your local path first:

```yaml
keyfile: C:/path/to/credentials/service-account-key.json  # Windows
keyfile: /path/to/credentials/service-account-key.json    # Mac/Linux
```

Then run:

```bash
cd dbt/stock_market_analytics
dbt debug
```

Remember to change the keyfile path back to `/opt/airflow/credentials/service-account-key.json` 
before running the Airflow pipeline, otherwise dbt will fail inside the container.

### 7. Set up Airflow

```bash
cd airflow
```

Create the `.env` file with your own bucket name:

```bash
# Mac/Linux
cat > .env << EOF
AIRFLOW_UID=$(id -u)
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/credentials/service-account-key.json
GCS_BUCKET_NAME=your-project-id-data-lake
AIRFLOW__CELERY__WORKER_HOSTNAME=airflow-worker@%h
EOF

# Windows PowerShell
[System.IO.File]::WriteAllText("$PWD\.env", "AIRFLOW_UID=50000`nGOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/credentials/service-account-key.json`nGCS_BUCKET_NAME=your-project-id-data-lake`nAIRFLOW__CELERY__WORKER_HOSTNAME=airflow-worker@%h`n", [System.Text.Encoding]::UTF8)
```

Replace `your-project-id-data-lake` with your actual GCS bucket name (it follows the format `your-project-id-data-lake` based on what Terraform created).

Initialize and start Airflow (ensure Docker Desktop is running):

```bash
docker compose up airflow-init
docker compose up -d
```

Open http://localhost:8080 and log in with username `airflow` and password `airflow`.

### 8. Run the backfill

Once the DAG appears in the Airflow UI, trigger the backfill from the Airflow CLI:

```bash
docker compose exec airflow-scheduler airflow dags backfill \
  stock_pipeline \
  --start-date 2025-01-01 \
  --end-date 2025-12-01
```

This runs the full pipeline (ingest -> GCS -> BigQuery -> dbt) for every trading day in the date range.

### 9. Verify data in BigQuery

```sql
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT symbol) AS unique_tickers,
    MIN(date) AS earliest_date,
    MAX(date) AS latest_date
FROM `raw_stock_data.stock_prices`
```

### 10. View the dashboard

The dashboard has two pages and is publicly accessible here: [link](https://app.powerbi.com/view?r=eyJrIjoiOTA4NmQ4MDItZDQ4YS00ODIyLWEwM2ItZDQ4NTdhMmNjYTA2IiwidCI6ImYzNGEzNWJkLWE2NWQtNDYwNS1iMGZhLWQyNTcxZjgzMWY1ZSIsImMiOjEwfQ%3D%3D)

**Page 1 - Sector Performance Over Time**
Shows the cumulative return (%) of each S&P 500 sector from January 2025 onwards. Cumulative return measures how much a sector has grown from the starting point, so a value of 10% means a $100 investment at the start would now be worth $110. Use the sector filter to compare specific sectors.

**Page 2 - Monthly Return Volatility by Sector**
Shows the average monthly volatility (standard deviation of daily returns) for each sector in 2025. Volatility measures how much prices fluctuate day to day, so a higher volatility means more unpredictable price swings and generally indicates higher risk. Information Technology is the most volatile sector while Real Estate is the most stable.

## Notes

- The S&P 500 has 503 ticker symbols rather than 500 because some companies have multiple share classes (e.g. Alphabet has both GOOGL and GOOG). This is expected and all 503 are valid.
- Weekends and market holidays return no data from yfinance. The pipeline handles this by skipping those dates.
- Yahoo Finance rate limits bulk requests. If the backfill fails with a YFRateLimitError, wait a few hours before retrying — the block is temporary. The pipeline uses yf.download() to batch all tickers into a single request per day to minimize this risk.
- The following files are gitignored and must be created locally by each user: `credentials/service-account-key.json`, `terraform/terraform.tfvars`, `airflow/.env`, and `dbt/stock_market_analytics/profiles.yml`.
- The `GCS_BUCKET_NAME` environment variable must be set in `airflow/.env` before running the pipeline. The bucket name follows the format `your-project-id-data-lake` based on what Terraform creates.
