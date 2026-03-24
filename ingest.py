import pandas as pd
import requests
import io
import yfinance as yf
from google.cloud import storage
from datetime import date, timedelta
import sys
import os
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
US_BD = CustomBusinessDay(calendar=USFederalHolidayCalendar())

# ------------------- EXTRACT S&P 500 SYMBOLS ---------------------------
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    table = pd.read_html(io.StringIO(response.text))[0]
    table['Symbol'] = table['Symbol'].str.replace('.', '-', regex=False)
    return table[['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']]



def is_trading_day(target_date):
    # Generate the range of US business days around the target date
    bday_range = pd.bdate_range(
        start=target_date, 
        end=target_date, 
        freq=US_BD
    )
    return len(bday_range) > 0

# ------------------- FETCH STOCK PRICES FOR A SINGLE DAY ---------------
def fetch_stock_data(symbols, target_date):
    if not is_trading_day(target_date):
        print(f"  {target_date} is a weekend or US holiday, skipping.")
        return None
    # yfinance end date is exclusive, so add 1 day
    next_day = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    target_date_str = target_date.strftime("%Y-%m-%d")

    all_data = []
    for symbol in symbols:
        print(f"Fetching {symbol} for {target_date_str}...")
        hist = yf.Ticker(symbol).history(start=target_date_str, end=next_day)
        if hist.empty:
            print(f"  Skipping {symbol} no data")
            continue
        hist = hist.reset_index()
        hist["symbol"] = symbol
        hist = hist[["symbol", "Date", "Open", "Close", "High", "Low", "Volume"]]
        hist.columns = ["symbol", "date", "open", "close", "high", "low", "volume"]
        hist["date"] = hist["date"].dt.date
        all_data.append(hist)

    if not all_data:
        print(f"No data for {target_date_str}, skipping.")
        return None

    return pd.concat(all_data, ignore_index=True)

# ------------------- UPLOAD TO GCS ------------------------------------
def upload_to_gcs(df, bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    print(f"Uploaded to gs://{bucket_name}/{blob_name}")

# ------------------- MAIN ----------------------------------------------
if __name__ == "__main__":

    # Accept a date argument for backfilling, default to yesterday
    if len(sys.argv) > 1:
        target_date = date.fromisoformat(sys.argv[1])
    else:
        target_date = date.today() - timedelta(days=1)

    print(f"Running pipeline for {target_date}...")

    print("Fetching S&P 500 tickers...")
    tickers_df = get_sp500_tickers()
    symbols = tickers_df['Symbol'].tolist()

    # Only upload metadata once (skip if already exists)
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    if not bucket.blob("metadata/sp500_tickers.parquet").exists():
        print("Uploading ticker metadata...")
        upload_to_gcs(tickers_df, BUCKET_NAME, "metadata/sp500_tickers.parquet")
    else:
        print("Ticker metadata already exists, skipping...")

    print(f"Fetching stock prices for {target_date}...")
    prices_df = fetch_stock_data(symbols, target_date)

    if prices_df is not None:
        date_str = target_date.strftime("%Y-%m-%d")
        upload_to_gcs(prices_df, BUCKET_NAME, f"prices/date={date_str}/raw.parquet")

    print("Done!")