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
import time
from yfinance.exceptions import YFRateLimitError
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
    target_date_str = target_date.strftime("%Y-%m-%d")
    next_day = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Downloading all tickers for {target_date_str}...")

    for attempt in range(3):  # retry up to 3 times
        try:
            data = yf.download(
                tickers=symbols,
                start=target_date_str,
                end=next_day,
                group_by='ticker',
                auto_adjust=True,
                progress=False
            )
            break
        except YFRateLimitError:
            wait = (attempt + 1) * 60  # wait 60s, 120s, 180s
            print(f"Rate limited. Waiting {wait} seconds before retry...")
            time.sleep(wait)
    else:
        print(f"Failed after 3 attempts for {target_date_str}, skipping.")
        return None

    if data.empty:
        print(f"No data for {target_date_str} — likely a weekend or holiday.")
        return None

    # Reshape from wide format to long format
    all_data = []
    for symbol in symbols:
        try:
            ticker_data = data[symbol].dropna()
            if ticker_data.empty:
                continue
            ticker_data = ticker_data.reset_index()
            ticker_data["symbol"] = symbol
            ticker_data = ticker_data[["symbol", "Date", "Open", "Close", "High", "Low", "Volume"]]
            ticker_data.columns = ["symbol", "date", "open", "close", "high", "low", "volume"]
            ticker_data["date"] = ticker_data["date"].dt.date
            all_data.append(ticker_data)
        except KeyError:
            continue

    if not all_data:
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