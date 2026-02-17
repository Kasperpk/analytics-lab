import yfinance as yf
import polars as pl
from datetime import datetime
from pathlib import Path

RAW_PATH = Path("data/raw")
RAW_PATH.mkdir(parents=True, exist_ok=True)

TICKERS = ["AAPL", "MSFT", "^GSPC", "^IXIC"]

def normalize_columns(columns):
    clean = []
    for col in columns:
        if isinstance(col, tuple):
            clean.append(col[0].lower())
        else:
            clean.append(str(col).lower())
    return clean

def fetch_prices():
    all_data = []

    for ticker in TICKERS:
        df = yf.download(ticker, period="1y", interval="1d", progress=False)

        df = df.reset_index()

        # 🔥 Robust column normalization
        df.columns = normalize_columns(df.columns)

        df["ticker"] = ticker

        all_data.append(pl.from_pandas(df))

    combined = pl.concat(all_data)

    combined = combined.with_columns(
        pl.lit(datetime.utcnow()).alias("ingested_at")
    )

    output_file = RAW_PATH / f"finance_prices_{datetime.utcnow().date()}.parquet"
    combined.write_parquet(output_file)

    print(f"Saved finance data to {output_file}")

if __name__ == "__main__":
    fetch_prices()

