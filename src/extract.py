import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf


RAW_DIR = Path(os.getenv("RAW_DIR", "/opt/airflow/data/raw"))


def check_market_api(ticker: str, alpha_vantage_api_key: str | None = None) -> bool:
    """Return True if at least one market data source is reachable."""
    try:
        sample = yf.Ticker(ticker).history(period="5d")
        if not sample.empty:
            return True
    except Exception:
        pass

    if not alpha_vantage_api_key:
        return False

    try:
        response = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "TIME_SERIES_DAILY",
                "symbol": ticker,
                "apikey": alpha_vantage_api_key,
                "outputsize": "compact",
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        return "Time Series (Daily)" in payload
    except Exception:
        return False


def extract_market_data(execution_date: str, ticker: str, alpha_vantage_api_key: str | None = None) -> str:
    """Extract market data and store it as JSON in the raw landing zone."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    df = yf.Ticker(ticker).history(period="60d", auto_adjust=False)
    if df.empty and alpha_vantage_api_key:
        response = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "TIME_SERIES_DAILY",
                "symbol": ticker,
                "apikey": alpha_vantage_api_key,
                "outputsize": "compact",
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json().get("Time Series (Daily)", {})
        rows = []
        for dt, values in payload.items():
            rows.append(
                {
                    "date": dt,
                    "open": float(values["1. open"]),
                    "close": float(values["4. close"]),
                    "volume": int(float(values["5. volume"])),
                    "ticker": ticker,
                }
            )
        if not rows:
            raise RuntimeError("No market data returned from Alpha Vantage")
        out_path = RAW_DIR / f"market_{ticker}_{execution_date}.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2)
        return str(out_path)

    if df.empty:
        raise RuntimeError("No market data returned from yfinance")

    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    records = [
        {
            "date": row["Date"],
            "open": float(row["Open"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]),
            "ticker": ticker,
        }
        for _, row in df.iterrows()
        if row["Date"] <= execution_date
    ]

    if not records:
        raise RuntimeError("No market records available before execution date")

    out_path = RAW_DIR / f"market_{ticker}_{execution_date}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    return str(out_path)


def extract_news_data(execution_date: str, source_csv_path: str, ticker: str) -> str:
    """Extract daily news rows from a static CSV as a simulated daily feed."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    source = Path(source_csv_path)
    if not source.exists():
        raise FileNotFoundError(f"News source file not found: {source_csv_path}")

    news_df = pd.read_csv(source)

    date_column = "published_at" if "published_at" in news_df.columns else "date"
    if date_column not in news_df.columns:
        raise ValueError("News CSV must contain either 'published_at' or 'date' column")

    if "title" not in news_df.columns:
        raise ValueError("News CSV must contain a 'title' column")

    news_df[date_column] = pd.to_datetime(news_df[date_column], errors="coerce")
    run_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    filtered = news_df[news_df[date_column].dt.date == run_date].copy()

    if "ticker" in filtered.columns:
        filtered = filtered[filtered["ticker"].fillna("").str.upper().eq(ticker.upper()) | filtered["ticker"].isna()]
    else:
        filtered["ticker"] = ticker.upper()

    filtered[date_column] = filtered[date_column].dt.strftime("%Y-%m-%d")

    out_path = RAW_DIR / f"news_{ticker}_{execution_date}.csv"
    filtered.to_csv(out_path, index=False)
    return str(out_path)
