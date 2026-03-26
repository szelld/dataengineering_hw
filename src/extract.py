import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf


RAW_DIR = Path(os.getenv("RAW_DIR", "/opt/airflow/data/raw"))
LOGGER = logging.getLogger(__name__)


def _fetch_yfinance_history_with_retry(ticker: str, period: str, auto_adjust: bool = False) -> pd.DataFrame:
    """Fetch yfinance history with exponential backoff on transient failures."""
    attempts = int(os.getenv("YFINANCE_RETRY_ATTEMPTS", "3"))
    base_sleep = float(os.getenv("YFINANCE_RETRY_BASE_SECONDS", "2"))

    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            LOGGER.info("yfinance request start | ticker=%s period=%s attempt=%s/%s", ticker, period, attempt, attempts)
            return yf.Ticker(ticker).history(period=period, auto_adjust=auto_adjust)
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                sleep_seconds = base_sleep * (2 ** (attempt - 1))
                LOGGER.warning(
                    "yfinance request failed | ticker=%s attempt=%s/%s error=%s | retry_in=%.1fs",
                    ticker,
                    attempt,
                    attempts,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

    if last_error is not None:
        raise last_error
    return pd.DataFrame()


def _fetch_alpha_vantage_daily_with_retry(ticker: str, alpha_vantage_api_key: str) -> list[dict]:
    """Fetch Alpha Vantage daily series with retry/backoff on free-tier rate limits."""
    attempts = int(os.getenv("ALPHA_VANTAGE_RETRY_ATTEMPTS", "5"))
    base_sleep = float(os.getenv("ALPHA_VANTAGE_RETRY_BASE_SECONDS", "15"))

    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            LOGGER.info("alpha_vantage request start | ticker=%s attempt=%s/%s", ticker, attempt, attempts)
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

            if "Time Series (Daily)" in payload and payload["Time Series (Daily)"]:
                LOGGER.info(
                    "alpha_vantage request success | ticker=%s records=%s",
                    ticker,
                    len(payload["Time Series (Daily)"]),
                )
                rows = []
                for dt, values in payload["Time Series (Daily)"].items():
                    rows.append(
                        {
                            "date": dt,
                            "open": float(values["1. open"]),
                            "close": float(values["4. close"]),
                            "volume": int(float(values["5. volume"])),
                            "ticker": ticker,
                        }
                    )
                return rows

            note = payload.get("Note")
            error_message = payload.get("Error Message")
            info = payload.get("Information")

            if note or info:
                if attempt < attempts:
                    sleep_seconds = base_sleep * (2 ** (attempt - 1))
                    LOGGER.warning(
                        "alpha_vantage limit/info | ticker=%s attempt=%s/%s message=%s | retry_in=%.1fs",
                        ticker,
                        attempt,
                        attempts,
                        note or info,
                        sleep_seconds,
                    )
                    time.sleep(sleep_seconds)
                    continue
                last_error = RuntimeError(f"Alpha Vantage limit/info for {ticker}: {note or info}")
                break

            last_error = RuntimeError(f"Alpha Vantage returned no daily series for {ticker}: {error_message or payload}")
            break
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                sleep_seconds = base_sleep * (2 ** (attempt - 1))
                LOGGER.warning(
                    "alpha_vantage request failed | ticker=%s attempt=%s/%s error=%s | retry_in=%.1fs",
                    ticker,
                    attempt,
                    attempts,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

    if last_error is not None:
        raise last_error
    return []


def check_market_api(ticker: str, alpha_vantage_api_key: str | None = None) -> bool:
    """Return True if at least one market data source is reachable."""
    try:
        sample = _fetch_yfinance_history_with_retry(ticker=ticker, period="5d", auto_adjust=False)
        if not sample.empty:
            return True
    except Exception:
        pass

    if not alpha_vantage_api_key:
        return False

    try:
        rows = _fetch_alpha_vantage_daily_with_retry(ticker=ticker, alpha_vantage_api_key=alpha_vantage_api_key)
        return bool(rows)
    except Exception:
        return False


def extract_market_data(execution_date: str, ticker: str, alpha_vantage_api_key: str | None = None) -> str:
    """Extract market data and store it as JSON in the raw landing zone."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # yfinance can be temporarily unavailable (e.g. rate limits), so fall back to Alpha Vantage.
    try:
        df = _fetch_yfinance_history_with_retry(ticker=ticker, period="60d", auto_adjust=False)
        if not df.empty:
            LOGGER.info("market source used | ticker=%s source=yfinance rows=%s", ticker, len(df))
    except Exception:
        LOGGER.warning("yfinance exhausted for ticker=%s, falling back to Alpha Vantage", ticker)
        df = pd.DataFrame()

    if df.empty and alpha_vantage_api_key:
        rows = _fetch_alpha_vantage_daily_with_retry(ticker=ticker, alpha_vantage_api_key=alpha_vantage_api_key)
        if not rows:
            raise RuntimeError("No market data returned from Alpha Vantage")
        LOGGER.info("market source used | ticker=%s source=alpha_vantage rows=%s", ticker, len(rows))
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
