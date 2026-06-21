import json
import logging
import os
import time
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf


RAW_DIR = Path(os.getenv("RAW_DIR", "/opt/airflow/data/raw"))
STOCK_SEED_PATH = Path(os.getenv("STOCK_SEED_PATH", "/opt/airflow/data/reference/stock_prices_seed.csv"))
LOGGER = logging.getLogger(__name__)


def _save_market_records(records: list[dict], ticker: str, execution_date: str) -> str:
    out_path = RAW_DIR / f"market_{ticker}_{execution_date}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    return str(out_path)


def _filter_market_records(records: list[dict], execution_date: str) -> list[dict]:
    return sorted(
        [record for record in records if str(record["date"]) <= execution_date],
        key=lambda record: record["date"],
    )


def _build_stooq_url(stooq_symbol: str, start_date: datetime, end_date: datetime, api_key: str = "") -> str:
    """Build the Stooq daily CSV URL in the documented query-string format."""
    url = (
        "https://stooq.com/q/d/l/"
        f"?s={stooq_symbol}"
        f"&d1={start_date:%Y%m%d}"
        f"&d2={end_date:%Y%m%d}"
        "&i=d"
    )
    if api_key:
        url = f"{url}&apikey={api_key}"
    return url


def _build_stooq_quote_url(stooq_symbol: str) -> str:
    """Build a no-key Stooq quote CSV URL for latest available daily OHLCV data."""
    return f"https://stooq.com/q/l/?s={stooq_symbol}&f=sd2t2ohlcv&h&e=csv"


def _parse_stooq_daily_csv(csv_text: str, ticker: str, execution_date: str, allow_latest_after_execution: bool = False) -> list[dict]:
    if not csv_text.strip() or "No data" in csv_text:
        raise RuntimeError(f"Stooq returned no data for {ticker}")
    if "Get your apikey" in csv_text or not csv_text.lstrip().startswith(("Date,", "Symbol,")):
        raise RuntimeError("Stooq returned a non-CSV response; it may be asking for an optional API key")

    df = pd.read_csv(StringIO(csv_text))
    if "Date" not in df.columns:
        raise RuntimeError(f"Stooq payload malformed for {ticker}: columns={list(df.columns)}")

    if "Symbol" in df.columns:
        # Latest quote endpoint uses capitalized OHLCV columns but includes N/D for unknown symbols.
        df = df.rename(columns={"Open": "Open", "Close": "Close", "Volume": "Volume"})

    required_cols = {"Date", "Open", "Close", "Volume"}
    if df.empty or not required_cols.issubset(df.columns):
        raise RuntimeError(f"Stooq payload malformed for {ticker}: columns={list(df.columns)}")

    df = df[df["Date"].astype(str).str.upper() != "N/D"].copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Open"] = pd.to_numeric(df["Open"], errors="coerce")
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
    df = df.dropna(subset=["Date", "Open", "Close", "Volume"])

    all_records = [
        {
            "date": row["Date"],
            "open": float(row["Open"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]),
            "ticker": ticker.upper(),
        }
        for _, row in df.iterrows()
    ]
    records = _filter_market_records(all_records, execution_date)
    if not records and allow_latest_after_execution:
        records = sorted(all_records, key=lambda record: record["date"])[-1:]

    if not records:
        raise RuntimeError(f"Stooq returned no records on or before {execution_date} for {ticker}")
    return records


def _fetch_stooq_daily_with_retry(ticker: str, execution_date: str) -> list[dict]:
    """Fetch daily stock prices from Stooq CSV endpoint without an API key."""
    attempts = int(os.getenv("STOOQ_RETRY_ATTEMPTS", "3"))
    base_sleep = float(os.getenv("STOOQ_RETRY_BASE_SECONDS", "2"))
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "365"))
    end_date = datetime.strptime(execution_date, "%Y-%m-%d")
    start_date = end_date - timedelta(days=max(lookback_days + 14, 45))
    stooq_symbol = os.getenv(f"STOOQ_SYMBOL_{ticker.upper()}", f"{ticker.lower()}.us")
    stooq_api_key = os.getenv("STOOQ_API_KEY", "").strip()

    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            stooq_url = _build_stooq_url(stooq_symbol, start_date, end_date, stooq_api_key)
            LOGGER.info("stooq request start | ticker=%s symbol=%s attempt=%s/%s", ticker, stooq_symbol, attempt, attempts)
            response = requests.get(
                stooq_url,
                timeout=20,
            )
            response.raise_for_status()

            try:
                records = _parse_stooq_daily_csv(response.text, ticker, execution_date)
            except RuntimeError as exc:
                allow_quote_fallback = os.getenv("ALLOW_STOOQ_QUOTE_FALLBACK", "false").lower() == "true"
                if not allow_quote_fallback:
                    raise

                LOGGER.warning("stooq historical CSV unavailable | ticker=%s error=%s", ticker, exc)
                quote_response = requests.get(_build_stooq_quote_url(stooq_symbol), timeout=20)
                quote_response.raise_for_status()
                records = _parse_stooq_daily_csv(
                    quote_response.text,
                    ticker,
                    execution_date,
                    allow_latest_after_execution=True,
                )

            LOGGER.info("stooq request success | ticker=%s rows=%s", ticker, len(records))
            return records
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                sleep_seconds = base_sleep * (2 ** (attempt - 1))
                LOGGER.warning(
                    "stooq request failed | ticker=%s attempt=%s/%s error=%s | retry_in=%.1fs",
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


def _generate_simulated_market_data(ticker: str, execution_date: str) -> list[dict]:
    """Generate deterministic fallback prices so arbitrary tickers do not break demos."""
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "365"))
    end_date = datetime.strptime(execution_date, "%Y-%m-%d")
    start_date = end_date - timedelta(days=max(lookback_days, 30))
    ticker_score = sum(ord(char) for char in ticker.upper())
    base_price = 75 + (ticker_score % 250)
    base_volume = 750_000 + (ticker_score % 40) * 50_000

    records: list[dict] = []
    current_date = start_date
    business_day_index = 0
    while current_date <= end_date:
        if current_date.weekday() < 5:
            drift = business_day_index * 0.18
            seasonal = ((business_day_index % 21) - 10) * 0.35
            open_price = round(base_price + drift + seasonal, 2)
            close_price = round(open_price + (((business_day_index + ticker_score) % 7) - 3) * 0.42, 2)
            volume = int(base_volume + ((business_day_index * 37_000) % 900_000))
            records.append(
                {
                    "date": current_date.strftime("%Y-%m-%d"),
                    "open": open_price,
                    "close": close_price,
                    "volume": volume,
                    "ticker": ticker.upper(),
                }
            )
            business_day_index += 1
        current_date += timedelta(days=1)

    LOGGER.warning("market source used | ticker=%s source=simulated_fallback rows=%s", ticker, len(records))
    return records


def _load_seed_market_data(ticker: str, execution_date: str) -> list[dict]:
    """Load deterministic local stock data when external finance APIs are unavailable."""
    if not STOCK_SEED_PATH.exists():
        LOGGER.warning("Stock seed file not found at %s; using simulated fallback", STOCK_SEED_PATH)
        return _generate_simulated_market_data(ticker, execution_date)

    df = pd.read_csv(STOCK_SEED_PATH)
    required_cols = {"date", "ticker", "open", "close", "volume"}
    if df.empty or not required_cols.issubset(df.columns):
        raise RuntimeError(f"Stock seed file malformed: columns={list(df.columns)}")

    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df[df["ticker"] == ticker.upper()].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["date", "open", "close", "volume"])

    records = _filter_market_records(
        [
            {
                "date": row["date"],
                "open": float(row["open"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
                "ticker": ticker.upper(),
            }
            for _, row in df.iterrows()
        ],
        execution_date,
    )
    if not records:
        LOGGER.warning("No seed stock records on or before %s for %s; using simulated fallback", execution_date, ticker)
        return _generate_simulated_market_data(ticker, execution_date)

    min_seed_rows = int(os.getenv("MIN_SEED_MARKET_ROWS", "30"))
    if len(records) < min_seed_rows:
        LOGGER.warning(
            "Seed stock data is too sparse for %s: rows=%s minimum=%s; using simulated fallback",
            ticker,
            len(records),
            min_seed_rows,
        )
        return _generate_simulated_market_data(ticker, execution_date)

    LOGGER.info("market source used | ticker=%s source=seed_csv rows=%s", ticker, len(records))
    return records


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


def _fetch_alpha_vantage_daily_with_retry(
    ticker: str,
    alpha_vantage_api_key: str,
    outputsize: str = "compact",
) -> list[dict]:
    """Fetch Alpha Vantage daily series with retry/backoff on free-tier rate limits."""
    attempts = int(os.getenv("ALPHA_VANTAGE_RETRY_ATTEMPTS", "5"))
    base_sleep = float(os.getenv("ALPHA_VANTAGE_RETRY_BASE_SECONDS", "15"))

    last_error: Exception | None = None
    current_outputsize = outputsize
    for attempt in range(1, attempts + 1):
        try:
            LOGGER.info("alpha_vantage request start | ticker=%s attempt=%s/%s", ticker, attempt, attempts)
            response = requests.get(
                "https://www.alphavantage.co/query",
                params={
                    "function": "TIME_SERIES_DAILY",
                    "symbol": ticker,
                    "apikey": alpha_vantage_api_key,
                    "outputsize": current_outputsize,
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
                message = str(note or info)
                if (
                    current_outputsize == "full"
                    and "outputsize=full" in message
                    and "premium" in message.lower()
                ):
                    LOGGER.warning(
                        "alpha_vantage full history unavailable on current plan | ticker=%s | falling back to compact",
                        ticker,
                    )
                    current_outputsize = "compact"
                    continue

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
    execution_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        rows = _fetch_stooq_daily_with_retry(ticker=ticker, execution_date=execution_date)
        if rows:
            return True
    except Exception:
        pass

    try:
        sample = _fetch_yfinance_history_with_retry(ticker=ticker, period="5d", auto_adjust=False)
        if not sample.empty:
            return True
    except Exception:
        pass

    if alpha_vantage_api_key:
        try:
            rows = _fetch_alpha_vantage_daily_with_retry(ticker=ticker, alpha_vantage_api_key=alpha_vantage_api_key)
            if rows:
                return True
        except Exception:
            pass

    try:
        rows = _load_seed_market_data(ticker=ticker, execution_date=execution_date)
        return bool(rows)
    except Exception:
        return False


def _resolve_market_history_request() -> tuple[str, str]:
    """Return (yfinance_period, alpha_vantage_outputsize) based on configured lookback."""
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "30"))

    # yfinance period buckets: 60d, 1y, 2y (to safely cover 365-day lookback + market holidays)
    if lookback_days <= 60:
        yf_period = "60d"
    elif lookback_days <= 365:
        yf_period = "2y"
    else:
        yf_period = "5y"

    av_outputsize = "full" if lookback_days > 90 else "compact"
    return yf_period, av_outputsize


def extract_market_data(execution_date: str, ticker: str, alpha_vantage_api_key: str | None = None) -> str:
    """Extract market data and store it as JSON in the raw landing zone."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    yf_period, av_outputsize = _resolve_market_history_request()

    try:
        records = _fetch_stooq_daily_with_retry(ticker=ticker, execution_date=execution_date)
        LOGGER.info("market source used | ticker=%s source=stooq rows=%s", ticker, len(records))
        return _save_market_records(records, ticker, execution_date)
    except Exception as exc:
        LOGGER.warning("Stooq exhausted for ticker=%s, falling back to yfinance | error=%s", ticker, exc)

    # yfinance can be temporarily unavailable (e.g. rate limits), so fall back after Stooq.
    try:
        df = _fetch_yfinance_history_with_retry(ticker=ticker, period=yf_period, auto_adjust=False)
        if not df.empty:
            LOGGER.info(
                "market source used | ticker=%s source=yfinance period=%s rows=%s",
                ticker,
                yf_period,
                len(df),
            )
    except Exception as exc:
        LOGGER.warning("yfinance exhausted for ticker=%s, falling back to Alpha Vantage | error=%s", ticker, exc)
        df = pd.DataFrame()

    if df.empty and alpha_vantage_api_key:
        try:
            rows = _fetch_alpha_vantage_daily_with_retry(
                ticker=ticker,
                alpha_vantage_api_key=alpha_vantage_api_key,
                outputsize=av_outputsize,
            )
            if not rows:
                raise RuntimeError("No market data returned from Alpha Vantage")
            LOGGER.info(
                "market source used | ticker=%s source=alpha_vantage outputsize=%s rows=%s",
                ticker,
                av_outputsize,
                len(rows),
            )
            records = _filter_market_records(rows, execution_date)
            if not records:
                raise RuntimeError("No Alpha Vantage market records available before execution date")
            return _save_market_records(records, ticker, execution_date)
        except Exception as exc:
            LOGGER.warning("Alpha Vantage exhausted for ticker=%s, falling back to seed CSV | error=%s", ticker, exc)

    if df.empty:
        records = _load_seed_market_data(ticker=ticker, execution_date=execution_date)
        return _save_market_records(records, ticker, execution_date)

    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    records = _filter_market_records(
        [
            {
                "date": row["Date"],
                "open": float(row["Open"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"]),
                "ticker": ticker.upper(),
            }
            for _, row in df.iterrows()
        ],
        execution_date,
    )

    if not records:
        records = _load_seed_market_data(ticker=ticker, execution_date=execution_date)

    return _save_market_records(records, ticker, execution_date)


def extract_eonet_data(execution_date: str, start_date: str | None = None, end_date: str | None = None) -> str:
    """
    Extract NASA EONET (Earth Observatory Natural Event Tracker) data.
    
    Fetches natural disaster events from the NASA EONET API.
    No API key required. Data is shared globally (not ticker-specific).
    
    Args:
        execution_date: Date string in format YYYY-MM-DD (for filename consistency)
        start_date: Optional inclusive EONET search start date in YYYY-MM-DD format
        end_date: Optional inclusive EONET search end date in YYYY-MM-DD format
    
    Returns:
        Path to the saved raw EONET JSON file
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    window_start = start_date or os.getenv("ANALYSIS_START_DATE")
    window_end = end_date or execution_date
    if window_start:
        out_path = RAW_DIR / f"eonet_{window_start}_{window_end}.json"
    else:
        out_path = RAW_DIR / f"eonet_{execution_date}.json"
    
    eonet_url = "https://eonet.gsfc.nasa.gov/api/v3/events"
    attempts = int(os.getenv("EONET_RETRY_ATTEMPTS", "3"))
    base_sleep = float(os.getenv("EONET_RETRY_BASE_SECONDS", "5"))
    limit = int(os.getenv("EONET_LIMIT", "2000"))
    params = {
        "status": "all",
        "limit": limit,
    }
    if window_start:
        params["start"] = window_start
    if window_end:
        params["end"] = window_end
    
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            LOGGER.info("eonet request start | attempt=%s/%s url=%s params=%s", attempt, attempts, eonet_url, params)
            response = requests.get(
                eonet_url,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            
            events = payload.get("events", [])
            LOGGER.info("eonet request success | events=%s", len(events))
            
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            
            return str(out_path)
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                sleep_seconds = base_sleep * (2 ** (attempt - 1))
                LOGGER.warning(
                    "eonet request failed | attempt=%s/%s error=%s | retry_in=%.1fs",
                    attempt,
                    attempts,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
    
    if last_error is not None:
        raise last_error
    
    raise RuntimeError("EONET API request failed")
