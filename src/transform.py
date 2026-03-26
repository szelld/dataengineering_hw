import os
from pathlib import Path

import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", "/opt/airflow/data/processed"))


def _company_metadata(ticker: str) -> tuple[str, str]:
    lookup = {
        "AAPL": ("Apple Inc.", "Technology"),
        "MSFT": ("Microsoft Corporation", "Technology"),
        "GOOGL": ("Alphabet Inc.", "Communication Services"),
    }
    return lookup.get(ticker.upper(), (ticker.upper(), "Unknown"))


def _compute_sentiment_score(text: str, analyzer: SentimentIntensityAnalyzer) -> float:
    if not isinstance(text, str) or not text.strip():
        return 0.0
    return float(analyzer.polarity_scores(text)["compound"])


def build_daily_datasets(market_path: str, news_path: str, ticker: str, execution_date: str) -> dict:
    """Build daily fact dataset and article-level sentiment dataset."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    market_df = pd.read_json(market_path)
    market_df["date"] = pd.to_datetime(market_df["date"], errors="coerce")
    market_df["open"] = pd.to_numeric(market_df["open"], errors="coerce")
    market_df["close"] = pd.to_numeric(market_df["close"], errors="coerce")
    market_df["volume"] = pd.to_numeric(market_df["volume"], errors="coerce")
    market_df = market_df.dropna(subset=["date", "open", "close", "volume"]).copy()

    exec_ts = pd.to_datetime(execution_date)
    market_daily = market_df[market_df["date"] <= exec_ts].sort_values("date").tail(1).copy()
    if market_daily.empty:
        raise RuntimeError("No market data row available for execution date")

    news_df = pd.read_csv(news_path)
    if news_df.empty:
        news_sentiment_df = pd.DataFrame(columns=["date_key", "ticker_id", "title", "sentiment_score"])
        news_daily = pd.DataFrame(
            {
                "date": [exec_ts],
                "news_count": [0],
                "avg_sentiment_score": [0.0],
            }
        )
    else:
        date_column = "published_at" if "published_at" in news_df.columns else "date"
        news_df[date_column] = pd.to_datetime(news_df[date_column], errors="coerce")
        news_df = news_df.dropna(subset=[date_column, "title"]).copy()
        news_df["title"] = news_df["title"].astype(str).str.strip().str.lower()
        news_df["date"] = news_df[date_column].dt.normalize()

        analyzer = SentimentIntensityAnalyzer()
        news_df["sentiment_score"] = news_df["title"].apply(lambda value: _compute_sentiment_score(value, analyzer))
        news_sentiment_df = pd.DataFrame(
            {
                "date_key": news_df["date"].dt.date.astype(str),
                "ticker_id": ticker.upper(),
                "title": news_df["title"],
                "sentiment_score": news_df["sentiment_score"].astype(float),
            }
        )

        news_daily = (
            news_df.groupby("date", as_index=False)
            .agg(news_count=("title", "count"), avg_sentiment_score=("sentiment_score", "mean"))
        )
        if news_daily.empty:
            news_daily = pd.DataFrame(
                {
                    "date": [exec_ts],
                    "news_count": [0],
                    "avg_sentiment_score": [0.0],
                }
            )

    market_daily["merge_date"] = market_daily["date"].dt.normalize()
    news_daily["merge_date"] = pd.to_datetime(news_daily["date"], errors="coerce").dt.normalize()
    merged = pd.merge(market_daily, news_daily, on="merge_date", how="left")
    market_date_col = "date" if "date" in merged.columns else "date_x"

    company_name, sector = _company_metadata(ticker)

    output_df = pd.DataFrame(
        {
            "date_key": merged[market_date_col].dt.date.astype(str),
            "ticker_id": ticker.upper(),
            "open_price": merged["open"].astype(float),
            "close_price": merged["close"].astype(float),
            "volume": merged["volume"].astype("int64"),
            "news_count": merged["news_count"].fillna(0).astype(int),
            "avg_sentiment_score": merged["avg_sentiment_score"].fillna(0.0).astype(float),
            "year": merged[market_date_col].dt.year.astype(int),
            "month": merged[market_date_col].dt.month.astype(int),
            "day": merged[market_date_col].dt.day.astype(int),
            "is_weekend": merged[market_date_col].dt.dayofweek >= 5,
            "company_name": company_name,
            "sector": sector,
        }
    )

    out_path = PROCESSED_DIR / f"fact_market_sentiment_{ticker}_{execution_date}.csv"
    news_out_path = PROCESSED_DIR / f"news_sentiment_{ticker}_{execution_date}.csv"
    output_df.to_csv(out_path, index=False)
    news_sentiment_df.to_csv(news_out_path, index=False)

    return {
        "fact_path": str(out_path),
        "news_sentiment_path": str(news_out_path),
    }


def build_daily_dataset(market_path: str, news_path: str, ticker: str, execution_date: str) -> str:
    """Backward-compatible wrapper returning only the fact dataset path."""
    outputs = build_daily_datasets(
        market_path=market_path,
        news_path=news_path,
        ticker=ticker,
        execution_date=execution_date,
    )
    return outputs["fact_path"]
