import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def _get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        dbname=os.getenv("WAREHOUSE_DB", "market_dw"),
    )


def _infer_news_sentiment_path(processed_file_path: str) -> Path | None:
    path = Path(processed_file_path)
    prefix = "fact_market_sentiment_"
    if not path.name.startswith(prefix):
        return None
    return path.with_name(path.name.replace(prefix, "news_sentiment_", 1))


def load_to_postgres(processed_file_path: str, news_sentiment_file_path: str | None = None) -> str:
    """Idempotent load into dim and fact tables."""
    path = Path(processed_file_path)
    if not path.exists():
        raise FileNotFoundError(f"Processed file not found: {processed_file_path}")

    df = pd.read_csv(path)
    if df.empty:
        return "No rows to load"

    inferred_news_path = _infer_news_sentiment_path(processed_file_path)
    news_path = Path(news_sentiment_file_path) if news_sentiment_file_path else inferred_news_path
    news_df = None
    if news_path and news_path.exists():
        news_df = pd.read_csv(news_path)

    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS fact_news_sentiment (
                    date_key DATE NOT NULL,
                    ticker_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    sentiment_score NUMERIC(6, 4),
                    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                    CONSTRAINT fact_news_sentiment_pk PRIMARY KEY (date_key, ticker_id, title),
                    CONSTRAINT fact_news_sentiment_date_fk FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                    CONSTRAINT fact_news_sentiment_company_fk FOREIGN KEY (ticker_id) REFERENCES dim_company(ticker_id)
                )
                """
            )

            company_rows = sorted(
                {
                    (row["ticker_id"], row["company_name"], row["sector"])
                    for _, row in df.iterrows()
                }
            )
            execute_values(
                cur,
                """
                INSERT INTO dim_company (ticker_id, company_name, sector)
                VALUES %s
                ON CONFLICT (ticker_id) DO UPDATE
                SET company_name = EXCLUDED.company_name,
                    sector = EXCLUDED.sector
                """,
                company_rows,
            )

            date_rows = sorted(
                {
                    (
                        row["date_key"],
                        int(row["year"]),
                        int(row["month"]),
                        int(row["day"]),
                        bool(row["is_weekend"]),
                    )
                    for _, row in df.iterrows()
                }
            )
            execute_values(
                cur,
                """
                INSERT INTO dim_date (date_key, year, month, day, is_weekend)
                VALUES %s
                ON CONFLICT (date_key) DO UPDATE
                SET year = EXCLUDED.year,
                    month = EXCLUDED.month,
                    day = EXCLUDED.day,
                    is_weekend = EXCLUDED.is_weekend
                """,
                date_rows,
            )

            fact_rows = [
                (
                    row["date_key"],
                    row["ticker_id"],
                    float(row["open_price"]),
                    float(row["close_price"]),
                    int(row["volume"]),
                    int(row["news_count"]),
                    float(row["avg_sentiment_score"]),
                )
                for _, row in df.iterrows()
            ]
            execute_values(
                cur,
                """
                INSERT INTO fact_market_sentiment (
                    date_key,
                    ticker_id,
                    open_price,
                    close_price,
                    volume,
                    news_count,
                    avg_sentiment_score
                )
                VALUES %s
                ON CONFLICT (date_key, ticker_id) DO UPDATE
                SET open_price = EXCLUDED.open_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    news_count = EXCLUDED.news_count,
                    avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                    updated_at = NOW()
                """,
                fact_rows,
            )

            if news_df is not None and not news_df.empty:
                news_rows = [
                    (
                        row["date_key"],
                        row["ticker_id"],
                        str(row["title"]),
                        float(row["sentiment_score"]),
                    )
                    for _, row in news_df.iterrows()
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO fact_news_sentiment (
                        date_key,
                        ticker_id,
                        title,
                        sentiment_score
                    )
                    VALUES %s
                    ON CONFLICT (date_key, ticker_id, title) DO UPDATE
                    SET sentiment_score = EXCLUDED.sentiment_score,
                        updated_at = NOW()
                    """,
                    news_rows,
                )

        conn.commit()

    return f"Loaded {len(df)} rows from {processed_file_path}"
