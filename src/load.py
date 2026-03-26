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


def load_to_postgres(processed_file_path: str) -> str:
    """Idempotent load into dim and fact tables."""
    path = Path(processed_file_path)
    if not path.exists():
        raise FileNotFoundError(f"Processed file not found: {processed_file_path}")

    df = pd.read_csv(path)
    if df.empty:
        return "No rows to load"

    with _get_connection() as conn:
        with conn.cursor() as cur:
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

        conn.commit()

    return f"Loaded {len(df)} rows from {processed_file_path}"
