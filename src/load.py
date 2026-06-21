import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from src.transform import _company_metadata


def _get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        dbname=os.getenv("DISASTER_WAREHOUSE_DB", os.getenv("WAREHOUSE_DB", "disaster_dw")),
    )


def load_to_postgres(stock_path: str, city_path: str) -> str:
    """
    Idempotent load into dim and fact tables for NASA disaster correlation pipeline.
    
    Loads data into:
    - dim_date
    - fact_stock_daily
    - fact_city_disaster_daily
    """
    stock_p = Path(stock_path)
    city_p = Path(city_path)
    
    if not stock_p.exists() or not city_p.exists():
        raise FileNotFoundError(f"Processed files not found: {stock_path} or {city_path}")

    stock_df = pd.read_csv(stock_p)
    city_df = pd.read_csv(city_p)

    with _get_connection() as conn:
        with conn.cursor() as cur:
            # Upsert dim_date (from city_df which has ALL dates including weekends)
            if not city_df.empty:
                date_rows = sorted(
                    {
                        (
                            row["date_key"],
                            int(row["year"]),
                            int(row["month"]),
                            int(row["day"]),
                            bool(row["is_weekend"]),
                        )
                        for _, row in city_df.iterrows()
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

            # Upsert dim_company first so fact_stock_daily satisfies its
            # foreign key on a fresh warehouse (company metadata is derived
            # from the tickers present in the stock facts, so arbitrary
            # tickers are supported without manual seeding).
            if not stock_df.empty:
                company_rows = sorted(
                    {
                        (ticker, *_company_metadata(ticker))
                        for ticker in stock_df["ticker"].astype(str).str.upper().unique()
                    }
                )
                execute_values(
                    cur,
                    """
                    INSERT INTO dim_company (ticker, company_name, sector)
                    VALUES %s
                    ON CONFLICT (ticker) DO UPDATE
                    SET company_name = EXCLUDED.company_name,
                        sector = EXCLUDED.sector
                    """,
                    company_rows,
                )

            # Upsert fact_stock_daily
            if not stock_df.empty:
                stock_rows = [
                    (
                        row["date_key"],
                        row["ticker"],
                        float(row["stock_close_price"]),
                        int(row["stock_volume"])
                    )
                    for _, row in stock_df.iterrows()
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO fact_stock_daily (
                        date_key, ticker, stock_close_price, stock_volume
                    )
                    VALUES %s
                    ON CONFLICT (date_key, ticker) DO UPDATE
                    SET stock_close_price = EXCLUDED.stock_close_price,
                        stock_volume = EXCLUDED.stock_volume,
                        updated_at = NOW()
                    """,
                    stock_rows,
                )

            # Upsert fact_city_disaster_daily
            if not city_df.empty:
                city_disaster_rows = [
                    (
                        row["date_key"],
                        row["city_id"],
                        int(row["active_disaster_count"]),
                        int(row["nearby_disaster_count"]),
                        None if pd.isna(row.get("nearest_disaster_distance_km")) else float(row["nearest_disaster_distance_km"]),
                        bool(row["is_nearby_disaster"])
                    )
                    for _, row in city_df.iterrows()
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO fact_city_disaster_daily (
                        date_key, city_id, active_disaster_count, nearby_disaster_count,
                        nearest_disaster_distance_km, is_nearby_disaster
                    )
                    VALUES %s
                    ON CONFLICT (date_key, city_id) DO UPDATE
                    SET active_disaster_count = EXCLUDED.active_disaster_count,
                        nearby_disaster_count = EXCLUDED.nearby_disaster_count,
                        nearest_disaster_distance_km = EXCLUDED.nearest_disaster_distance_km,
                        is_nearby_disaster = EXCLUDED.is_nearby_disaster,
                        updated_at = NOW()
                    """,
                    city_disaster_rows,
                )

        conn.commit()

    return f"Loaded stock ({len(stock_df)}) and city ({len(city_df)}) rows."
