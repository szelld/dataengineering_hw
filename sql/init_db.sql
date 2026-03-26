-- Database bootstrap for Airflow metadata, data warehouse, and Metabase metadata.

CREATE DATABASE airflow;
CREATE DATABASE market_dw;
CREATE DATABASE metabase;

\connect market_dw;

CREATE TABLE IF NOT EXISTS dim_company (
    ticker_id TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    sector TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_market_sentiment (
    date_key DATE NOT NULL,
    ticker_id TEXT NOT NULL,
    open_price NUMERIC(14, 4),
    close_price NUMERIC(14, 4),
    volume BIGINT,
    news_count INTEGER,
    avg_sentiment_score NUMERIC(6, 4),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    CONSTRAINT fact_market_sentiment_pk PRIMARY KEY (date_key, ticker_id),
    CONSTRAINT fact_market_sentiment_date_fk FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fact_market_sentiment_company_fk FOREIGN KEY (ticker_id) REFERENCES dim_company(ticker_id)
);
