# Financial Data Engineering Pipeline

## Project Goal
This project implements a containerized, production-style data engineering pipeline that ingests daily stock market data and daily financial news, computes sentiment signals, and loads curated facts into a PostgreSQL data warehouse using Apache Airflow orchestration.

## Tech Stack
- Host: Windows 10/11 (recommended with Docker Desktop + WSL2 backend)
- Orchestration: Apache Airflow 2.9 (Docker)
- Data Warehouse: PostgreSQL 15 (Docker)
- BI / Data Serving: Metabase (Docker)
- Language: Python 3.10+
- Processing: pandas, requests, yfinance, vaderSentiment, psycopg2

## Architecture Rationale
- PostgreSQL is used as the warehouse because it is robust, SQL-native, easy to containerize, and supports upsert semantics (`ON CONFLICT`) for idempotent loads.
- Airflow is used because it provides explicit DAG-based orchestration, retries, scheduling, observability, and production-ready task dependency management.
- Docker Compose is used to make environment setup reproducible across machines and to align Windows host development with Linux container runtime.

## Repository Layout
```text
financial_de_pipeline/
├── docker-compose.yml
├── .env
├── requirements.txt
├── README.md
├── dags/
│   └── pipeline_dag.py
├── data/
│   ├── raw/
│   │   └── news_feed.csv
│   └── processed/
├── sql/
│   └── init_db.sql
└── src/
    ├── extract.py
    ├── transform.py
    └── load.py
```

## Data Model (Star Schema)
### `dim_company`
- `ticker_id` (PK)
- `company_name`
- `sector`

### `dim_date`
- `date_key` (PK)
- `year`
- `month`
- `day`
- `is_weekend`

### `fact_market_sentiment`
- `date_key` (FK -> `dim_date.date_key`)
- `ticker_id` (FK -> `dim_company.ticker_id`)
- `open_price`
- `close_price`
- `volume`
- `news_count`
- `avg_sentiment_score`

Primary key on fact table: (`date_key`, `ticker_id`).

## Prerequisites
- Docker Desktop installed and running
- Docker Compose enabled
- Alpha Vantage API key (optional if yfinance is used as primary source)

## Configuration
Update `.env` before first run:
```env
ALPHA_VANTAGE_API_KEY=replace_with_real_key
STOCK_TICKER=AAPL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
AIRFLOW_DB=airflow
WAREHOUSE_DB=market_dw
METABASE_DB=metabase
```

## Run Instructions
From the `financial_de_pipeline` directory:

```bash
docker-compose up -d
```

Then open:
- Airflow UI: http://localhost:8080 (admin / admin)
- Metabase UI: http://localhost:3000

Enable DAG: `financial_market_sentiment_pipeline` and trigger it manually or wait for the daily schedule.

## DAG Tasks
- `task_check_api`: verifies market source availability (yfinance and/or Alpha Vantage)
- `task_extract`: extracts market and daily news data into `data/raw`
- `task_transform`: cleans, computes sentiment, aggregates by day, merges with market data
- `task_load`: idempotent upsert into warehouse dimensions and fact table

## Idempotency Strategy
Loading is implemented with `INSERT ... ON CONFLICT DO UPDATE` for all dimension and fact tables. Re-running the same business date updates existing rows instead of creating duplicates.

## Example Analytical SQL Queries
1. Days with strongest negative sentiment and corresponding stock close:
```sql
SELECT
    f.date_key,
    c.ticker_id,
    f.avg_sentiment_score,
    f.news_count,
    f.close_price
FROM fact_market_sentiment f
JOIN dim_company c ON c.ticker_id = f.ticker_id
WHERE f.news_count > 0
ORDER BY f.avg_sentiment_score ASC, f.news_count DESC
LIMIT 10;
```

2. Monthly average sentiment and average close price trend:
```sql
SELECT
    d.year,
    d.month,
    f.ticker_id,
    ROUND(AVG(f.avg_sentiment_score)::numeric, 4) AS avg_monthly_sentiment,
    ROUND(AVG(f.close_price)::numeric, 2) AS avg_monthly_close
FROM fact_market_sentiment f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.month, f.ticker_id
ORDER BY d.year, d.month;
```

3. Sentiment vs. day-over-day close movement:
```sql
WITH series AS (
    SELECT
        f.date_key,
        f.ticker_id,
        f.close_price,
        f.avg_sentiment_score,
        LAG(f.close_price) OVER (PARTITION BY f.ticker_id ORDER BY f.date_key) AS prev_close
    FROM fact_market_sentiment f
)
SELECT
    date_key,
    ticker_id,
    avg_sentiment_score,
    close_price,
    prev_close,
    ROUND((close_price - prev_close)::numeric, 4) AS close_delta
FROM series
WHERE prev_close IS NOT NULL
ORDER BY date_key DESC;
```

## Notes
- `data/raw/news_feed.csv` is included as a simulated static daily news source.
- You can replace it with a Kaggle-exported CSV as long as it has `published_at` (or `date`) and `title` columns.
