# Analytical Queries — Disaster & Stock Correlation Data Warehouse

> **Database:** `disaster_dw` (PostgreSQL)  
> **Use-case:** LA Wildfires (Jan–Mar 2025) impact on disaster-sensitive insurance stocks

---

## Database Schema

### Dimension Tables

| Table | Primary Key | Purpose |
|---|---|---|
| `dim_company` | `ticker` | One row per stock ticker — stores company name and sector |
| `dim_date` | `date_key` | Calendar dimension — year, month, day, is_weekend flag |
| `dim_city` | `city_id` | Major US cities used as proximity reference points for disaster exposure |
| `dim_event_category` | `category_id` | NASA EONET event categories (Wildfires, Severe Storms, Floods, …) |

#### `dim_company`
```
ticker       TEXT  PK   e.g. "ALL", "CB", "TRV", "AIG", "PGR", "BRK-B"
company_name TEXT       e.g. "Allstate Corporation"
sector       TEXT       e.g. "Insurance", "Construction", "Energy"
```

#### `dim_date`
```
date_key   DATE  PK    e.g. 2025-01-08
year       INT         2025
month      INT         1
day        INT         8
is_weekend BOOL        false
```

#### `dim_city`
```
city_id    TEXT  PK    e.g. "US-LOS_ANGELES"
city_name  TEXT        "Los Angeles"
country    TEXT        "United States"
latitude   NUMERIC
longitude  NUMERIC
```

#### `dim_event_category`
```
category_id   INT  PK   NASA EONET numeric category ID
category_name TEXT       "Wildfires", "Severe Storms", "Floods", ...
```

---

### Fact Table: `fact_daily_impact`

One row per **(trading day x stock ticker)**. Joins each day's stock price with the disaster intensity measured around the major US cities on that same day.

```
date_key                      DATE     FK -> dim_date        trading date
ticker                        TEXT     FK -> dim_company      stock ticker
stock_close_price             NUMERIC  closing price (USD)
stock_volume                  BIGINT   shares traded
active_disaster_count         INT      # active wildfire/storm events globally that day
nearby_disaster_count         INT      # events within 100 km of any tracked city
nearest_city_id               TEXT     FK -> dim_city         city closest to any disaster
nearest_city_name             TEXT     denormalized copy of city name
nearest_disaster_distance_km  NUMERIC  km between nearest disaster and nearest city
updated_at                    TIMESTAMP  last pipeline write time
```

> **Composite PK:** `(date_key, ticker)` — one fact row per day per stock.

---

### Views (pre-built joins in `init_db.sql`)

| View | Built on | Adds |
|---|---|---|
| `vw_daily_disaster_stock_impact` | `fact_daily_impact` + all dims | Full joined view with company name, sector, city details |
| `vw_stock_disaster_price_movement` | `vw_daily_disaster_stock_impact` | `previous_close_price` + `pct_close_change` (day-over-day % via `LAG`) |
| `vw_insurance_city_risk_days` | `vw_daily_disaster_stock_impact` | Pre-filtered: Insurance sector rows where a nearby disaster was active |

---

## Analytical Queries

> All queries target the `disaster_dw` database. Run from psql or Metabase.

---

### Query 1 — LA Wildfire Peak: Insurance Stock Reaction During the Worst Days

**Business question:** During the days when wildfires were closest to Los Angeles (Jan 7–14 2025, the Palisades & Eaton fire peak), how did each insurance stock's close price change day-over-day?

```sql
SELECT
    date_key,
    ticker,
    company_name,
    stock_close_price,
    pct_close_change,
    active_disaster_count,
    nearby_disaster_count,
    nearest_city_name,
    nearest_disaster_distance_km
FROM vw_stock_disaster_price_movement
WHERE date_key BETWEEN '2025-01-07' AND '2025-01-24'
  AND sector = 'Insurance'
ORDER BY date_key, ticker;
```

**What it shows:** The direct stock-price reaction of all 6 tracked insurers (Allstate, Chubb, Travelers, AIG, Progressive, Berkshire) during and immediately after the LA wildfire outbreak. A negative `pct_close_change` on high-disaster days confirms the disaster -> stock impact hypothesis.

---

### Query 2 — Correlation: Nearby Disaster Count vs. Average Daily Stock Return (by Sector)

**Business question:** Is there a meaningful link between the number of disasters near major cities and how the sector performs on average that day?

```sql
SELECT
    sector,
    nearby_disaster_count,
    COUNT(*)                          AS trading_days,
    ROUND(AVG(pct_close_change), 3)   AS avg_pct_change,
    ROUND(MIN(pct_close_change), 3)   AS worst_day_pct,
    ROUND(MAX(pct_close_change), 3)   AS best_day_pct
FROM vw_stock_disaster_price_movement
WHERE pct_close_change IS NOT NULL
GROUP BY sector, nearby_disaster_count
ORDER BY sector, nearby_disaster_count;
```

**What it shows:** Groups every trading day by how many disasters were within 100 km of a major city. If the insurance sector average return drops as `nearby_disaster_count` rises, that is the core pipeline hypothesis confirmed in a single query.

---

### Query 3 — Cumulative Stock Performance: High-Disaster vs. Calm Periods

**Business question:** Do insurance stocks underperform during sustained high-disaster periods (>= 2 nearby events) compared to calm periods?

```sql
WITH classified AS (
    SELECT
        date_key,
        ticker,
        company_name,
        stock_close_price,
        pct_close_change,
        active_disaster_count,
        nearby_disaster_count,
        CASE
            WHEN nearby_disaster_count >= 2 THEN 'High Disaster'
            WHEN nearby_disaster_count = 1  THEN 'Moderate Disaster'
            ELSE                                 'Calm'
        END AS period_type
    FROM vw_stock_disaster_price_movement
    WHERE sector = 'Insurance'
      AND pct_close_change IS NOT NULL
)
SELECT
    period_type,
    ticker,
    company_name,
    COUNT(*)                            AS trading_days,
    ROUND(AVG(pct_close_change), 3)     AS avg_daily_return_pct,
    ROUND(SUM(pct_close_change), 3)     AS cumulative_return_pct,
    ROUND(STDDEV(pct_close_change), 3)  AS volatility
FROM classified
GROUP BY period_type, ticker, company_name
ORDER BY period_type, avg_daily_return_pct;
```

**What it shows:** Compares average and cumulative returns across three disaster regimes. Higher volatility and lower average return in the "High Disaster" bucket confirms disaster proximity as a risk factor for insurer stocks.

---

### Query 4 — Top Risk Days Ranking (City Exposure Leaderboard)

**Business question:** Which calendar days were the most dangerous for insurers based on combined disaster proximity and stock price drop?

```sql
SELECT
    date_key,
    nearest_city_name,
    nearest_disaster_distance_km,
    nearby_disaster_count,
    active_disaster_count,
    ROUND(AVG(stock_close_price), 2)  AS avg_sector_close,
    ROUND(AVG(pct_close_change), 2)   AS avg_sector_pct_change,
    COUNT(DISTINCT ticker)            AS tickers_affected
FROM vw_stock_disaster_price_movement
WHERE sector = 'Insurance'
  AND nearby_disaster_count > 0
  AND pct_close_change IS NOT NULL
GROUP BY
    date_key,
    nearest_city_name,
    nearest_disaster_distance_km,
    nearby_disaster_count,
    active_disaster_count
ORDER BY nearby_disaster_count DESC, avg_sector_pct_change ASC
LIMIT 20;
```

**What it shows:** Ranked list of the top 20 highest-risk trading days, ranked by disaster proximity + severity, with the sector's average stock reaction. Jan 8–10 2025 should appear at the top, confirming the LA wildfire event as the dominant risk signal in the dataset.

---

### Query 5 — Monthly Disaster Exposure Summary

**Business question:** How did disaster exposure and insurance stock performance evolve month-by-month across the Dec 2024–Mar 2025 window?

```sql
SELECT
    d.year,
    d.month,
    TO_CHAR(DATE_TRUNC('month', f.date_key), 'YYYY-Mon') AS month_label,
    ROUND(AVG(f.active_disaster_count), 1)    AS avg_daily_active_disasters,
    ROUND(AVG(f.nearby_disaster_count), 2)    AS avg_daily_nearby_disasters,
    ROUND(AVG(f.stock_close_price), 2)        AS avg_close_price,
    ROUND(AVG(m.pct_close_change), 3)         AS avg_daily_return_pct
FROM fact_daily_impact f
JOIN dim_date d ON d.date_key = f.date_key
JOIN dim_company c ON c.ticker = f.ticker
JOIN vw_stock_disaster_price_movement m
    ON m.date_key = f.date_key AND m.ticker = f.ticker
WHERE c.sector = 'Insurance'
GROUP BY d.year, d.month, DATE_TRUNC('month', f.date_key)
ORDER BY d.year, d.month;
```

**What it shows:** Month-by-month aggregation — January 2025 should show the peak disaster count (LA wildfires) and whether that coincided with the worst average daily return for the insurance sector.

---

## Task Requirement Fulfillment

> **Feladat:** *Legalább 3 értelmes analitikai lekérdezés (pl. SQL) dokumentálva*
> ("At least 3 meaningful analytical queries (e.g. SQL) documented")

| # | Query | Analytical Value |
|---|---|---|
| 1 | **LA Wildfire Peak Reaction** | Stock price changes during the Jan 2025 wildfire peak — the core use-case event |
| 2 | **Nearby Disaster Count vs. Sector Return** | Tests the pipeline hypothesis: more nearby disasters -> lower insurance returns |
| 3 | **High vs. Calm Period Performance** | Classifies every trading day into disaster regimes, compares cumulative returns |
| 4 | **Top Risk Days Leaderboard** | Ranks the most dangerous trading days by disaster proximity + stock drop |
| 5 | **Monthly Disaster Exposure Summary** | Longitudinal view of the full Dec 2024 - Mar 2025 window |

**The requirement is fully met** — 5 documented SQL queries, each with a clear business question and explanation, directly tied to the LA wildfire use-case and the disaster/stock correlation hypothesis.
