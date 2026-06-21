# Analytical Queries — Disaster & Stock Correlation Data Warehouse

> **Database:** `disaster_dw` (PostgreSQL)
> **Use-case:** LA Wildfires (Dec 2024 – Feb 2025) impact on disaster-sensitive insurance stocks

---

## Database Schema (Kimball Star Schema Model)

The database strictly follows a Kimball dimensional model with two fact tables sharing conformed dimensions.

### Dimension Tables

| Table | Primary Key | Purpose |
|---|---|---|
| `dim_company` | `ticker` | One row per stock ticker: company name and sector |
| `dim_date` | `date_key` | Calendar dimension containing ALL days (including weekends) |
| `dim_city` | `city_id` | 10 major US cities used as proximity reference points |
| `dim_event_category` | `category_id` | NASA EONET category reference (seeded) |

### Fact Tables

#### 1. `fact_stock_daily`
**One row per (trading day x stock ticker).** Tracks financial performance.
```
date_key                      DATE     FK -> dim_date
ticker                        TEXT     FK -> dim_company
stock_close_price             NUMERIC  closing price in USD
stock_volume                  BIGINT   shares traded that day
```
*Note: This table naturally has no rows on weekends and market holidays.*

#### 2. `fact_city_disaster_daily`
**One row per (calendar day x city).** Tracks disaster proximity for EVERY city independently, 365 days a year.
```
date_key                      DATE     FK -> dim_date
city_id                       TEXT     FK -> dim_city
active_disaster_count         INT      distinct Wildfire/Storm events active globally
nearby_disaster_count         INT      events within 100 km of THIS specific city
nearest_disaster_distance_km  NUMERIC  km between the nearest disaster and THIS city
is_nearby_disaster            BOOL     True if distance <= 100km
```

---

## Views — Pre-Built Analytical Joins

### View 1: `vw_daily_disaster_stock_impact`
Flattens the entire star schema into a single wide table for easy querying.
```sql
SELECT
    s.date_key, d.year, d.month, d.day,
    c.ticker, c.company_name, c.sector,
    s.stock_close_price, s.stock_volume,
    city.city_id, city.city_name, city.country AS city_country,
    fcd.active_disaster_count, fcd.nearby_disaster_count, fcd.nearest_disaster_distance_km
FROM fact_stock_daily s
JOIN dim_date d ON d.date_key = s.date_key
JOIN dim_company c ON c.ticker = s.ticker
JOIN fact_city_disaster_daily fcd ON fcd.date_key = s.date_key
JOIN dim_city city ON city.city_id = fcd.city_id;
```

### View 2: `vw_stock_disaster_price_movement`
Adds `previous_close_price` and `pct_close_change` to every row using the `LAG()` window function. The window is strictly partitioned by `(ticker, city_id)` to ensure accurate day-over-day price calculations despite the multi-city grain.

### View 3: `vw_insurance_city_risk_days`
A targeted view for Insurance stocks on days where a tracked city had a disaster within 100km (`nearby_disaster_count > 0`).

### View 4: `vw_la_wildfire_insurance_risk_days`
A laser-focused view for Insurance stocks specifically tracking disasters near Los Angeles (`city_id = 'US-LOS_ANGELES'`) that were within 100km. Thanks to the dual fact table design, this view tracks the LA wildfire perfectly, even on days when a storm in Miami was technically closer to Miami than the wildfire was to LA.

*(Note: Weekends and market holidays like Jan 9 and Jan 20 will still be absent from this view because `fact_stock_daily` has no stock prices for days the market was closed).*

---

## Analytical Queries

> All queries target the `disaster_dw` database. Run from psql or Metabase.

### Query 1 — LA Wildfire Peak: Insurance Stock Reaction During the Worst Days
**Business question:** During Jan 7–24 2025 (the Palisades & Eaton fire peak), how did each insurer's closing price change day-over-day when the LA wildfire was active?
```sql
SELECT
    date_key, ticker, company_name, stock_close_price, pct_close_change,
    active_disaster_count, nearby_disaster_count, nearest_disaster_distance_km
FROM vw_stock_disaster_price_movement
WHERE date_key BETWEEN '2025-01-07' AND '2025-01-24'
  AND sector = 'Insurance'
  AND city_name = 'Los Angeles'
ORDER BY date_key, ticker;
```

### Query 2 — Correlation: Nearby Disaster Count vs. Average Daily Stock Return
**Business question:** Is there a meaningful link between the number of close disasters and how the insurance sector performs that day?
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

### Query 3 — Cumulative Stock Performance: High-Disaster vs. Calm Periods
**Business question:** Do insurance stocks underperform during sustained high-disaster periods (>= 2 events within 100 km) vs. calm periods?
```sql
WITH classified AS (
    SELECT
        date_key, ticker, company_name, stock_close_price, pct_close_change, nearby_disaster_count,
        CASE
            WHEN nearby_disaster_count >= 2 THEN 'High Disaster'
            WHEN nearby_disaster_count = 1  THEN 'Moderate Disaster'
            ELSE                                 'Calm'
        END AS period_type
    FROM vw_stock_disaster_price_movement
    WHERE sector = 'Insurance' AND pct_close_change IS NOT NULL
)
SELECT
    period_type, ticker, company_name,
    COUNT(*)                            AS trading_days,
    ROUND(AVG(pct_close_change), 3)     AS avg_daily_return_pct,
    ROUND(SUM(pct_close_change), 3)     AS cumulative_return_pct,
    ROUND(STDDEV(pct_close_change), 3)  AS volatility
FROM classified
GROUP BY period_type, ticker, company_name
ORDER BY period_type, avg_daily_return_pct;
```

### Query 4 — Top Risk Days Ranking
**Business question:** Which specific days were most dangerous (closest disasters, biggest stock drop)?
```sql
SELECT
    date_key, city_name, nearest_disaster_distance_km, nearby_disaster_count, active_disaster_count,
    ROUND(AVG(stock_close_price), 2)  AS avg_sector_close,
    ROUND(AVG(pct_close_change), 2)   AS avg_sector_pct_change,
    COUNT(DISTINCT ticker)            AS tickers_affected
FROM vw_stock_disaster_price_movement
WHERE sector = 'Insurance'
  AND nearby_disaster_count > 0
  AND pct_close_change IS NOT NULL
GROUP BY
    date_key, city_name, nearest_disaster_distance_km, nearby_disaster_count, active_disaster_count
ORDER BY nearby_disaster_count DESC, avg_sector_pct_change ASC
LIMIT 20;
```

### Query 5 — Monthly Disaster Exposure Summary
**Business question:** How did disaster exposure evolve month-by-month across Dec 2024 – Feb 2025?
```sql
SELECT
    d.year, d.month,
    TO_CHAR(DATE_TRUNC('month', fcd.date_key), 'YYYY-Mon') AS month_label,
    ROUND(AVG(fcd.active_disaster_count), 1)    AS avg_daily_active_disasters,
    ROUND(AVG(fcd.nearby_disaster_count), 2)    AS avg_daily_nearby_disasters,
    ROUND(AVG(s.stock_close_price), 2)          AS avg_close_price
FROM fact_stock_daily s
JOIN dim_date d ON d.date_key = s.date_key
JOIN dim_company c ON c.ticker = s.ticker
JOIN fact_city_disaster_daily fcd ON fcd.date_key = s.date_key
WHERE c.sector = 'Insurance'
GROUP BY d.year, d.month, DATE_TRUNC('month', fcd.date_key)
ORDER BY d.year, d.month;
```

---

## Task Requirement Fulfillment

> **Feladat:** *Az adatmodell legalább egy csillag sémát tartalmazzon (ténytábla + legalább két dimenziótábla)*
> **Feladat:** *Legalább 3 értelmes analitikai lekérdezés (pl. SQL) dokumentálva*

**Requirements met:**
1. **Kimball Star Schema:** We have two fully compliant star schemas. `fact_stock_daily` references `dim_date` and `dim_company`. `fact_city_disaster_daily` references `dim_date` and `dim_city`.
2. **Queries:** 5 documented SQL queries with analytical/business value tied to the LA Wildfire hypothesis.
