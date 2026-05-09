# Analytical Queries — Disaster & Stock Correlation Data Warehouse

> **Database:** `disaster_dw` (PostgreSQL)
> **Use-case:** LA Wildfires (Jan–Mar 2025) impact on disaster-sensitive insurance stocks

---

## Database Schema

### Dimension Tables

| Table | Primary Key | Purpose |
|---|---|---|
| `dim_company` | `ticker` | One row per stock ticker: company name and sector |
| `dim_date` | `date_key` | Calendar dimension for dates that have fact rows (trading days only) |
| `dim_city` | `city_id` | 10 major US cities used as proximity reference points |
| `dim_event_category` | `category_id` | NASA EONET category reference (seeded, not FK'd to fact) |

---

#### `dim_company`
```
ticker       TEXT  PK   e.g. "ALL", "CB", "TRV", "AIG", "PGR", "BRK-B"
company_name TEXT       e.g. "Allstate Corporation"
sector       TEXT       "Insurance", "Construction", "Energy"
```
Six rows, one per tracked stock. Used by the fact table as a foreign key.

---

#### `dim_date`
```
date_key   DATE  PK    e.g. 2025-01-08
year       INT         2025
month      INT         1
day        INT         8
is_weekend BOOL        always false in current data (see note below)
```

> **Important — why weekends are missing:**
> The pipeline's `_business_dates_between()` function intentionally skips Saturday/Sunday:
> ```python
> if current_dt.weekday() < 5:   # 0=Mon .. 4=Fri, skip 5=Sat, 6=Sun
>     dates.append(...)
> ```
> The load step only writes `dim_date` rows for dates that appear in the fact table.
> Because no fact rows are ever created for weekends (stock markets are closed),
> `dim_date` contains **trading days only**. Dec 7-8 and Dec 14-15 2024 are
> Saturday/Sunday and are correctly absent.
>
> The `is_weekend` column exists because this is the standard star-schema date dimension
> pattern — but in this use-case it will always be `False`. It would only matter if
> the pipeline were extended to track weekend events separately.

---

#### `dim_city`
```
city_id    TEXT  PK    e.g. "US-LOS_ANGELES"
city_name  TEXT        "Los Angeles"
country    TEXT        "United States"
latitude   NUMERIC     34.0522
longitude  NUMERIC     -118.2437
```
10 rows, seeded at DB init. These are the US cities with the highest property/casualty
insurance exposure: LA, San Francisco, Sacramento, Denver, Phoenix, Austin, Dallas,
Houston, Miami, New Orleans. They act as **reference points** for the proximity
calculation — see "How Cities Work" below.

---

#### `dim_event_category`
```
category_id   INT  PK   NASA EONET numeric ID
category_name TEXT       e.g. "Wildfires", "Severe Storms"
```
13 rows, seeded at DB init from the NASA EONET v3 taxonomy.

| ID | Name |
|---|---|
| 6 | Drought |
| 7 | Dust and Haze |
| 8 | **Wildfires** |
| 9 | Floods |
| 10 | **Severe Storms** |
| 12 | Snow |
| 13 | Temperature Extremes |
| 14 | Volcanoes |
| 15 | Water Color |
| 16 | Landslides |
| 17 | Sea and Lake Ice |
| 18 | Earthquakes |
| 19 | Manmade |

> **Why no categories 0-5 or 11?**
> NASA EONET v3 never issued these IDs — the numbering simply starts at 6, with 11
> skipped. There are no categories above 19 in the current EONET taxonomy.
>
> **Important:** This table is seeded reference data only. It has **no foreign key
> from the fact table** and the pipeline **never queries it**. Disaster filtering
> happens in Python before data reaches the DB:
> ```python
> relevant_categories = ["Wildfires", "Severe Storms"]  # transform.py line 370
> ```
> `dim_event_category` is documentation-in-the-DB — useful for understanding the
> EONET taxonomy, not for joining in queries.

---

### Fact Table: `fact_daily_impact`

**One row per (trading day x stock ticker).**

```
date_key                      DATE     FK -> dim_date
ticker                        TEXT     FK -> dim_company
stock_close_price             NUMERIC  closing price in USD
stock_volume                  BIGINT   shares traded that day
active_disaster_count         INT      distinct Wildfire/Storm events active globally
nearby_disaster_count         INT      distinct events within 100 km of any tracked city
nearest_city_id               TEXT     FK -> dim_city (city closest to any disaster that day)
nearest_city_name             TEXT     denormalized city name (for convenience)
nearest_disaster_distance_km  NUMERIC  km between the nearest disaster and the nearest city
updated_at                    TIMESTAMP  last pipeline write
```

> **Composite PK:** `(date_key, ticker)` — guarantees one row per day per stock.
> The load step uses `ON CONFLICT ... DO UPDATE` so re-running the pipeline is safe.

The disaster columns (`active_disaster_count`, `nearby_disaster_count`, `nearest_city_*`)
are **identical for all 6 tickers on the same day** — they are date-level aggregates
applied to every ticker that trades on that date. See "How Cities Work" below.

---

## How Cities Work — The Full Mechanism

Understanding this is key to interpreting all three views.

### Step 1: Every EONET event has GPS coordinates

Each NASA disaster event contains one or more geometry observations — a lat/lon point
recorded on each day the event was active. Example for the Palisades Wildfire:
```
2025-01-08  lat=34.11, lon=-118.52   (fire active here on Jan 8)
2025-01-09  lat=34.13, lon=-118.55   (slightly spread on Jan 9)
```

### Step 2: Haversine distance to every tracked city

For **every single geometry observation**, the transform runs `_attach_nearest_city()`,
which computes the great-circle (haversine) distance from that GPS point to all
10 tracked cities and keeps the closest one:

```
Palisades fire on Jan 8 at (-118.52, 34.11):
  -> LA:          36.49 km   <- MINIMUM, this becomes nearest_city
  -> SF:          554 km
  -> Sacramento:  578 km
  -> Denver:      1,200 km
  -> ...
  Result: nearest_city = "US-LOS_ANGELES", distance = 36.49 km
```

This runs for every event observation across the entire EONET dataset (~2000 events x
multiple geometry points each = tens of thousands of calculations — this is why
`task_transform` originally took 10 minutes).

### Step 3: The 100 km threshold

```python
filtered["is_near_city"] = filtered["nearest_disaster_distance_km"].le(100)
```

A disaster observation is considered "near a city" if its nearest tracked city is
within 100 km. This threshold is configurable via the `CITY_IMPACT_RADIUS_KM`
environment variable.

### Step 4: Daily aggregation

For each calendar date, the transform collapses all event observations into one row:

| Column | Meaning |
|---|---|
| `active_disaster_count` | Distinct wildfire/storm event IDs active globally that day |
| `nearby_disaster_count` | Distinct event IDs where nearest city <= 100 km |
| `nearest_city_id/name` | The single city that was closest to ANY disaster that day |
| `nearest_disaster_distance_km` | The global minimum distance (any disaster to any city) that day |

### Step 5: Applied to all tickers

This per-date disaster summary is joined to every ticker. On Jan 8 2025:
- ALL, CB, TRV, AIG, PGR and BRK-B all get the same disaster values
- `nearby_disaster_count=2`, `nearest_city=LA`, `dist=36.49 km`

The difference between rows on the same day is only in `stock_close_price` and `stock_volume`.

---

## Views — Detailed Explanation

### View 1: `vw_daily_disaster_stock_impact`

```sql
SELECT f.date_key, d.year, d.month, d.day,
       c.ticker, c.company_name, c.sector,
       f.stock_close_price, f.stock_volume,
       f.active_disaster_count, f.nearby_disaster_count,
       f.nearest_city_id,
       COALESCE(city.city_name, f.nearest_city_name) AS nearest_city_name,
       city.country AS nearest_city_country,
       f.nearest_disaster_distance_km
FROM fact_daily_impact f
JOIN dim_date d    ON d.date_key = f.date_key
JOIN dim_company c ON c.ticker   = f.ticker
LEFT JOIN dim_city city ON city.city_id = f.nearest_city_id;
```

**What it is:** The main readable view. Flattens all dimension data onto the fact rows.

**One row =** one insurance/energy/construction company stock, on one trading day,
with all its disaster metrics and readable labels attached.

**Row count:** All fact rows (~480 rows for 6 tickers x ~80 trading days).

**Notable design choices:**
- `COALESCE(city.city_name, f.nearest_city_name)` — uses the dim_city normalized
  name if available, falls back to the denormalized copy stored in the fact table.
- `LEFT JOIN` on dim_city — so rows where no city was nearby (nearest_city_id is NULL)
  are still included; they just have NULL city columns.

---

### View 2: `vw_stock_disaster_price_movement`

```sql
SELECT
    impact.*,
    LAG(stock_close_price) OVER (PARTITION BY ticker ORDER BY date_key)
        AS previous_close_price,
    ROUND(
        (
            (stock_close_price - LAG(stock_close_price) OVER (...))
            / NULLIF(LAG(stock_close_price) OVER (...), 0)
            * 100
        )::numeric,
        2
    ) AS pct_close_change
FROM vw_daily_disaster_stock_impact impact;
```

**What it is:** Adds day-over-day price change (%) to every row using a window function.

**One row =** same as View 1, but with two extra columns:
- `previous_close_price` — the closing price of that ticker on the previous trading day
- `pct_close_change` — the percentage change from the previous day

**How `LAG` works here:**
- `PARTITION BY ticker` — the window resets for each stock independently
- `ORDER BY date_key` — looks back to the immediately preceding trading day in the dataset
- First row for each ticker has `NULL` for both (no previous day to reference)
- `NULLIF(..., 0)` prevents division-by-zero if a stock ever closed at exactly 0

**Why this view is the most useful for analysis:** It lets you directly ask "did AIG
drop more than usual on the days when LA fires were close?" without needing a self-join.

---

### View 3: `vw_insurance_city_risk_days`

```sql
SELECT date_key, ticker, company_name, stock_close_price,
       active_disaster_count, nearby_disaster_count,
       nearest_city_name, nearest_disaster_distance_km
FROM vw_daily_disaster_stock_impact
WHERE sector = 'Insurance'
  AND nearby_disaster_count > 0;
```

**What it is:** A focused, pre-filtered view for the core hypothesis — insurance stocks
on days when a disaster was actually close to a major city.

**One row =** one insurance company, on one specific trading day where at least one
wildfire or severe storm was within 100 km of one of the 10 tracked cities.

**Why only 108 rows:**
- 6 insurance tickers in the dataset
- 108 / 6 = **18 distinct trading days** had `nearby_disaster_count > 0`
- The other ~62 business days had no disasters within 100 km of any tracked city
  (disasters existed but were in remote areas — Canadian forests, Pacific islands, etc.)
- Those 18 days are dominated by **Jan 7–14 2025** (Palisades & Eaton fires, LA)
  and some severe storm days in February/March 2025

**What is NOT in this view (by design):**
- Construction and Energy sector stocks (filtered by `sector = 'Insurance'`)
- Calm days where disasters were far from cities (`nearby_disaster_count = 0`)
- The `pct_close_change` column (needs View 2 for that)

**Typical usage:** Join this view with `vw_stock_disaster_price_movement` to get
the price change % specifically on high-risk days.

---

## Analytical Queries

> All queries target the `disaster_dw` database. Run from psql or Metabase.

---

### Query 1 — LA Wildfire Peak: Insurance Stock Reaction During the Worst Days

**Business question:** During Jan 7–24 2025 (the Palisades & Eaton fire peak), how did
each insurer's closing price change day-over-day?

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

**What it shows:** Direct stock-price reaction of all 6 insurers during and after
the LA wildfire outbreak. A negative `pct_close_change` on high-`nearby_disaster_count`
days confirms the disaster -> stock impact hypothesis.

---

### Query 2 — Correlation: Nearby Disaster Count vs. Average Daily Stock Return

**Business question:** Is there a meaningful link between the number of close disasters
and how the insurance sector performs that day?

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

**What it shows:** If `avg_pct_change` drops as `nearby_disaster_count` rises for the
Insurance sector, that confirms the pipeline's core hypothesis in a single query.

---

### Query 3 — Cumulative Stock Performance: High-Disaster vs. Calm Periods

**Business question:** Do insurance stocks underperform during sustained high-disaster
periods (>= 2 events within 100 km) vs. calm periods?

```sql
WITH classified AS (
    SELECT
        date_key, ticker, company_name,
        stock_close_price, pct_close_change,
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

**What it shows:** Compares returns across three disaster regimes. Higher volatility
and lower avg return in "High Disaster" confirms disaster proximity as a risk factor.

---

### Query 4 — Top Risk Days Ranking

**Business question:** Which specific days were most dangerous (closest disasters,
biggest stock drop)?

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
    date_key, nearest_city_name,
    nearest_disaster_distance_km,
    nearby_disaster_count, active_disaster_count
ORDER BY nearby_disaster_count DESC, avg_sector_pct_change ASC
LIMIT 20;
```

**What it shows:** Top 20 most dangerous trading days ranked by proximity + stock drop.
Jan 8–10 2025 should dominate, confirming the LA wildfire as the primary risk signal.

---

### Query 5 — Monthly Disaster Exposure Summary

**Business question:** How did disaster exposure evolve month-by-month across Dec 2024–Mar 2025?

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

**What it shows:** January 2025 should show the highest `avg_daily_nearby_disasters`
and the worst `avg_daily_return_pct`, directly linking the LA wildfire period to
the sector's worst monthly performance.

---

## Task Requirement Fulfillment

> **Feladat:** *Legalabb 3 ertelmes analitikai lekerdezEs (pl. SQL) dokumentalva*

| # | Query | Analytical Value |
|---|---|---|
| 1 | **LA Wildfire Peak Reaction** | Stock price changes during the Jan 2025 wildfire peak |
| 2 | **Nearby Disaster Count vs. Sector Return** | Tests the pipeline's core hypothesis |
| 3 | **High vs. Calm Period Performance** | Cumulative returns by disaster regime |
| 4 | **Top Risk Days Leaderboard** | Ranks worst 20 days by proximity + stock drop |
| 5 | **Monthly Disaster Exposure Summary** | Longitudinal Dec 2024 - Mar 2025 view |

**The requirement is fully met** with 5 documented SQL queries, each with a clear
business question and explanation, tied to the LA wildfire use-case.
